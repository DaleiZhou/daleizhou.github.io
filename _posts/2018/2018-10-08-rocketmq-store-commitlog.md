---
layout: post
category: RocketMQ
title: RocketMQ存储实现分析
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　赵家庄王铁柱曾经教过我他看源码的技巧，那就是他习惯先抓住底层的数据结构，然后逐步拓展到围绕数据而展开的一系列系统行为实现过程(他个人习惯)。这篇我们从RocketMQ最基础的存储部分切入，解析一下RocketMQ数据存储结构及索引等实现细节，在熟悉底层存储结构的基础上，我们可以很自然的领会RocketMQ中一部分设计理念。
<!--more-->

　　RocketMQ中的消息最终会存储到名为`CommitLog`中，与Kafka中的无限长Log相似，都是只会通过往末尾追加的方式进行写入的Log，读取阶段根据offset检索到具体消息进行读取，Kafka Log的具体设计细节可以参考[Kafka log的读写分析[1]](https://daleizhou.github.io/posts/log-fetch-produce.html)。通过对比我们发现区别也是很明显的，在Kafka中的Log于具体的TopicPartition一一对应，而RocketMQ的CommitLog则在每个Broker上只有一个，这样的结构如何通过Topic为单位去消费呢，带着这样的问题我们具体看CommitLog的设计与实现细节。

## <a id="CommitLog">CommitLog</a>

　　这里，先根据源码我们抽象出如下的结构示意图，给读者一个宏观的印象，也方便后续的讲解说明。

<div align="center">
<img src="/assets/img/2018/10/08/RocketMQCommitLog.png" width="90%" height="90%"/>
</div>

　　在`store`包里可以直接看到CommitLog的定义，CommitLog中有个重要的成员变量**MappedFileQueue**，该变量保存着MappedFile列表。MappedFile保存着最终的消息数据。在逻辑上CommitLog可以看成一个无限长的Log文件，消息不断通过追加的方式写到CommitLog的末尾。在实际实现时，这个无限长的日志文件由顺序的MappedFile列表组成，每个MappedFile则顺序保存一定数量的消息。

　　每个MappedFile会对应一个具体的文件用于将消息数据存储于物理磁盘上，文件名有一定的实际意义，这个后面具体分析。每个MappedFile的大小上限是在Broker启动时设置的，值的大小为**MessageStoreConfig.mapedFileSizeCommitLog**，默认为**1024 * 1024 * 1024**，即每个MappedFile默认最大为`1G`。因为每个消息的大小并不是固定的，因此每个MappedFile的末尾可能会有一定的空白空间。

　　Broker启动时会初始化一个**DefaultMessageStore**，在初始化时会初始化并运行**AllocateMappedFileService**后台服务。从名字上看可以知道这个服务的作用是用于分配MappedFile。按照常理来说，分配初始化一个MappedFile需要时调用就行，为什么需要这么个服务呢。下面具体看这个服务**run()**方法的实现细节来寻找答案。

## <a id="AllocateMappedFileService">AllocateMappedFileService</a>

　　在**AllocateMappedFileService.run()**中,如果没有外部线程将**stopped**变量置为true,则会不间断的执行私有方法**mmapOperation()**。该方法的作用是不断异步地从任务队列里取出请求生成对应的MappedFile，用于AllocateMappedFileService提供的另一个获取MappedFild方法快速获取结果，下面看具体实现。

```java
    // AllocateMappedFileService.java
    public void run() {
        log.info(this.getServiceName() + " service started");
        while (!this.isStopped() && this.mmapOperation()) { }
        log.info(this.getServiceName() + " service end");
    }

    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {

            // requestQueue是个优先级的无界阻塞队列,
            // 其中的元素类型为AllocateRequest，
            // 根据实现可以看到请求的fileSize越大优先级越高
            // 如果size相同，则根据文件名比较物理偏移量，偏移量越小优先级越高
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                // 遇到超时删除了请求
                return true;
            }
            if (expectedRequest != req) {
                // 异常情况，可能是超时导致queue与table里的request不对应了
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                
                // 两种生成MappedFile的方式
                // isTransientStorePoolEnable设置为true且刷盘方式为异步刷盘
                // TransientStorePool的使用是数据先写入堆外内存，然后异步刷新到磁盘持久化
                if (messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                    try {
                        // 通过ServiceLoader的方式起一个MappedFile并init初始化
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        mappedFile = new MappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    mappedFile = new MappedFile(req.getFilePath(), req.getFileSize());
                }

                long eclipseTime = UtilAll.computeEclipseTimeMilliseconds(beginTime);
                if (eclipseTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + eclipseTime + " queue size " + queueSize
                        + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // 新生成的mappedFile大小至少有MapedFileSizeCommitLog配置的那么大
                // 且配置了warmMappedFile = true, 则对该mappedFile进行warmup,具体实现看后面代码解析
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                    .getMapedFileSizeCommitLog()
                    &&
                    this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    // 文件预热
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                        this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                // IO异常，还可以挣扎挣扎，将请求重新放入requestQueue
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                // 获取MappedFile的方法处会等待这个值进行同步
                req.getCountDownLatch().countDown();
        }
        return true;
    }
```

　　`MappedFile`之所以称为Mapped,是因为RocketMQ在实现持久化时，将CommitLog磁盘文件通过内存映射的方式进行操作。内存映射的方式直接将文件的内容复制到用户进程地址空间内，减少了用户态和内核态的切换及少了一次文件复制的成本。MappedFile是作为最终的物理文件的内存映射的一个类，RocketMQ通过该类进行读写操作，最终作用到物理文件上。都是通过如下的方式进行初始化。

```java
    this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
    this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
```

　　如果`MessageStoreConfig().isTransientStorePoolEnable()`为true, 即Broker上transientStorePoolEnable==true,刷盘模式配置为异步刷盘且Broker的角色不为SLAVE这三个条件同时满足。这三个条件同时成立，这种条件下，MappedFile会借用TransientStorePool中的缓存缓存空间。如果是这种方法，Broker上的消息会先写入ByteBuffer.allocateDirect方式调用直接申请堆外内存中，由异步刷盘线程写入fileChannel中，最后进行flush。RocketMQ之所以这么实现是因为这种方案下写数据是完全写内存，速度相较于写文件对应的Page Cache更快[[3]](https://www.cnblogs.com/duanxz/p/4875020.html)，而且减少锁的占用，提升效率。不过在Broker出问题的情况下丢失的数据可能更多。

　　熟悉操作系统的同学都知道现在操作系统通过段页管理的手段，通过缺页中断的方式替换合适的内存页，将缺失的内存页换到内存中，从而给每个用户进程可以提供突破物理内存大小限制的虚拟内存空间。而文件的内存映射初始化之后仅仅是建立了逻辑地址，并没有立即在实际的在物理内存中申请空间并将数据复制进来。因此，RocketMQ提供了MappedFile的`预热`的功能。

```java
    // MappedFile.java
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        // OS_PAGE_SIZE默认配置 1024 * 4
        // fileSize默认配置 1024 * 1024 * 1024 = 1G
        // 每隔OS_PAGE_SIZE写入一个0进行warmup, 减少后面os缺页中断
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // pages 默认设置 = 1024 / 4 * 16
            if (type == FlushDiskType.SYNC_FLUSH) {

                // 如果刷盘方式为同步刷盘，则每 pages 刷盘一次
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // 最后强制flush一次
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }

        // 通过jna将内存页锁定在物理内存中，防止被放入swap分区
        this.mlock();
    }

    // LibC继承自com.sun.jna.Library，通过jna方法访问一些native的系统调用
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        // jna
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }
```

　　**MappedFile.warmMappedFile()**方法即实现文件预热的功能，每个OS_PAGE写入一个任意值(这里为0)，也就是说在初始化状态下，这样操作会给每个页产生恰好一次的缺页中断，这样操作系统会分配物理内存并且将物理地址与逻辑地址简历映射关系。最后配合jna方法，传入mappedByteBuffer的地址及文件长度，告诉内核即将要访问这部分文件，希望能将这些页面都锁定在物理内存中，不换进行swapout [[2]](https://events.static.linuxfound.org/sites/events/files/lcjp13_moriya.pdf),从而在后续实际使用这个文件时提升读写性能。

　　上面只是讲解了RocketMQ不间断地创建MappedFile的Loop过程，那么创建MappedFile的请求是什么时候放入请求队列的，为什么要通过这种异步创建的方式创建MappedFile呢，我们看下面的代码继续分析。

```java
    // AllocateMappedFileService.java
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        // 一次最多放两个请求
        int canSubmitRequests = 2;
        // 如果TransientStorePoolEnable且角色不是SLAVE，如果pool中没有多余的空间则进行fast fail
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) {
                canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs() - this.requestQueue.size();
            }
        }

        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        // 当且仅当requestTable中nextFilePath之前没有已经放入的请求
        // 这里可能跳过，因为正常一次放两个请求，可能本次请求的path对应的MappedFile在之前的请求中已经生成了
        if (nextPutOK) {
            // 初始化为2，如果TransientStorePoolEnable=true且角色不为SLAVE,该值会被调整为pool剩余的buffer数目
            if (canSubmitRequests <= 0) {
                // pool空间不足删除请求直接返回null
                this.requestTable.remove(nextFilePath);
                return null;
            }
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) { }
            canSubmitRequests--;
        }

        // 处理过程和nextFilePath的处理一致
        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) { }
            }
        }

        // 异步创建的Loop有异常，直接返回null
        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        // 
        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                // 等待异步的Loop创建完成，通计数栓将异步转同步等待
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    // 假如超时则直接返回null, 但并没有从requestTable中删除请求
                    // 因此在创建的Loop中会有一些检查操作
                    return null;
                } else {
                    this.requestTable.remove(nextFilePath);
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }
```

　　这里是RocketMQ另一个实现上巧妙的地方了，之前代码分析过，创建MappedFile后台循环优先创建文件名小的请求。而通过阅读代码我们知道，每个MappedFile的文件名是根据上一个MappedFile的**FileFromOffset** + mappedFileSize得到的，代表该文件在CommitLog中的offset。因此文件名小的会被优先创建。每次申请获取MappedFile会创建一个冗余的创建请求。在方法结束时通过计数栓等待机制将异步创建转为同步等结果。如果是已经通过之前的冗余请求创建完了，不仅本次创建请求得到满足，而且还会带来一个冗余的创建请求副作用。而这个副作用会被异步创建线程读取并进行创建但是不会阻塞调用获取的线程。等下次真的有请求获取这个结果可以很快的返回结果。

## <a id="Flush Disk">Flush Disk</a>

　　RocketMQ相较于Kafka而言，在不仅提供异步刷盘的能力，同时还提供了可选的同步刷盘的能力。当然，RocketMQ中的同步刷盘也是通过异步线程实现，不过通过计数栓的阻塞等待将异步转同步，这种处理方法在RocketMQ中比较常见，例如前面章节的创建MappedFile也是通过这种方法同步得到结果。下面看两种刷盘方式的具体实现。

```java
    // CommitLog.java
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // other code ...

        //在PutMessage调用的最后，进行刷盘处理
        handleDiskFlush(result, putMessageResult, msg);
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 同步刷盘处理
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            // CommitLog初始化时，如果是SYNC_FLUSH方式刷盘，则flushCommitLogService的实现类为GroupCommitService
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            // 消息中PROPERTY_WAIT_STORE_MSG_OK如果是true,则插入刷盘请求并同步等待刷盘结果
            if (messageExt.isWaitStoreMsgOK()) {
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                service.putRequest(request);
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                // 如果同步等待countDownLatch.await
                // 若超时还没返回结果，则设置put结果属性并返回，客户端会感知到
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                        + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                service.wakeup();
            }
        }
        // 两种异步刷盘，对应两种MappedFile生成方式
        else {
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
        }
    }
```

　　每次写完消息之后需要进行刷盘操作，Broker可以配置同步或者异步的刷盘方式，如果是同步刷盘则写完消息后需要等待刷盘结果，如果是异步刷盘，则不一定需要等待刷盘结果就进行下一步操作。两种处理手段各有特点，下面先看**GroupCommitService**中同步刷盘的实现过程。

```java
    //CommitLog#GroupCommitService.java

    // 通过维护read,write两个列表，通过执行完一轮交换的方式读写互不干扰
    private void swapRequests() {
        List<GroupCommitRequest> tmp = this.requestsWrite;
        this.requestsWrite = this.requestsRead;
        this.requestsRead = tmp;
    }

    private void doCommit() {
        synchronized (this.requestsRead) {
            if (!this.requestsRead.isEmpty()) {
                for (GroupCommitRequest req : this.requestsRead) {
                    // 有可能req请求的offset在下一个MappedFile里，因此通过最多两轮循环取刷盘，
                    // 如果消息在下一个MappedFile里，则第一次刷盘后MappedFileQueue#flushedWhere会更新，
                    // 再一次通过MappedFileQueue#findMappedFileByOffset()获取到就是最新的MappedFile
                    boolean flushOK = false;
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        // 第一次刷盘后没有使得FlushedWhere大于请求的offset,则会第二次刷盘
                        if (!flushOK) {
                            CommitLog.this.mappedFileQueue.flush(0);
                        }
                    }
                    // 通知同步等待结果的线程
                    req.wakeupCustomer(flushOK);
                }

                this.requestsRead.clear();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }
    }

    public void run() {
        while (!this.isStopped()) {
            try {
                // waitForRunning()中如果hasNotified已经被设置为true,则直接交换read/write进行doCommit
                // 正常最多等待10ms,然后交换read/write进行doCommit
                this.waitForRunning(10);
                this.doCommit();
            } catch (Exception e) {
                CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        // 关闭后休眠10ms再挣扎一把
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            CommitLog.log.warn("GroupCommitService Exception, ", e);
        }
        synchronized (this) { this.swapRequests(); }
        this.doCommit();
    }
```

　　下面说一下每次写入消息后异步刷盘处理过程。RocketMQ提供两种异步刷盘通过两种途径实现：**FlushRealTimeService**和**CommitRealTimeService**。其中CommitRealTimeService最终还是通过flushCommitLogService服务完成异步刷盘，这里先来看FlushRealTimeService的实现过程。

```java
    // CommitLog#FlushRealTimeService.java
    public void run() {
            while (!this.isStopped()) {
                // 是否是定时flush, 默认为false即实时
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                // 调度flush时间间隔
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 至少有多少page没有flush才进行flush,如果为0，则意味着需要立马flush
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();
                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // 如果上次flush执行已经超过flushPhysicQueueThoroughInterval，默认值为10s
                // 如果条件成立，则打印进度并且将flushPhysicQueueLeastPages设置为0
                // 以为不管缓存了多少都需要进行flush
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    // 如果是是定时flush
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        // 有调用weak或者等待超时都会进中断
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    // flush
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                } catch (Throwable e) {
                    this.printFlushProgress();
                }
            }

            // 正常终止，flush所有缓存的数据
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
        }
```

　　如前文介绍MappedFile时所说`MessageStoreConfig().isTransientStorePoolEnable()`为true时，写消息时的数据是先写入堆外内存，因此这种条件下异步刷盘需要先将数据从堆外内存commit到fileChannel的Page Cache中，然后唤醒FlushRealTimeService进行flush。

```java
    //CommitLog#CommitRealTimeService.java
    public void run() {
        while (!this.isStopped()) {
            long begin = System.currentTimeMillis();
            // 时间太久需要立即commit
            if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                this.lastCommitTimestamp = begin;
                commitDataLeastPages = 0;
            }

            try {
                // 从堆外内存写到filechannel中
                boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                long end = System.currentTimeMillis();
                if (!result) {
                    // result为false时则说明有数据提交
                    // 唤醒flush线程，将数据从从page cache刷新到磁盘上
                    flushCommitLogService.wakeup();
                }

                this.waitForRunning(interval);
            } catch (Throwable e) {
                CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
            }
        }

        boolean result = false;
        for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
            result = CommitLog.this.mappedFileQueue.commit(0);
            CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
        }
    }
```

## <a id="CommitLogDispatcher">CommitLogDispatcher</a>

　　回到一开始的问题，所有Topic的数据都通过追加的方式写到CommitLog末尾，那么Consumer如何通过Topic和Offset进行消息的消费呢。答案就是Broker启动之后，有名为**ReputMessageService**的后台线程不断的进行将新写入的数据更新**Index**和**ConsumeQueue**。Index的作用是通过Topic+Key与CommitLogOffset建立关联关系关系用于QueryMessageProcessor的消息消费。ConsumerQueue的作用是建立Queue+QueueOffset与CommitLogOffset之间的关联关系，用于通过PullMessageProcessor进行消费消息。下面来看具体的构建过程。

```java
    // IndexService.java
    public void buildIndex(DispatchRequest req) {
        // 最多尝试三次获取最后一个非空IndexFile,如果没有或者最后一个写满则生成一个新的
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            // 旧的请求就会直接退出
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }

            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                // ...
                // 剔除TRANSACTION_ROLLBACK_TYPE类型
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            if (req.getUniqKey() != null) {

                // 在IndexFile中写入Topic#UniqKey与CommitLogOffset的关联
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                // ...
            }

            if (keys != null && keys.length() > 0) {
                // 每个key在IndexFile中写入Topic#Key与CommitLogOffset的关联 
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        // ...
                    }
                }
            }
        } else {
            log.error("build index error, stop building index");
        }
    }

```

　　IndexFile与CommitLog相似，可以看成分段的无限长的Log的抽象。并且大小固定，写满则新生成一个继续写。IndexService通过调用IndexFile.putKey()方法不断地将Topic#Key与CommitLogOffset关联关系按照固定格式写到IndexFile文件中，不过写的过程中并不是简单的将记录写在尾部，而是还会对一些其他数据做出修改，使得文件内部结构类似于拉链的Hash表。对照博文开头的大图看具体写入过程。

```java
    // IndexFile.java
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            int keyHash = indexKeyHashMethod(key);
            // 根据key的hash取模获得槽的位置 hashSlotNum默认值为500w
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                // 查看该槽内有多少记录，该记录数会写入本条index中用于逻辑上将这些记录串起来
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                long timeDiff = // timeDiff处理
                // 计算在FileIndex中的偏移位置
                // head(40B) + SlotTable大小(400w * 4B) + 已有记录数目*20B
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                // 写入记录
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // 将本槽内已有记录数（不含本条）写入用于将记录串联
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                // 在槽内写入本条Index的位置信息
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 更新缓存
                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } 

            // other ...

        return false;
    }
```

　　ConsumeQueue的更新比较简单了，根据Topic和QueueId获取到ConsumeQueue。每一个ConsumeQueue也可以看成分段的无限长的AppendLog。更新记录时按顺序写入记录，记录固定是CommitLogOffset+MessageSize+TagCode,共20Byte。Consumer根据QueueId+QueueOffset读到一条记录按照里面信息再去CommitLog中读取数据进行消费。

```java
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }
```

## <a id="conclusion">Conclusion</a>

　　到这里RocketMQ存储CommitLog及围绕在周边的一些后台任务和访问方法就介绍完了。RocketMQ为了实现高效的消息队列读写进行了一系列优化，通过后台定时任务+CountDownLatch方式将同步优化为异步、文件预热、jna锁定物理页，MappedFile之外通过堆外内存异步更新数据、后台线程异步更新Index,ConsumeQueue等方式提升读写性能，对存储部分的优化做了很深入的设计与实现。

## <a id="References">References</a>

* [1] https://daleizhou.github.io/posts/log-fetch-produce.html

* [2] https://events.static.linuxfound.org/sites/events/files/lcjp13_moriya.pdf

* [3] https://www.cnblogs.com/duanxz/p/4875020.html
