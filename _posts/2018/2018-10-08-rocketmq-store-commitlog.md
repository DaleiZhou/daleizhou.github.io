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
<img src="/assets/img/2018/10/08/RocketMQCommitLog.png" width="75%" height="75%"/>
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
                // TransientStorePool的使用是数据先写入堆内存，然后异步刷新到磁盘持久化
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

　　熟悉操作系统的同学都知道现在操作系统通过段页管理的手段，通过缺页中断的方式替换合适的内存页，讲缺失的内存页换到内存中，从而给每个用户进程可以提供突破物理内存大小限制的虚拟内存空间。而文件的内存映射初始化之后仅仅是建立了逻辑地址，并没有立即在实际的在物理内存中申请空间并将数据复制进来。因此，RocketMQ提供了MappedFile的`预热`的功能。

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

        // 通过jna讲内存页锁定在物理内存中，防止被放入swap分区
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

```java
    // AllocateMappedFileService.java
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
        int canSubmitRequests = 2;
        if (this.messageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                canSubmitRequests = this.messageStore.getTransientStorePool().remainBufferNumbs() - this.requestQueue.size();
            }
        }

        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }
            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            canSubmitRequests--;
        }

        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                    "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.getTransientStorePool().remainBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
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

## <a id="FlushDisk">FlushDisk</a>

　　**Todo**: FlushDisk

## <a id="CommitLogDispatcher">CommitLogDispatcher</a>

　　**Todo**: CommitLogDispatcher

## <a id="References">References</a>

* [1] https://daleizhou.github.io/posts/log-fetch-produce.html

* [2] https://events.static.linuxfound.org/sites/events/files/lcjp13_moriya.pdf
