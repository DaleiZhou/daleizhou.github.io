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

　　在**AllocateMappedFileService.run()**中,如果没有外部线程将**stopped**变量置为true,则会不间断的执行私有方法**mmapOperation()**。该方法的作用是不断异步地从任务队列里取出请求生成对应的MappedFile，

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

　　RocketMQ提供两种刷盘方式：同步和异步。

```java
    // MappedFile.java
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        // OS_PAGE_SIZE默认配置 1024 * 4
        // fileSize默认配置 1024 * 1024 * 1024
        // 每隔OS_PAGE_SIZE写入一个0进行warmup, 减少os缺页中断
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // pages 默认设置 = 1024 / 4 * 16
            if (type == FlushDiskType.SYNC_FLUSH) {
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

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }
```

## <a id="references">References</a>

* https://daleizhou.github.io/posts/log-fetch-produce.html