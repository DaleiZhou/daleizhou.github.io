---
layout: post
category: Kafka
title: Kafka事务消息过程分析(二)
---

## 内容 
>Status: Draft

　　事务初始化完毕之后，KafkaProducer就可以向集群发送具体的消息数据。本篇从KafkaProducer.send()为切入点，介绍发送事务消息客户端和服务端的具体实现。

## <a id="beginTransaction">beginTransaction()</a>

　　在调用KafkaProducer.send()方法之前需要调用KafkaProducer.beginTransaction()方法，这个方法的实现很简单，只是检查一些参数和修改内存状态,不需要与集群交互。

```java
    public synchronized void beginTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION); //READY -> IN_TRANSACTION
    }
```

## <a id="send">send()</a>

　　KafkaProducer.send()方法实现生产消息的发送，发送的record不仅可以指定topic,还可以指定partition，即指定消息发到指定分区中。如果用户不提供则使用系统默认的根据一些参数通过简单取模的方式得到分区，这种取模的方式可以做到消息基本上均匀分布在各个分区上。

　　在doSend()方法中只是把record放在消息累加器中。累加器中给每个topic-partition对应待发送数据的batch，达到一定条件才整体发送。之所以这么设计应该是为了减少网络通信次数，提高整体的运行效率。下面是具体代码的注释。

```java
    //KafkaProducer.java
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    //异步发送的实现
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            // 首先保证topic的metadata是可用的
            // waitOnMetadata方法中如果topic对应的metadata没准备好，
            //调用metadata.requestUpdate()设置更新时间，并且在Sender线程中会一直轮询的调用client.poll(),
            //而在poll()中调用metadataUpdater.maybeUpdate(now)立即更新metadata
            ClusterAndWaitTime clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            
            // other code ...

            //获取记录对应的partition，如果record中指定partition则使用，否则根据Partitioner计算得到
            //Kafka的源码中提供了DefaultPartitioner，实现了partition()方法，具体过程就是根据Key, DefaultPartitioner
            //维护的一个<topic, counter>计数器等数据简单的取模得到对应的partition
            int partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);

            //other code ...

            //压入请求数据到队列，等待异步发送
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs);
            //如果batch已经满了或者产生了新的batch则发送数据
            if (result.batchIsFull || result.newBatchCreated) {
                this.sender.wakeup();
            }
            return result.future;
        }

        //other code ...
    }
```

　　KafkaProducer.doSend()方法在最后通过RecordAccumulator.append将消息放入RecordAccumulator中。RecordAccumulator类中通过ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches属性存储TopicPartition对应的batchs。batch放在双端队列中，是由于发送线程是从队首取数据进行发送，如果中途遇到异常则重新把batch塞回到队首，是一种容错的设计。

```java
    //RecordAccumulator.java
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Header[] headers,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        ByteBuffer buffer = null;
        if (headers == null) headers = Record.EMPTY_HEADERS;
        try {
            // check if we have an in-progress batch
            Deque<ProducerBatch> dq = getOrCreateDeque(tp); //取或者创建
            synchronized (dq) {
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                //如果dq为null，返回appendResult = null，
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");

                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, time.milliseconds());
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, headers, callback, time.milliseconds()));

                dq.addLast(batch);
                incomplete.add(batch);

                // Don't deallocate this buffer in the finally block as it's being used in the record batch
                buffer = null;

                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true);
            }
        } finally {
            if (buffer != null)
                free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }
```

## TBD
