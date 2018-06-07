---
layout: post
category: Kafka
title: Kafka事务消息过程分析(二)
---

## 内容 
>Status: Draft

　　事务初始化完毕之后，KafkaProducer就可以向集群发送具体的消息数据。本篇从KafkaProducer.send()为切入点，介绍发送事务消息客户端和服务端的具体实现。

## <a id="beginTransaction">beginTransaction()</a>

　　在调用KafkaProducer.send()方法之前需要调用KafkaProducer.beginTransaction()方法，这个方法的实现很简单，只是检查一些参数和修改内存状态,本地认为事务开启，不需要与集群交互。TransactionCoordinator在第一条消息发送后才认为事务开启。

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

            //检查是否需要发送AddPartitionsToTxnRequest,
            //如果第一次发送给该TopicPartition,则将topicpartition写入TransactionManage.newPartitionsInTransaction缓存中，后续会有线程在发送消息数据之前处理该请求
            //producer给新的topicPartition发送数据时需要向Coordinator发送该请求
            //Coordinator会写入logb并置状态为BEGIN等
            if (transactionManager != null && transactionManager.isTransactional())
                transactionManager.maybeAddPartitionToTransaction(tp);

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

　　在发送消息数据之前，如果producer是第一次发送给topicpartition，则生成一个AddPartitionsToTxnRequest请求，将所有类似的topicpartition放在请求中请求Coordinator。Coordinator的具体处理细节后面会介绍，这里先看producer端处理。请求是放在一个优先队列中，ADD_PARTITIONS_OR_OFFSETS的优先级为2，排在FIND_COORDINATOR和INIT_PRODUCER_ID后面。Producer端收到返回后更新本地缓存信息。

```java
    //TransactionManager.java
    //在上一篇介绍过的TransactionManager.nextRequestHandler()方法中处理AddPartitionsToTxnRequest发送请求
    synchronized TxnRequestHandler nextRequestHandler(boolean hasIncompleteBatches) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        //other code ...
    }

    //处理过程是将pendingPartitionsInTransaction + newPartitionsInTransaction数据拼成一个请求，带上ProducerId, epoch值发送给broker
    private synchronized TxnRequestHandler addPartitionsToTransactionHandler() {
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
        newPartitionsInTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, new ArrayList<>(pendingPartitionsInTransaction));
        return new AddPartitionsToTxnHandler(builder);
    }
```

　　KafkaProducer.doSend()方法在最后通过RecordAccumulator.append将消息放入RecordAccumulator中。RecordAccumulator类中通过ConcurrentMap<TopicPartition, Deque<ProducerBatch>> batches属性存储TopicPartition对应的batches。batch放在双端队列中，是由于发送线程是从队首取数据进行发送，如果中途遇到异常则重新把batch塞回到队首，是一种容错的设计。

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
            // 从本地内存中batches变量中获取topicpartition对应的batch队列，如果没有则创建一个空的放入batches中并返回
            Deque<ProducerBatch> dq = getOrCreateDeque(tp);
            synchronized (dq) {
                // close情况下抛异常
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");

                // 调用tryAppend方法试着append消息, 如果append成功则返回，如果tp对应的batch队列为空则继续往下执行
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                //如果dq为null，返回appendResult = null
                if (appendResult != null)
                    return appendResult;    
            }

            byte maxUsableMagic = apiVersions.maxUsableProduceMagic();
            int size = Math.max(this.batchSize, AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");

                //对dq加锁之后再次尝试tryAppend,同样地如果为append成功则直接返回结果
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, headers, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    return appendResult;
                }

                //尝试append不成功，则新生成一个batch并放在dp中
                MemoryRecordsBuilder recordsBuilder = recordsBuilder(buffer, maxUsableMagic);
                ProducerBatch batch = new ProducerBatch(tp, recordsBuilder, time.milliseconds());
                //调用producerBatch.tryAppend方法将消息放入队列中，
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, headers, callback, time.milliseconds()));

                dq.addLast(batch);
                incomplete.add(batch);

                // Don't deallocate this buffer in the finally block as it's being used in the record batch
                buffer = null;

                //执行到这里说明是新生成一个batch，返回appendresult时设置newBatchCreated = true, batchIsFull = dp.size>1 or 新生成的batch写入的字节数大于写入上限
                //KafkaProducer的doSend中会检查这两个参数，决定是否调用sender.weakup
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true);
            }
        } finally {
            if (buffer != null)
                free.deallocate(buffer);
            appendsInProgress.decrementAndGet();
        }
    }

    //try append, 如果对应的dq为空队列，或者是batch.tryAppend如果在batch上没有足够空间等情况下直接返回null，在append()方法中会多次尝试，如果都收到null则会创建一个新的batch
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers,
                                         Callback callback, Deque<ProducerBatch> deque) {
        ProducerBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, headers, callback, time.milliseconds());
            if (future == null)
                last.closeForRecordAppends();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false);
        }
        return null;
    }
```

　　RecordAccumulator.append()或者是RecordAccumulator.tryAppend()方法append消息数据都是通过batch.tryAppend()来实现的。在batch.tryAppend()中通过recordsBuilder.append()方法来append数据。

```java
    //ProducerBatch.java
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 如果没有足够的空间则返回null,调用者最终会生成一个新的batch
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            //this.recordsBuilder.append()进行append数据
            Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            
            //more code ...
        }
    }
```

　　MemoryRecordsBuilder维护一个offset,这个offset会写入最终发送的消息中，

//todo
borker端根据这个seqid和ProducerIdAndEpoch进行事务控制。

根据消息版本不同使用不同的使用不同的方式写消息数据。

```java
    //MemoryRecordsBuilder.java
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        //nextSequentialOffset()获取SequentialOffset(),用于表示这个消息的Seqid
        return appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    private Long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                  ByteBuffer value, Header[] headers) {
        try {
            // more code ...

            //根据消息版本设置的magic值决定如何append数据,消息格式不一样
            //如果版本>=2则调用DefaultRecord进行写数据，否则使用LegacyRecord进行写数据
            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
                return null;
            } else {
                return appendLegacyRecord(offset, timestamp, key, value);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    // 这里只贴appendDefaultRecord()
    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        // 这里发送相对时间戳和offset的偏移量
        int offsetDelta = (int) (offset - baseOffset);
        long timestampDelta = timestamp - firstTimestamp;
        //通过DefaultRecord.writeTo()方法将数据写进appendStream.
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }
```

　　到这里消息数据应该已经append到batch里了，每个topicPartition对应一个batch队列。在后台线程Sender.run()的循环中，每次循环中都会调用Sender.sendProducerData()方法拼装ProduceRequest将消息发送到对应的broker上。

## TBD
