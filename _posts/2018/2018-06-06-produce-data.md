---
layout: post
category: [Kafka, Source Code]
title: Kafka事务消息过程分析(二)
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　事务初始化完毕之后，KafkaProducer就可以向集群发送具体的消息数据。本篇从KafkaProducer.send()为切入点，介绍发送事务消息客户端和服务端的具体实现。
<!--more-->

## <a id="KafkaProducer">KafkaProducer</a>

**beginTransaction**

　　在调用KafkaProducer.send()方法之前需要调用KafkaProducer.beginTransaction()方法，这个方法的实现很简单，只是检查一些参数和修改内存状态,本地认为事务开启，不需要与集群交互。TransactionCoordinator在第一条消息发送后才认为事务开启。

```java
    public synchronized void beginTransaction() {
        ensureTransactional();
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION); //READY -> IN_TRANSACTION
    }
```

**send**

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

　　AddPartitionsToTxnRequest中带有本Producer中transactionalId, producerId, epoch等信息，Coordinattor在接收到请求之后有异常处理，例如epoch过期，producerId和transactionId不合预期等情况进行错误返回，Producer在AddPartitionsToTxnHandler回调方法中对这些错误情况进行处理，使得跨Session的事务控制的部分异常情况在AddPartition阶段就开始避免。

```java
    //TransactionManager.java
    //在上一篇介绍过的TransactionManager.nextRequestHandler()方法中处理AddPartitionsToTxnRequest发送请求
    synchronized TxnRequestHandler nextRequestHandler(boolean hasIncompleteBatches) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        //other code ...
    }

    //处理过程是将pendingPartitionsInTransaction + newPartitionsInTransaction数据拼成一个请求，带上ProducerId, epoch值发送给broker，用于broker端对这些值进行检查并且错误处理
    private synchronized TxnRequestHandler addPartitionsToTransactionHandler() {
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
        newPartitionsInTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, new ArrayList<>(pendingPartitionsInTransaction));
        return new AddPartitionsToTxnHandler(builder);
    }

    private class AddPartitionsToTxnHandler extends TxnRequestHandler {
        
        @Override
        public void handleResponse(AbstractResponse response) {
                //more code ...

                else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.INVALID_PRODUCER_ID_MAPPING
                        || error == Errors.INVALID_TXN_STATE) {
                    fatalError(new KafkaException(error.exception()));
                    return;
                }
            }
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

　　MemoryRecordsBuilder维护一个offset,这个offset会写入最终发送的消息中。在MemoryRecordsBuilder中根据magic值不同使用不同的使用不同的方式写消息数据。

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

```java
    //Sender.java
    private long sendProducerData(long now) {
        Cluster cluster = metadata.fetch();

        // 获取数据已经可以发送的partitions列表
        // accumulator.ready()方法中遍历batches,对于每个tp对应的batch队列：
        // 如果leader为null，但队列不为空则将topic加入unknownLeaderTopics中
        // tp不在muted队列中而且队列中，而且满足如如下任一条件都会将对应的node认为是ready的
        // 1. 第一个batch为full 2. record等待时间至少lingerMs毫秒 3. accumulator被关闭
        // 4. flushesInProgress数目大于0， 5. 等待分配内存的线程数目大于0
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        // 因为topic过期或者某个topic处于选举过程中，使得unknownLeaderTopics不为空，即有的tp对应的leader无法获取，则强制更新metadata
        if (!result.unknownLeaderTopics.isEmpty()) {
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic);
            this.metadata.requestUpdate();
        }

        // 从网络连接角度处理没有ready的node
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            if (!this.client.ready(node, now)) {
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.connectionDelay(node, now));
            }
        }

        // 创建produce请求数据准备，在accumulator.drain方法中通过一个drainIndex%size避免每次循环都从0开始，均衡所有请求。
        //对于那些不在in-flight队列中的batches且不在重试等待期内,对size, sequence, producerIdAndEpoch等进行检查，最终返回一个每个node对应的ready队列用于发送给服务器
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(cluster, result.readyNodes,
                this.maxRequestSize, now);
        if (guaranteeMessageOrder) {
            // 如果是消息保序的，则将drain得到的batches对应的tp放入mute队列中
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        // more code ...

        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    //内部方法，分别发送每个node对应的batches
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeout, entry.getValue());
    }

    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        // more code ...

        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();

            // down convert if necessary 
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(minUsedMagic, 0, time).records();
            produceRecordsByPartition.put(tp, records);
            recordsByPartition.put(tp, batch);
        }

        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
        }

        //构造produce请求，设置回调handler
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(minUsedMagic, acks, timeout,
                produceRecordsByPartition, transactionalId);
        RequestCompletionHandler callback = new RequestCompletionHandler() {
            public void onComplete(ClientResponse response) {
                //设置结果回调方法，在handleProduceResponse对服务端返回结果进行处理，包括成功情况下删除，失败情况下重新放入队列，序列号重复情况下处理等。
                handleProduceResponse(response, recordsByPartition, time.milliseconds());
            }
        };

        String nodeId = Integer.toString(destination);
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0, callback);
        // 放入in-fight队列中，会有后台线程从infight队列中取数据发送给brokers
        client.send(clientRequest, now);
    }
```

## <a id="KafkaApis">KafkaApis</a>

　　下面我们看服务器部分如何处理AddPartitionToTxnRequest和ProduceRequest。

**handleAddPartitionToTxnRequest**

　　下面我们看服务器部分如何处理AddPartitionToTxnRequest和ProduceRequest。AddPartitionToTxnRequest的请求处理过程比较简单，handleAddPartitionToTxnRequest()方法中处理。如果有身份验证失败或者不能识别的topicpartition则直接返回给客户端错误。

```scala
    //KafkaApis.scala
   def handle(request: RequestChannel.Request) {
        case ApiKeys.ADD_PARTITIONS_TO_TXN => handleAddPartitionToTxnRequest(request)
    }

   def handleAddPartitionToTxnRequest(request: RequestChannel.Request): Unit = {
      //处理身份验证和无法识别的topic-partition

      //mote code ...

      if (unauthorizedTopicErrors.nonEmpty || nonExistingTopicErrors.nonEmpty) {
        //如果存在身份验证失败或无法识别的tp，直接返回给客户端错误交由客户端处理
      } else {
        def sendResponseCallback(error: Errors): Unit = {
          def createResponse(requestThrottleMs: Int): AbstractResponse = {
            val responseBody: AddPartitionsToTxnResponse = new AddPartitionsToTxnResponse(requestThrottleMs,
              partitionsToAdd.map{tp => (tp, error)}.toMap.asJava)
            trace(s"Completed $transactionalId's AddPartitionsToTxnRequest with partitions $partitionsToAdd: errors: $error from client ${request.header.clientId}")
            responseBody
          }

          sendResponseMaybeThrottle(request, createResponse)
        }

        txnCoordinator.handleAddPartitionsToTransaction(transactionalId,
          addPartitionsToTxnRequest.producerId,
          addPartitionsToTxnRequest.producerEpoch,
          authorizedPartitions,
          sendResponseCallback)
      }
    }
  }
```

　　handleAddPartitionToTxnRequest()在正常情况下会调用txnCoordinator.handleAddPartitionsToTransaction()方法。handleAddPartitionsToTransaction()方法中首先对各种不正常情况进行处理，如没有正常的transactionalId，producerEpoch过期，producerId不合预期等情况下会返回错误给客户端。如果没有错误则调用txnManager.appendTransactionToLog()方法写入日志中及等待足够数量的followers返回。

　　特别地在调用txnManager.appendTransactionToLog()方法前，会调用txnMetadata.prepareAddPartitions()方法将txnMetadata状态置为Ongoing，用于后面的事务的提交。事务状态从Empty变为Ongoing状态情况总共有两种，一种是AddPartitionsToTxnRequest请求处理，另一种是后面会介绍的AddOffsetsToTxnRequest处理。

```scala
   //TransactionCoordinator.scala
   def handleAddPartitionsToTransaction(transactionalId: String,
                                       producerId: Long,
                                       producerEpoch: Short,
                                       partitions: collection.Set[TopicPartition],
                                       responseCallback: AddPartitionsCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request")
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid producerId mapping error.
      val result: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndMetadata) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // 异常处情况及正常情况处理
          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId) {
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.producerEpoch != producerEpoch) {
              Left(Errors.INVALID_PRODUCER_EPOCH)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the client backoff and retry
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              Left(Errors.NONE)
            } else {
              //在正常情况下会调用prepareAddPartitions()方法将TransactionMetadata状态置为Ongoing
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds()))
            }
          }
      }

      result match {
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          //正常情况处理，将metadata写入日志中，appendTransactionToLog()中将消息写入日志，并等待足够数量的ack，如果成功则更新缓存信息并调用responseCallback()返回结果给客户端
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, responseCallback)
      }
    }
  }
```

**handleProduceRequest**

　　客户端发送的ProducerRequest同样也在KafkaApis中处理。为了提高整体运行效率，kafka在0.9版本引入了配额（quotas）控制的概念。每个client的数据交互速率通过多个小窗口抽样得到，broker会对每个ClientId有一定的阈值，延迟返结果给交互太快的客户端。这种限速对客户端是透明的，而不是broker直接告诉客户端需要降速，因此客户端不需要考虑特别多的复杂情况。限于篇幅和主题的原因，这个部分后面有机会详细展开，这里只需要认为返回给客户端结果，不影响理解。

```scala
   // KafkaApis.scala
   def handle(request: RequestChannel.Request) {
        case ApiKeys.PRODUCE => handleProduceRequest(request)
    }

   // producer request 处理过程
   def handleProduceRequest(request: RequestChannel.Request) {
    val produceRequest = request.body[ProduceRequest]
    val numBytesAppended = request.header.toStruct.sizeOf + request.sizeOfBodyInBytes

    // 身份验证，未知tp处理 more code ...

    // produce结果回调
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {

      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses
      var errorInResponse = false

      // more code ...

      def produceResponseCallback(bandwidthThrottleTimeMs: Int) {
        if (produceRequest.acks == 0) {
          // ack == 0又无错误时不需要做操作返回给客户端，
          // 而如果如果errorInResponse != null
          // 则关闭连接，客户端感知到连接关闭会刷新metadata
          if (errorInResponse) {
            val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
              topicPartition -> status.error.exceptionName
            }.mkString(", ")
            //log code ...

            closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
          } else {
            sendNoOpResponseExemptThrottle(request)
          }
        } else {
          //最终会调用这个方法进行限速发送回结果
          sendResponseMaybeThrottle(request, requestThrottleMs =>
            new ProduceResponse(mergedResponseStatus.asJava, bandwidthThrottleTimeMs + requestThrottleMs))
        }
      }

      // When this callback is triggered, the remote API call has completed
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      //流量控制发送会结果
      quotas.produce.maybeRecordAndThrottle(
        request.session,
        request.header.clientId,
        numBytesAppended,
        produceResponseCallback)
    }

    def processingStatsCallback(processingStats: Map[TopicPartition, RecordsProcessingStats]): Unit = {
      processingStats.foreach { case (tp, info) =>
        updateRecordsProcessingStats(request, tp, info)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      // 内部topic只允许AdmintUtil调用
      val internalTopicsAllowed = request.header.clientId == AdminUtils.AdminClientId

      // 通过replicaManage进行append消息到副本leader中，并且等待其他副本复制完毕，callback方法会在超时或者ack数目得到满足时被调用
      replicaManager.appendRecords(
        timeout = produceRequest.timeout.toLong,
        requiredAcks = produceRequest.acks,
        internalTopicsAllowed = internalTopicsAllowed,
        isFromClient = true,
        entriesPerPartition = authorizedRequestInfo,
        responseCallback = sendResponseCallback,
        processingStatsCallback = processingStatsCallback)

      //正常append结束后清空requestrecords,否则如果请求被放入purgatory中，因为一直持引用，导致gc无法回收
      produceRequest.clearPartitionRecords()
    }
  }
```

　　Kafka的消息的发布与消费都是客户端从metadata中获取topicpartition的leader并与之交互，因此ReplicaManager.appendRecords()方法需要将消息写入本地副本中，并且等待其他副本对这条消息的复制，等待ack是否满足一定条件或超时就调用callback方法返回处理结果。下面看具体的代码分析:

```scala
  //ReplicaManager.scala
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    isFromClient: Boolean,
                    entriesPerPartition: Map[TopicPartition, MemoryRecords],
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                    delayedProduceLock: Option[Lock] = None,
                    processingStatsCallback: Map[TopicPartition, RecordsProcessingStats] => Unit = _ => ()) {
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds

      //作为leader写入本地消息日志中，首选杜绝写入内部topic,
      //此时已经不是leader，则抛异常
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        isFromClient = isFromClient, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      //生成produceStatus用于DelayedProduce的检查，包含offset等信息用于检查副本对leader的追赶程度
      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  result.info.lastOffset + 1, // required offset
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime, result.info.logStartOffset)) // response status
      }

      processingStatsCallback(localProduceResults.mapValues(_.info.recordsProcessingStats))

      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // DelayedProduce延迟等待主从同步结果
        
        // more code ...
      } else {
        // delayedProduceRequestRequired为false情况下立即返回
        val produceResponseStatus = produceStatus.mapValues(status => status.responseStatus)
        responseCallback(produceResponseStatus)
      }
    } else {
      //ack不在可接受范围之内，直接不处理请求，直接返回错误给客户端
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      responseCallback(responseStatus)
    }
  }
```

　　消息写入分区leader的本地日志后，需要等待其它副本对他的复制，满足一定的ack条件时才认为消息提交完成，返回结果给客户端。主从同步是followers发送fetchrequest同副本leader进行同步，来追赶leader。消息写入leader的日志后，使用DelayedProduce来检查副本对leader追赶程度，这里不详细介绍。

## <a id="conclusion">总结</a>

　　本篇介绍了一下Producer发送消息过程。Kafka在运行效率和一致性方面加入了一些设计，例如消息Accumulator，delayedProduce等，客户端只与leader交互，对每个客户端流量控制等，来提高Kafka集群整体运行效率。

　　至此本篇的内容介绍完毕，下一篇会介绍发送完消息后commit/abort事务。

## <a id="references">References</a>

* http://www.infoq.com/cn/articles/kafka-analysis-part-8?utm_source=articles_about_Kafka&utm_medium=link&utm_campaign=Kafka#

