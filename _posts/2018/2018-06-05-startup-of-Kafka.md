---
layout: post
category: Kafka
title: Kafka事务消息过程分析(一)
---

## 内容 

>代码版本: 2.0.0-SNAPSHOT
>Blog tatus: Draft

　　Kafka从0.11.0.0包含两个比较大的特性，exactly once delivery和transactional transactional messaging。从消息的生产端保证了跨session的事务消息投递。这里计划从源码实现的角度跟踪分析，因为源码贴的太多，分大概三到四篇来说明。这里是第一篇：事务的初始化。

## <a id="baseApi">基本调用</a>

　　从客户端角度而言，完整的事务使用一般有如下几个调用：

```java
KafkaProducer.initTransactions() // 事务初始化
KafkaProducer.beginTransaction() // 开始事务
KafkaProducer.send() // 发送数据
KafkaProducer.sendOffsetsToTransaction() //发送offset到消费组协调者
KafkaProducer.commitTransaction() // 提交事务
KafkaProducer.abortTransaction() // 终止事务
```
　　下面分别从客户端和服务端两个角度来看事务初始化过程。

## <a id="producer">KafkaProducer</a>

　　初始化事务时检查transactionManager，initTransactionsResult，如果initTransactionsResult为null(初始化过程中不为null)则修改状态并生成InitProducerIdRequest请求获取ProducerId和Epoch的值，用于事务交互过程中服务端进行事务处理。

**KafkaProducer**
```java
    public void initTransactions() {
        //如果transactionManager为null则无法使用事务，抛出异常
        throwIfNoTransactionManager();
        if (initTransactionsResult == null) {
            initTransactionsResult = transactionManager.initializeTransactions();
            //唤醒sender线程发送请求
            sender.wakeup();
        }

        try {
            if (initTransactionsResult.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)) {
                initTransactionsResult = null;
            } else {
                throw new TimeoutException("Timeout expired while initializing transactional state in " + maxBlockTimeMs + "ms.");
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Initialize transactions interrupted.", e);
        }
    }
```

　　初始化过程中调用TransactionManager.initializeTransactions()方法用于从TransactionCoordinate中获得ProducerId,Epoch。为了提高运行的效率，Kafka的请求通过后台专门的线程发送，后面会有专门的主题讲解，这里只需要了解请求通过enqueueRequest压入请求队列即可。

```java
    public synchronized TransactionalRequestResult initializeTransactions() {
        ensureTransactional(); //保证用户有设置transactionalId
        transitionTo(State.INITIALIZING); // -> INITIALIZING
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE); 
        this.nextSequence.clear();
        InitProducerIdRequest.Builder builder = new InitProducerIdRequest.Builder(transactionalId, transactionTimeoutMs);
        //InitProducerIdHandler继承自TxnRequestHandler, 类的定义中重写了
        //handleResponse方法，主要根据服务器返回初始化的结果更新本地ProducerId和Epoch
        InitProducerIdHandler handler = new InitProducerIdHandler(builder);
        enqueueRequest(handler); 
        return handler.result;
    }
```

**Sender**

　　Sender是KafkaProducer发送请求到与Kafka集群的后台线程，主要用于更新metadata和发送produce请求到合适的broker节点。run()方法循环调用run(long now)。

```java
//Sender.java
    void run(long now) {
        //other code...

        else if (transactionManager.hasInFlightTransactionalRequest() || maybeSendTransactionalRequest(now)) {
            //在初始化阶段最重要的是maybeSendTransactionalRequest()方法，
            //因为InitProducerIdRequest是需要发送到对应的Coordinator上(needsCoordinator() == true), 
            //因此在发送请求之前需要保证节点缓存里有相应的值，如果没有则优先查找Coordinator

            // as long as there are outstanding transactional requests, we simply wait for them to return
            client.poll(retryBackoffMs, now);
            return;
        }

        // other code ...
    }

    private boolean maybeSendTransactionalRequest(long now) {
        if (transactionManager.isCompleting() && accumulator.hasIncomplete()) {
            if (transactionManager.isAborting())
                accumulator.abortUndrainedBatches(new KafkaException("Failing batch since transaction was aborted"));

            if (!accumulator.flushInProgress())
                accumulator.beginFlush();
        }

        //获取TransactionManager.pendingRequests队列中待处理的请求
        //加入这时获取到之前的InitProducerIdRequest
        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequestHandler(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        while (!forceClose) {
            Node targetNode = null;
            try {
                //InitProducerIdRequest.needsCoordinator() == true
                if (nextRequestHandler.needsCoordinator()) {
                    targetNode = transactionManager.coordinator(nextRequestHandler.coordinatorType()); //TRANSACTIONcoordinatorType() == TRANSACTION
                    if (targetNode == null) {
                        transactionManager.lookupCoordinator(nextRequestHandler);
                        break;
                    }

                    if (!NetworkClientUtils.awaitReady(client, targetNode, time, requestTimeout)) {
                        transactionManager.lookupCoordinator(nextRequestHandler);
                        break;
                    }
                } else {
                    //重入这个方法后因为插入了FindCoordinatorRequest，但因为其不需要协调员，因此取一个正常能联通的node发送请求
                    targetNode = awaitLeastLoadedNodeReady(requestTimeout);
                }
                //
                if (targetNode != null) {
                    if (nextRequestHandler.isRetry())
                        time.sleep(nextRequestHandler.retryBackoffMs());

                    ClientRequest clientRequest = client.newClientRequest(targetNode.idString(),
                            requestBuilder, now, true, nextRequestHandler);
                    transactionManager.setInFlightTransactionalRequestCorrelationId(clientRequest.correlationId());
                    log.debug("Sending transactional request {} to node {}", requestBuilder, targetNode);

                    client.send(clientRequest, now);
                    return true;
                }
            } catch (IOException e) {
                log.debug("Disconnect from {} while trying to send request {}. Going " +
                        "to back off and retry", targetNode, requestBuilder);
                if (nextRequestHandler.needsCoordinator()) {
                    // We break here so that we pick up the FindCoordinator request immediately.
                    transactionManager.lookupCoordinator(nextRequestHandler);
                    break;
                }
            }

            time.sleep(retryBackoffMs);
            metadata.requestUpdate(); //如果执行到这里说明没有直接return出去，需要再次进行循环，这里更新metadata
        }

        transactionManager.retry(nextRequestHandler);
        return true;
    }    
```
　　从源码上看出在maybeSendTransactionalRequest()方法中如果事务协调node为null或不是ready状态则在transactionManager.lookupCoordinator()中插入发送FindCoordinatorRequest请求，重入maybeSendTransactionalRequest方法后随便一个正常的node就可以进行查找事务协调节点。事先取出来的InitProducerIdRequest请求会重新放到发送队列中。再重入时会因为协调员存在而完成初始化ProducerId,Epoch的请求。

　　FindCoordinatorHandler的回调很简单，更新TransactionManager.transactionCoordinator缓存保存对应的node信息用于Producer的事务处理。InitProducerIdHandler回调主要是更新本地ProducerId，epoch，并且修改事务状态为Ready。通过ProducerId，epoch就可以进行跨session的事务控制。

　　如果客户端事务的特性未被开启，InitPidRequest可以被发送至任何borker上，同样地可以获得ProducerAndEpoch,但是这时只能保证Session内的事务特性。

　　至此客户端部分事务初始化介绍完毕。

## <a id="brokerapi">KafkaApis</a>

　　事务初始化阶段Producer主要发送了FindCoordinatorRequest，InitProducerIdHandler来确定协调node和获取ProducerIdAndEpoch。

**FindCoordinatorRequest处理**

　　发送客户端获取ProducerIdAndEpoch之前通过请求获得对应的Corrodinator,服务端处理处理逻辑的入口在KafkaApis中。下面看具体代码:

```scala
    //KafkaApis.scala
    def handle(request: RequestChannel.Request) {
      //other code ...
      request.header.apiKey match {
        case ApiKeys.FIND_COORDINATOR => handleFindCoordinatorRequest(request)
        case ApiKeys.INIT_PRODUCER_ID => handleInitProducerIdRequest(request)
            } catch {
      case e: FatalExitError => throw e
      //other code...
    }

    def handleFindCoordinatorRequest(request: RequestChannel.Request) {
        val findCoordinatorRequest = request.body[FindCoordinatorRequest]

        //other code..
        else if (findCoordinatorRequest.coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION &&
            !authorize(request.session, Describe, new Resource(TransactionalId, findCoordinatorRequest.coordinatorKey))) //处理身份验证无法通过的分支
          sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        else {
          // get metadata (and create the topic if necessary)
          // 从请求中获取metadata,必要时需要创建topic
          val (partition, topicMetadata) = findCoordinatorRequest.coordinatorType match {
            // other code..
            // 处理TRANSACTION查找Coordinator请求
            case FindCoordinatorRequest.CoordinatorType.TRANSACTION =>
              // 根据transactionalId通过简单的hash方式获取对应的分区：
              // Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount 获取对应的分区
              val partition = txnCoordinator.partitionFor(findCoordinatorRequest.coordinatorKey)
              //获取或者创建metadata，内部的topic名为__transaction_state，
              //用于保存各个TopicMetadata数据，包含Topic的分区信息，副本leader信息等
              val metadata = getOrCreateInternalTopic(TRANSACTION_STATE_TOPIC_NAME, request.context.listenerName)
              (partition, metadata)

            //other code..
          }

          def createResponse(requestThrottleMs: Int): AbstractResponse = {
            val responseBody = if (topicMetadata.error != Errors.NONE) {
              new FindCoordinatorResponse(requestThrottleMs, Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
            } else {
              //为了保证一致性，kafka的消息生产者和消费者只与分区的leader交互
              //从topicmetadata中根据由hash得到的分区得到对应的分区leader
              val coordinatorEndpoint = topicMetadata.partitionMetadata.asScala
                .find(_.partition == partition)
                .map(_.leader)
                .flatMap(p => Option(p))
              //获取到endpoint如果正常则包装结果返回给客户端，否则设置error并返回给客户端
              coordinatorEndpoint match {
                case Some(endpoint) if !endpoint.isEmpty =>
                  new FindCoordinatorResponse(requestThrottleMs, Errors.NONE, endpoint)
                case _ =>
                  new FindCoordinatorResponse(requestThrottleMs, Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode)
              }
            }
            trace("Sending FindCoordinator response %s for correlation id %d to client %s."
              .format(responseBody, request.header.correlationId, request.header.clientId))
            responseBody
          }
        sendResponseMaybeThrottle(request, createResponse)
        }
    }
```

　　总得说来过程大概归纳为下面几个步骤：

        1. 根据TransactionId做简单hash得到对应的partition
        2. 如果没有对应的topicmetadata,则创建出来
        3. 根据metadata得到分区对应的leader,返回给客户端，或者返回COORDINATOR_NOT_AVAILABLE错误给客户端

**InitProducerIdRequest处理**
    
　　服务端对InitProducerIdRequest的处理用于返回ProducerIdAndEpoch,用于事务的处理，处理的入口一样在KafkaApis的handle()方法中，下面看具体代码:

```scala
   //kafkaApis.scala

   def handleInitProducerIdRequest(request: RequestChannel.Request): Unit = {
    val initProducerIdRequest = request.body[InitProducerIdRequest]
    val transactionalId = initProducerIdRequest.transactionalId
    //other code ...

    //回调发送结果，将InitProducerIdResult发送回客户端
    def sendResponseCallback(result: InitProducerIdResult): Unit = {
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        val responseBody = new InitProducerIdResponse(requestThrottleMs, result.error, result.producerId, result.producerEpoch)
        trace(s"Completed $transactionalId's InitProducerIdRequest with result $result from client ${request.header.clientId}.")
        responseBody
      }
      sendResponseMaybeThrottle(request, createResponse)
    }

    txnCoordinator.handleInitProducerId(transactionalId, initProducerIdRequest.transactionTimeoutMs, sendResponseCallback)
  }

```

　　KafkaApis中handleInitProducerIdRequest处理过程很简单，先进行身份验证等操作，通过调用txnCoordinator.handleInitProducerId()方法构造结果,并在这个方法里并通过sendResponseCallback()回调发送结果到客户端。

```scala
//TransactionCoordinator.scala
  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           responseCallback: InitProducerIdCallback): Unit = {

    if (transactionalId == null) {
      // 如果Producer端未设置transactionalId则在producerIdManager批量id号里
      // 获取一个ProducerId返回给客户端
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    } else if (transactionalId.isEmpty) {
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else {
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None =>
          //如果本地缓存里没有TransactionId对应的TransactionMeta数据则生成一个新的并更新缓存
          val producerId = producerIdManager.generateProducerId()
          val createdMetadata = new TransactionMetadata(transactionalId = transactionalId,
            producerId = producerId,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            txnTimeoutMs = transactionTimeoutMs,
            state = Empty,
            topicPartitions = collection.mutable.Set.empty[TopicPartition],
            txnLastUpdateTimestamp = time.milliseconds())
          txnManager.putTransactionStateIfNotExists(transactionalId, createdMetadata)

        //如果本地缓存有则直接取出来
        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.right.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          txnMetadata.inLock {
            //根据上述结果准备返回结果
            prepareInitProduceIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata)
          }
      }

      result match {
        case Left(error) =>
          responseCallback(initTransactionError(error))

        case Right((coordinatorEpoch, newMetadata)) =>
          if (newMetadata.txnState == PrepareEpochFence) {
            // abort the ongoing transaction and then return CONCURRENT_TRANSACTIONS to let client wait and retry
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              }
            }

            handleEndTransaction(transactionalId,
              newMetadata.producerId,
              newMetadata.producerEpoch,
              TransactionResult.ABORT,
              sendRetriableErrorCallback)
          } else {
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                info(s"Initialized transactionalId $transactionalId with producerId ${newMetadata.producerId} and producer " +
                  s"epoch ${newMetadata.producerEpoch} on partition " +
                  s"${Topic.TRANSACTION_STATE_TOPIC_NAME}-${txnManager.partitionFor(transactionalId)}")
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                info(s"Returning $error error code to client for $transactionalId's InitProducerId request")
                responseCallback(initTransactionError(error))
              }
            }

            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendPidResponseCallback)
          }
      }
    }
  }    

  private def prepareInitProduceIdTransit(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          coordinatorEpoch: Int,
                                          txnMetadata: TransactionMetadata): ApiResult[(Int, TxnTransitMetadata)] = {
    if (txnMetadata.pendingTransitionInProgress) {
      // return a retriable exception to let the client backoff and retry
      Left(Errors.CONCURRENT_TRANSACTIONS)
    } else {
      // caller should have synchronized on txnMetadata already
      txnMetadata.state match {
        //other code ...

        case CompleteAbort | CompleteCommit | Empty =>
          //初次生成或者是已经完全结束时
          //如果一个producerId对应的epoch耗尽则生成一个新的producerId，epoch重头开始
          val transitMetadata = if (txnMetadata.isProducerEpochExhausted) {
            val newProducerId = producerIdManager.generateProducerId()
            txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds())
          } else {
            //否则根据沿用同一个producerId，但epoch自增1
            txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, time.milliseconds())
          }

          Right(coordinatorEpoch, transitMetadata)

        //other code ...
      }
    }
  }

  

```

　　handleInitProducerId()中检查如果transactionalId为null,则在producerIdManager中获取一个递增的ProducerId,因为ProducerId是全局唯一的，但如果每次都和Zookeeper交互保证唯一性则效率比较低，因此Kafka解决的思路是每次获取一批id号用于分配，如果消耗光了再与Zookeeper交互重新获取一批。

　　在handleInitProducerId()的最后，如果是正常情况下会调用txnManager.appendTransactionToLog()中将结果接入日志中。

```scala
  def appendTransactionToLog(transactionalId: String,
                             coordinatorEpoch: Int,
                             newMetadata: TxnTransitMetadata,
                             responseCallback: Errors => Unit,
                             retryOnError: Errors => Boolean = _ => false): Unit = {

    // generate the message for this transaction metadata
    val keyBytes = TransactionLog.keyToBytes(transactionalId)
    val valueBytes = TransactionLog.valueToBytes(newMetadata)
    val timestamp = time.milliseconds()

    val records = MemoryRecords.withRecords(TransactionLog.EnforcedCompressionType, new SimpleRecord(timestamp, keyBytes, valueBytes))
    val topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionFor(transactionalId))
    val recordsPerPartition = Map(topicPartition -> records)

    // set the callback function to update transaction status in cache after log append completed
    //写入事务log之后调用的回调方法，用于更新内存中缓存的事务状态
    def updateCacheCallback(responseStatus: collection.Map[TopicPartition, PartitionResponse]): Unit = {
      
      //other code ...

      if (responseError == Errors.NONE) {
        // 没有错误的情况下更新缓存，只修改需要改的地方，不需要整体更新，不需要锁将整个缓存锁起来
        getTransactionState(transactionalId) match {
          //other code ...
          case Right(Some(epochAndMetadata)) =>
            val metadata = epochAndMetadata.transactionMetadata

            metadata.inLock {
              if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) 
                // other code ...
                // 如果topicpartition迁移了则发送NOT_COORDINATOR用于客户端重试
                responseError = Errors.NOT_COORDINATOR
              } else {
                // 更新缓存信息
                metadata.completeTransitionTo(newMetadata)
                debug(s"Updating $transactionalId's transaction state to $newMetadata with coordinator epoch $coordinatorEpoch for $transactionalId succeeded")
              }
            }

          case Right(None) =>
            // other code ...
            // 客户端处理
            responseError = Errors.NOT_COORDINATOR
        }
      } 

      // other code ...

      responseCallback(responseError)
    }

    inReadLock(stateLock) {
      // 直到写本地日志结束才放锁,这是为了避免在check操作完成之后，
      // appendRecords()操作完成之前发生了一个迁出迁入的完整过程使得更高的epoch写入到log中，就会导致appendRecords写入一个旧的epoch值，
      // followes上的副本还可以复制，让日志处于一个不正常的状态
 
      getTransactionState(transactionalId) match {
        case Left(err) =>
          responseCallback(err)

        case Right(None) =>
          // the coordinator metadata has been removed, reply to client immediately with NOT_COORDINATOR
          responseCallback(Errors.NOT_COORDINATOR)

        case Right(Some(epochAndMetadata)) =>
          val metadata = epochAndMetadata.transactionMetadata

          val append: Boolean = metadata.inLock {
            if (epochAndMetadata.coordinatorEpoch != coordinatorEpoch) {
              // the coordinator epoch has changed, reply to client immediately with NOT_COORDINATOR
              responseCallback(Errors.NOT_COORDINATOR)
              false
            } else {
              // 加锁之后不会更新metadata,因此如果epoch值是预期的，则append到日志中
              true
            }
          }
          if (append) {
            //如果metadata里的epoch值与预期的一样则写入本地事务log中
            replicaManager.appendRecords(
                newMetadata.txnTimeoutMs.toLong,
                TransactionLog.EnforcedRequiredAcks,
                internalTopicsAllowed = true,
                isFromClient = false,
                recordsPerPartition,
                updateCacheCallback,
                delayedProduceLock = Some(stateLock.readLock))

              // other code ...
          }
      }
    }
  }

```

　　在Replicamanager中appendRecords操作将MemoryRecords写入partition对应的leader中，并且等待其它副本的同步，当且ack数量满足条件或者超时立即调用callback，主从同步部分会有一个专门的章节介绍，这里先理解Kafka保证写入了leader和follow的日志中。

　　至此，事务初始化服务器端部分介绍完毕。

## <a id="conclusion">总结</a>
　　此篇为Kafka事务分析中的第一个部分，用户显示设置TransactionId，程序启动初始化事务时传入服务端，服务端根据这个id号做简单hash确定Coordinator。Producer和这个Coordinator交互获取ProducerIdAndEpoch，这个过程对用户是透明的。获取到的ProducerIdAndEpoch可以用于跨session的事务控制，具体的作用会在随后几篇博客中具体介绍。

## <a id="references">References</a>

* https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
* https://cwiki.apache.org/confluence/display/KAFKA/Idempotent+Producer
* https://cwiki.apache.org/confluence/display/KAFKA/Transactional+Messaging+in+Kafka
* http://www.infoq.com/cn/articles/kafka-analysis-part-8?utm_source=articles_about_Kafka&utm_medium=link&utm_campaign=Kafka#

