---
layout: post
category: Kafka
title: Kafka事务消息过程分析(三)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　上篇介绍完KafkaProducer发送事务消息，但是这些消息对于Consumer是不可见的。只有事务提交完才是可见的。本篇介绍消息事务的commit/abort。

## <a id="KafkaProducer">KafkaProducer</a>

　　提交事务的入口为KafkaProducer.commitTransaction()。事务的commit/abort在Producer端是相似的，都是会调用TransactionManager.beginCompletingTransaction()方法通过构造EndTxnRequest进行commit/abort，区别是EndTxnRequest中TransactionResult有所区别，服务端会根据这个值做不同的处理。

```java
    //KafkaProducer.java
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        //通过transactionManager提交事务
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        //等待结果返回或超时
        result.await();
    }
```

　　TransactionManager.beginCompletingTransaction()方法根据传入的TransactionResult构造EndTxnRequest。EndTxnRequest中会传入transactionalId, producerId, epoch, transactionResult等信息用于提交到服务端用于事务的提交。EndTxnHandler用于接收到服务端返回的成功结果后的处理。在EndTxnHandler.handleResponse()中，如果没有任何错误则设置本地状态及清空相关的缓存队列用于下一次事务提交准备，如果有错误则根据不同的错误类型有不同的处理方法。对于Coordinator不可用重新将请求放入队列稍后待条件满足时重新提交。对于一些其它的情况，如epoch过期，txn状态异常等情况设置状态为错误或直接抛出异常。
　　特别地，在TransactionManager.beginCommit()方法中将本地状态置为COMMITTING_TRANSACTION，Sender线程maybeSendTransactionalRequest()方法中会检查这个状态，使得accumulator中的数据立即都置为ready状态，立即全部发送出去。

```java
    //TransactionManager.java
    public synchronized TransactionalRequestResult beginCommit() {
        ensureTransactional();
        maybeFailWithError();
        //修改本地状态，变为COMMITTING_TRANSACTION，accumulator会进行flush发送完所有数据
        transitionTo(State.COMMITTING_TRANSACTION);
        //传入TransactionResult.COMMIT，构造EndTxnRequest提交事务
        return beginCompletingTransaction(TransactionResult.COMMIT);
    }

    private TransactionalRequestResult beginCompletingTransaction(TransactionResult transactionResult) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());
        //将transactionalId, producerId, epoch, transactionResult填入EndTxnRequest，发送给服务端用于确认事务
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId, producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch, transactionResult);
        EndTxnHandler handler = new EndTxnHandler(builder);
        enqueueRequest(handler);
        return handler.result;
    }

    // 用于处理接收到服务端发来的应答的回调
    private class EndTxnHandler extends TxnRequestHandler {
        private final EndTxnRequest.Builder builder;

        //more code ...

        @Override
        public void handleResponse(AbstractResponse response) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) response;
            Errors error = endTxnResponse.error();

            if (error == Errors.NONE) {
                //如果返回的结果中无错误，则调用completeTransaction，对一些缓存清空，状态重置，以便于后续的事务提交
                completeTransaction();
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                //因为服务端故障等原因可能需要重新查找coordinator
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                //这种情况下将请求重新放入队列，等待重新查找到coordinator后再次处理
                reenqueue();
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.CONCURRENT_TRANSACTIONS) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH) {
                fatalError(error.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.INVALID_TXN_STATE) {
                fatalError(error.exception());
            } else {
                fatalError(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
            }
        }
    }

    //事务提交成功后需要将本地一些状态进行重置，对事务相关的缓存进行清空
    private synchronized void completeTransaction() {
        transitionTo(State.READY);
        lastError = null;
        transactionStarted = false;
        newPartitionsInTransaction.clear();
        pendingPartitionsInTransaction.clear();
        partitionsInTransaction.clear();
    }
```

## <a id="KafkaApis">KafkaApis</a>

　　事务的提交客户端部分处理比较简单，主要来看服务端的处理。KafkaApis里接收到END_TXN请求后调用handleEndTxnRequest()方法处理，而handleEndTxnRequest()方法中验证身份通过之后将请求中的数据取出来，交给TransactionCoordinator去处理。

```scala
  //KafkaApis.scala 
  def handle(request: RequestChannel.Request) {
        case ApiKeys.END_TXN => handleEndTxnRequest(request)
  }

  handleEndTxnRequest(request: RequestChannel.Request): Unit = {
    ensureInterBrokerVersion(KAFKA_0_11_0_IV0)
    val endTxnRequest = request.body[EndTxnRequest]
    val transactionalId = endTxnRequest.transactionalId

    if (authorize(request.session, Write, new Resource(TransactionalId, transactionalId))) {
      def sendResponseCallback(error: Errors) {
        def createResponse(requestThrottleMs: Int): AbstractResponse = {
          val responseBody = new EndTxnResponse(requestThrottleMs, error)
          trace(s"Completed ${endTxnRequest.transactionalId}'s EndTxnRequest with command: ${endTxnRequest.command}, errors: $error from client ${request.header.clientId}.")
          responseBody
        }
        //限流发送回结果
        sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.transactionalId,
        endTxnRequest.producerId,
        endTxnRequest.producerEpoch,
        endTxnRequest.command,
        sendResponseCallback)
    } else
      //身份验证失败发送TRANSACTIONAL_ID_AUTHORIZATION_FAILED错误给客户端
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
  }
```

　　TransactionCoordinator中首先进行一系列的异常情况处理，如果有异常，如producerid,epoch不符合预期值，则生成错误返回给客户端让客户端自行进行异常处理。正常情况下handleEndTransaction()预期的txnMetadata的状态为Ongoing，

```scala
  //TransactionCoordinator.scala
  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
        case None =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata.inLock {
            // exception handle code ...

            else txnMetadata.state match {
              case Ongoing =>
                //根据传入的TransactionResult决定下一个状态为Prepare or abort
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
                  PrepareCommit
                else
                  PrepareAbort

                if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
                  // We should clear the pending state to make way for the transition to PrepareAbort and also bump
                  // the epoch in the transaction metadata we are about to append.
                  txnMetadata.pendingState = None
                  txnMetadata.producerEpoch = producerEpoch
                }

                //调用prepareAbortOrCommit()设置TransactionMeta的状态为PrepareCommit/PrepareAbort和更新一些缓存信息
                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))
              // more code ...
            }
          }
      }

      preAppendResult match {
        case Left(err) =>
          debug(s"Aborting append of $txnMarkerResult to transaction log with coordinator and returning $err error to client for $transactionalId's EndTransaction request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>

          // 定义事务成功写入日志后的回调的方法
          // 该回调方法用于发送事务Marker，在发送marker请求前，需要做各种异常检查用于故障处理
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).right.flatMap {
                case None =>
                  // more code ...

                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata.inLock {
                      // error handle code ... 

                      else txnMetadata.state match {
                        // error handle code ... 

                        case PrepareCommit =>
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            //根据txnMetadata生成 newPreSendMetadata，prepareComplete置状态为PrepareCommit
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        //more code ...
                      }
                    }
                  } else {
                    //more code ...
                  }
              }

              preSendResult match {
                //more code ...

                case Right((txnMetadata, newPreSendMetadata)) =>
                  //执行到这个部分，是因为因为metarecord写入log成功后回调了sendTxnMarkersCallback()方法，因此这里立即返回结果给客户端，如果txnmarker请求成功前该broker挂掉，新的coordinator会有异常处理流程
                  responseCallback(Errors.NONE)

                  //开始发送send marker流程
                  txnMarkerChannelManager.addTxnMarkersToSend(transactionalId, coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
              //more code ...
            }
          }

          //写入事务到log中
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
      }
    }
  }
```


　　txnManager.appendTransactionToLog()这个方法之前篇幅中已经见过，所做的处理大致总结为根据输入参数生成record,写入分区log中，更新本地缓存状态，返回正常/异常结果等。这里不做更详细的阐述，我们只需要知道handleEndTransaction()中生成的newMetadata被写入分区log中，并完成主从同步后调用sendTxnMarkersCallback()返回结果给客户端及开启发送TxnMarkers流程。

　　TransactionMarkerChannelManager.addTxnMarkersToSend()中开启发送TxnMarkers流程，该方法主要作用为构建一个DelayedTxnMarker，用于延迟检查是否marker请求成功发送并正常返回。如果正常则调用appendToLogCallback()将TxnLogAppend写入日志，完成整个事务提交的最后一步骤。

```scala
// TransactionMarkerChannelManager.scala
  def addTxnMarkersToSend(transactionalId: String,
                          coordinatorEpoch: Int,
                          txnResult: TransactionResult,
                          txnMetadata: TransactionMetadata,
                          newMetadata: TxnTransitMetadata): Unit = {
    // DelayedTxnMarker成功的callback，将metadata包装起来写入log中
    def appendToLogCallback(error: Errors): Unit = {
      error match {
        case Errors.NONE =>
          txnStateManager.getTransactionState(transactionalId) match {
            // more code ...

            case Right(Some(epochAndMetadata)) =>
              // 当epoch符合预期时调用tryAppendToLog方法将事务提交写入log中，
              // tryAppendToLog方法最核心的是使用appendTransactionToLog方法将metadata中一些数据包装起来写入log中和做异常检测重试等
              if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                tryAppendToLog(TxnLogAppend(transactionalId, coordinatorEpoch, txnMetadata, newMetadata))
              } 
              // more code ...
          }
          // more code ...
      }
    }

    // 构造一个DelayedTxnMarker，用于检查marker请求是否正常执行完毕，
    // DelayedTxnMarker中的tryComplete()方法检测metadata中topicpartition是否为空，检测WriteTxnMarkersRequest请求正常执行完毕，
    // 因为正常情况下WriteTxnMarkersRequest的回调方法中正常情况会remove掉topicpartition
    val delayedTxnMarker = new DelayedTxnMarker(txnMetadata, appendToLogCallback, txnStateManager.stateReadLock)
    txnMarkerPurgatory.tryCompleteElseWatch(delayedTxnMarker, Seq(transactionalId))

    // 向tp对应的broker发送WriteTxnMarkersRequest用于写入marker
    addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.producerId, txnMetadata.producerEpoch, txnResult, coordinatorEpoch, txnMetadata.topicPartitions.toSet)
  }

  def addTxnMarkersToBrokerQueue(transactionalId: String, producerId: Long, producerEpoch: Short,
                                 result: TransactionResult, coordinatorEpoch: Int,
                                 topicPartitions: immutable.Set[TopicPartition]): Unit = {
    val txnTopicPartition = txnStateManager.partitionFor(transactionalId)
    val partitionsByDestination: immutable.Map[Option[Node], immutable.Set[TopicPartition]] = topicPartitions.groupBy { topicPartition: TopicPartition =>
      metadataCache.getPartitionLeaderEndpoint(topicPartition.topic, topicPartition.partition, interBrokerListenerName)
    }

    for ((broker: Option[Node], topicPartitions: immutable.Set[TopicPartition]) <- partitionsByDestination) {
      broker match {
        case Some(brokerNode) =>
          // 构造Txn marker信息
          val marker = new TxnMarkerEntry(producerId, producerEpoch, coordinatorEpoch, result, topicPartitions.toList.asJava)
          val txnIdAndMarker = TxnIdAndMarkerEntry(transactionalId, marker)

          if (brokerNode == Node.noNode) {
            // 收集broker未知的topicpartition的marker,sender线程会进入查找对应的broker
            markersQueueForUnknownBroker.addMarkers(txnTopicPartition, txnIdAndMarker)
          } else {
            //broker已知，写入markersQueuePerBroker队列中
            //markersQueuePerBroker队列为每个broker对应一个队列，类似于客户端到服务端的网络请求合并处理
            addMarkersForBroker(brokerNode, txnTopicPartition, txnIdAndMarker)
          }

        case None =>
          // 异常情况处理
      }
    }

    wakeup()
  }
```




