---
layout: post
category: Kafka
title: Kafka事务消息过程分析(三)
---

## 内容 
>Status: Draft

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
        result.await();
    }
```

　　TransactionManager.beginCompletingTransaction()方法根据传入的TransactionResult构造EndTxnRequest。EndTxnRequest中会传入transactionalId, producerId, epoch, transactionResult等信息用于提交到服务端用于事务的提交。EndTxnHandler用于接收到服务端返回的成功结果后的处理。在EndTxnHandler.handleResponse()中，如果没有任何错误则设置本地状态及清空相关的缓存队列用于下一次事务提交准备，如果有错误则根据不同的错误类型有不同的处理方法。对于Coordinator不可用重新将请求放入队列稍后待条件满足时重新提交。对于一些其它的情况，如epoch过期，txn状态异常等情况设置状态为错误或直接抛出异常。

```java
    //TransactionManager.java
    public synchronized TransactionalRequestResult beginCommit() {
        ensureTransactional();
        maybeFailWithError();
        //修改本地状态，变为COMMITTING_TRANSACTION
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

　　事务的提交客户端部分处理比较简单，主要来看服务端的处理。

```scala
  //KafkaApis.scaladef 
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
        sendResponseMaybeThrottle(request, createResponse)
      }

      txnCoordinator.handleEndTransaction(endTxnRequest.transactionalId,
        endTxnRequest.producerId,
        endTxnRequest.producerEpoch,
        endTxnRequest.command,
        sendResponseCallback)
    } else
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new EndTxnResponse(requestThrottleMs, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED))
  }
```
