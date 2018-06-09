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
                completeTransaction();
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
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
```