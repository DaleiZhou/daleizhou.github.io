---
layout: post
category: Kafka
title: Kafka事务消息过程分析
---

## 内容 
>Status: Draft

Kafka从0.11.0.0包含两个比较大的特性，exactly once delivery和transactional transactional messaging。

## 事务过程
从客户端角度而言，完整的事务使用一般有如下几个调用：
```java
KafkaProducer.initTransactions() // 事务初始化
KafkaProducer.beginTransaction() // 开始事务
KafkaProducer.send() // 发送数据
KafkaProducer.sendOffsetsToTransaction() //发送offset到消费组协调者
KafkaProducer.commitTransaction() // 提交事务
KafkaProducer.abortTransaction() // 终止事务
```

## initTransactions
初始化事务时检查transactionManager，initTransactionsResult，如果initTransactionsResult为null(初始化过程中不为null)则修改状态并生成InitProducerIdRequest请求获取ProducerId和Epoch的值，用于事务交互过程中服务端进行事务处理。
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

Sender是KafkaProducer发送请求到与Kafka集群的后台线程，主要用于更新metadata和发送produce请求到合适的broker节点。run()方法循环调用run(long now)。
```java
void run(long now) {
        if (transactionManager != null) {
            try {
                if (transactionManager.shouldResetProducerStateAfterResolvingSequences())
                    // Check if the previous run expired batches which requires a reset of the producer state.
                    transactionManager.resetProducerId();

                if (!transactionManager.isTransactional()) {
                    // this is an idempotent producer, so make sure we have a producer id
                    maybeWaitForProducerId();
                } else if (transactionManager.hasUnresolvedSequences() && !transactionManager.hasFatalError()) {
                    transactionManager.transitionToFatalError(new KafkaException("The client hasn't received acknowledgment for " +
                            "some previously sent messages and can no longer retry them. It isn't safe to continue."));
                } else if (transactionManager.hasInFlightTransactionalRequest() || maybeSendTransactionalRequest(now)) {
                    // as long as there are outstanding transactional requests, we simply wait for them to return
                    client.poll(retryBackoffMs, now);
                    return;
                }

                // do not continue sending if the transaction manager is in a failed state or if there
                // is no producer id (for the idempotent case).
                if (transactionManager.hasFatalError() || !transactionManager.hasProducerId()) {
                    RuntimeException lastError = transactionManager.lastError();
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    client.poll(retryBackoffMs, now);
                    return;
                } else if (transactionManager.hasAbortableError()) {
                    accumulator.abortUndrainedBatches(transactionManager.lastError());
                }
            } catch (AuthenticationException e) {
                // This is already logged as error, but propagated here to perform any clean ups.
                log.trace("Authentication exception while processing transactional request: {}", e);
                transactionManager.authenticationFailed(e);
            }
        }

        long pollTimeout = sendProducerData(now);
        client.poll(pollTimeout, now);
    }
    ```

## 总结



##References

>https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging

>https://cwiki.apache.org/confluence/display/KAFKA/Idempotent+Producer

>https://cwiki.apache.org/confluence/display/KAFKA/Transactional+Messaging+in+Kafka

>http://www.infoq.com/cn/articles/kafka-analysis-part-8?utm_source=articles_about_Kafka&utm_medium=link&utm_campaign=Kafka#

