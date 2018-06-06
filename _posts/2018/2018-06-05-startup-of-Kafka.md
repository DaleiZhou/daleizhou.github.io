---
layout: post
category: Kafka
title: Kafka事务消息过程分析
---

## 内容 
>Status: Draft

　　Kafka从0.11.0.0包含两个比较大的特性，exactly once delivery和transactional transactional messaging。

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
　　从源码上看出在maybeSendTransactionalRequest()方法中如果事务协调node为null或不是ready状态则在transactionManager.lookupCoordinator()中插入发送FindCoordinatorRequest请求，重入maybeSendTransactionalRequest方法后随便一个正常的node就可以进行查找事务协调节点。时限取出来的InitProducerIdRequest请求会重新放到发送队列中。再重入时会因为协调员存在而完成初始化ProducerId,Epoch的请求。
FindCoordinatorHandler的回调很简单，更新TransactionManager.transactionCoordinator用于Producer的事务处理。InitProducerIdHandler回调主要是更新本地ProducerId，epoch，并且修改事务状态为Ready。通过ProducerId，epoch就可以进行跨session的事务控制。

　　至此客户端部分事务初始化介绍完毕。

## <a id="brokerapi">KafkaApis</a>

　　事务初始化阶段Producer主要发送了FindCoordinatorRequest，InitProducerIdHandler来确定协调node和获取ProducerIdAndEpoch。

**FindCoordinatorRequest处理**

**InitProducerIdHandler处理**

## <a id="exceptionhandle">异常处理</a>

## <a id="conclusion">总结</a>

## <a id="references">References</a>

>https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging

>https://cwiki.apache.org/confluence/display/KAFKA/Idempotent+Producer

>https://cwiki.apache.org/confluence/display/KAFKA/Transactional+Messaging+in+Kafka

>http://www.infoq.com/cn/articles/kafka-analysis-part-8?utm_source=articles_about_Kafka&utm_medium=link&utm_campaign=Kafka#

