---
layout: post
category: Kafka
title: Kafka事务消息过程分析(一)
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
            //Kafka的源码中提供了DefaultPartitioner，实现了partition()方法，具体过程就是根据Key, DefaultPartitioner维护的一个<topic, counter>计数器等数据简单的取模得到对应的partition
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