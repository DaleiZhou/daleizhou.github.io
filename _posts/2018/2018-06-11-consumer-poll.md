---
layout: post
category: Kafka
title: KafkaConsumer(一)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　从这篇开始从KafkaConsumer为切入点，大概用四篇左右学习一下消息的消费相关的调用的实现，介绍包含客户端与服务端的代码细节。本篇涉及KafkaConsumer订阅主题的相关实现。

## <a id="subscribe">subscribe</a>

　　Kafka的subscribe()方法有两种订阅方法，一种是直接通过Topic进行调用，另一种是通过传入Pattern的方式进行主题的订阅。下面分别来介绍这两种。

```java
//KafkaConsumer.java
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        //获得一个轻量锁，保证是未关闭状态
        acquireAndEnsureOpen();
        try {
            // 要求订阅的Topics不能为null
            if (topics == null) {
                throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
            } else if (topics.isEmpty()) {
                //如果topics.isEmpty,则意味着和清空订阅列表作用相同
                this.unsubscribe();
            } else {
                for (String topic : topics) {
                    // 订阅的topic列表所有topic都不能为null or 不能为空字符串
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
                }
                // 内部的assignors不能为empty
                throwIfNoAssignorsConfigured();
                // 设置SubscriptionState的subscriptionType=AUTO_TOPICS及更新subscription，groupSubscription
                this.subscriptions.subscribe(new HashSet<>(topics), listener);
                // 上一句将新的topic添加入groupSubscription，这里更新metadata,更新其内部的topics列表
                metadata.setTopics(subscriptions.groupSubscription());
            }
        } finally {
            // 释放轻量锁
            release();
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

```

　　通过KafkaConsumer.subscribe()的方式进行主题订阅内部实现细节很简单，设置subscriptionType，更新缓存信息，如metadata的内部主题等。下面我们来看通过设置Pattern的方式订阅主题。

```java
    // KafkaConsumer.java
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        //pattern不能为null
        if (pattern == null)
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be null");

        acquireAndEnsureOpen();
        try {
            throwIfNoAssignorsConfigured();
            // 更新subscriptions的订阅pattern缓存
            // setSubscriptionType = AUTO_PATTERN
            this.subscriptions.subscribe(pattern, listener);
            // 设置稍后更新kafka cluster的metadata缓存的标识
            this.metadata.needMetadataForAllTopics(true);
            // 根据pattern进行订阅主题
            this.coordinator.updatePatternSubscription(metadata.fetch());
            this.metadata.requestUpdate();
        } finally {
            release();
        }
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }
```

　　通过Pattern进行Topic的订阅与通过Topic订阅类似，区别在于设置setSubscriptionType及订阅主题。区别在于通过Pattern形式的主题订阅在metadata更新时会更新订阅列表。在ConsumerCoordinator初始化时会调用addMetadataListener()为metadata添加更新时的回调策略。metadata有更新时，更新客户端的订阅列表，必要的时候重新进行负载均衡及更新metadata。

```java
    // ConsumerCoordinator.java
    public ConsumerCoordinator(...) {
        addMetadataListener();
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                // other code ...

                if (subscriptions.hasPatternSubscription())
                    // metadata有更新，则更新订阅列表
                    updatePatternSubscription(cluster);

                // 检查是否rebalance
                if (subscriptions.partitionsAutoAssigned()) {
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    if (!snapshot.equals(metadataSnapshot))
                        metadataSnapshot = snapshot;
                }

                //如果metadata.topics()与unavailableTopics有交集，则更新metadata
                if (!Collections.disjoint(metadata.topics(), unavailableTopics))
                    metadata.requestUpdate();
            }
        });
    }
```

　　其实通过Topic和Pattern进行主题的订阅是相似的，形式上的区别是一个直接指明主题列表，一个是通过Pattern的方式指定，另一个重要区别是通过Pattern方式的订阅，会在订阅动作完成后，如果有新的符合Pattern的主题的加入，则会更新订阅列表将其加入进来。

## <a id="subscribe">subscribe</a>

　　kafka提供了另一种订阅模式:assign模式。这是一种由用户手工分配TopicPartition列表的非增量订阅模式。从实现上，assign模式不会使用Consumer Group的管理功能，因此如果consumer group的成员、cluster和topic metadata更新都不会触发rebalance操作，需要用户自行处理。

```java
    //KafkaConsumer.java
    public void assign(Collection<TopicPartition> partitions) {
        acquireAndEnsureOpen();
        try {
            if (partitions == null) {
                throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
            } else if (partitions.isEmpty()) {
                // 同样地，如果传入的partitions为empty，效果和unsubscribe()相同
                this.unsubscribe();
            } else {
                // 从手工指定的topicpartition列表中获取可用的topic列表用于订阅
                Set<String> topics = new HashSet<>();
                for (TopicPartition tp : partitions) {
                    String topic = (tp != null) ? tp.topic() : null;
                    if (topic == null || topic.trim().isEmpty())
                        throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
                    topics.add(topic);
                }

                // 在设置新的assign时，如果客户端开启了auto-commit，则旧的assign的异步commit需要全部提交
                this.coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

                this.subscriptions.assignFromUser(new HashSet<>(partitions));
                metadata.setTopics(topics);
            }
        } finally {
            release();
        }
    }
```


## TBD
