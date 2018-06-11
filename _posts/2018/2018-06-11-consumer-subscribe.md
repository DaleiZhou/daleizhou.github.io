---
layout: post
category: Kafka
title: Kafka Consumer(一)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT, 涉及内容有subscribe, assign, join group

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

## <a id="assign">assign</a>

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

                // 更新assignment，设定用户指定的partitions
                this.subscriptions.assignFromUser(new HashSet<>(partitions));
                metadata.setTopics(topics);
            }
        } finally {
            release();
        }
    }
```

　　KafkaConsumer.assign()方法调用了subscriptions.assignFromUser()进行手工设定assignment。另一种设定方式为assignFromSubscribed，这个将在后面讲解poll()时出现。使用subscribe和assign模式订阅消息的一个重要区别体现在GroupCoordinator是否可以感知管理这个Consumer实例。带着这样的问题我们来看Consumer在订阅主题之后是如何进行消息消费的。入口从KafkaConsumer.poll()方法开始。

```java
    //KaflaConsumer.java
    // 传入timeout, 循环拉取数据，如果超时还是没有数据，则抛出异常
    public ConsumerRecords<K, V> poll(long timeout) {
        acquireAndEnsureOpen();
        try {
            // more code ...

            do {
                //单次的拉取
                Map<TopicPartition, List<ConsumerRecord<K, V>>> records = pollOnce(remaining);
                if (!records.isEmpty()) {
                    // 如果拉取的记录不为empty,在返回给之前可以发送下一批的feitches请求，
                    //这样下一轮pollOnce()操作可能直接取到结果,
                    // 消息消费的位置已经发生了变化，在返回结果期间不允许weakup和做触发error的操作
                    if (fetcher.sendFetches() > 0 || client.hasPendingRequests())
                        client.pollNoWakeup();

                    return this.interceptors.onConsume(new ConsumerRecords<>(records));
                }

                long elapsed = time.milliseconds() - start;
                remaining = timeout - elapsed;
            } while (remaining > 0);

            return ConsumerRecords.empty();
        } finally {
            release();
        }
    }

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {
        client.maybeTriggerWakeup();

        long startMs = time.milliseconds();
        // poll coordinator消息进行一些管理工作，如果是使用subscribe方式订阅，需要知道coordinator及加入group, 这样才能消费消息
    // 如果是通过assign模式订阅消息，则需要等待node可用及metadata的更新
        // 并且如果是设置了auto-commit，则处理offset的commit
        coordinator.poll(startMs, timeout);

        // 更新partition的fetch位置
        boolean hasAllFetchPositions = updateFetchPositions();

        // 因为在前几轮的pollOnce()中返回结果之前又sendFetches(),因此此时可能结果已经直接ready
        Map<TopicPartition, List<ConsumerRecord<K, V>>> records = fetcher.fetchedRecords();
        if (!records.isEmpty())
            return records;

        // 发送新的fetch请求，pending中的请求不会重发
        fetcher.sendFetches();

        // more code ...

        client.poll(pollTimeout, nowMs, new PollCondition() {
            @Override
            public boolean shouldBlock() {
                // 如果有后台线程已经完成一个fetch请求，则不需要阻塞client
                return !fetcher.hasCompletedFetches();
            }
        });

        // 检查是否需要重新负载均衡
        if (coordinator.needRejoin())
            return Collections.emptyMap();

        return fetcher.fetchedRecords();
    }
```

　　上述代码中，kafkaConsumer.pollOnce()中在构造请求，获取结果前调用了coordinator.poll()方法来poll coordnator事件。该过程完成了Group的查找，加入一个Group，发送心跳数据，更新metadata,周期性的提交offset等处理。这篇文章的后面部分着重介绍这个过程的客户端与服务端的交互细节。

## <a id="ConsumerCoordinator.poll">ConsumerCoordinator.poll</a>

　　下面是ConsumerCoordinator.poll()方法的代码细节。

```java
    // ConsumerCoordinator.java
    public void poll(long now, long remainingMs) {
        invokeCompletedOffsetCommitCallbacks();

        // AUTO_TOPICS || AUTO_PATTERN，即通过subscribe进行的主题订阅
        if (subscriptions.partitionsAutoAssigned()) {
            // 这种模式下，需要保证coordinator处于ready状态
            // 否则将通过发送查找GROUP类型的FindCoordinatorRequest进行查找并更新缓存信息
            if (coordinatorUnknown()) {
                ensureCoordinatorReady();
                now = time.milliseconds();
            }

            //1.  AUTO_TOPICS || AUTO_PATTERN
            //and 如果assignment有更新或者joinedSubscription有更新则进行rejoin过程
            if (needRejoin()) {
                if (subscriptions.hasPatternSubscription())
                    //join前需要保证metadata更新完
                    client.ensureFreshMetadata();
                // 发送JoinGroupRequest请求加入group
                ensureActiveGroup();
                now = time.milliseconds();
            }

            //保持留在group中
            pollHeartbeat(now);
        } else {
            if (metadata.updateRequested() && !client.hasReadyNodes()) {
                boolean metadataUpdated = client.awaitMetadataUpdate(remainingMs);
                if (!metadataUpdated && !client.hasReadyNodes())
                    return;
                now = time.milliseconds();
            }
        }

        // 周期性地进行offset的commit
        maybeAutoCommitOffsetsAsync(now);
    }
```

```java
    // AbstractCoordinator.java
    public void ensureActiveGroup() {
        ensureCoordinatorReady();
        // 如果coordinator已经ready,则开启心跳检测线程
        startHeartbeatThreadIfNeeded();
        // 
        joinGroupIfNeeded();
    }

    void joinGroupIfNeeded() {
        //如果需要join,则轮询join,直到join成功
        while (needRejoin() || rejoinIncomplete()) {
            ensureCoordinatorReady();

            // 
            if (needsJoinPrepare) {
                //在rebalance过程结束前置调用一次，将上一次的一些缓存进行清空
                onJoinPrepare(generation.generationId, generation.memberId);
                needsJoinPrepare = false;
            }

            // 发送JoinGroupRequest请求并持有返回的结果
            // initiateJoinGroup方法中为了不让心跳线程妨碍join过程首先暂停心跳线程,重新开始是在joinFuture的成功回调中重新开启
            // 设置内部state = MemberState.REBALANCING
            // 发送并返回future用于阻塞等待
            RequestFuture<ByteBuffer> future = initiateJoinGroup();
            // 阻塞等待future结果
            client.poll(future);

            if (future.succeeded()) {
                onJoinComplete(generation.generationId, generation.memberId, generation.protocol, future.value());

                // join成功则重置本地变量
                resetJoinGroupFuture();
                needsJoinPrepare = true;
            } else {
                // failure handle code ...
            }
        }
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        if (joinFuture == null) {
            // 暂停心跳线程
            disableHeartbeatThread();
            state = MemberState.REBALANCING;

            // 构建JoinGroupRequest进行（re）join一个group
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                // result handle callback ...
            });
        }
        return joinFuture;
    }

    // joinGroup的结果处理回调
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            if (error == Errors.NONE) {
                // other code ...

                synchronized (AbstractCoordinator.this) {
                    // other code ...
                     else {
                        AbstractCoordinator.this.generation = new Generation(joinResponse.generationId(),
                                joinResponse.memberId(), joinResponse.groupProtocol());
                        if (joinResponse.isLeader()) {
                            //作为leaderjoin, 用本地的给member进行分配对应的topic-partition构造SyncGroupRequest发送回Broker
                            onJoinLeader(joinResponse).chain(future);
                        } else {
                            // 作为follower 发送空的SyncGroupRequest给Broker
                            onJoinFollower().chain(future);
                        }
                    }
                }
            } else {
                //异常处理
            }
        }
    }
```

　　从代码上看ConsumerCoordinator.poll()发起的join过程，最终构造JoinGroupRequest向GroupCoordinator进行(re)join请求。结果返回后的回调方法中会向Coordinator发回assign策略，细节是如果作为leader加入，需要将返回的menber和topic-partition通过某种策略进行分配，构造SyncGroupRequest放给服务端。如果作为follower加入，则发送回一个空的分配结果给服务端。

　　作为leader进行分配的实现有很多种方式，都是直接或间接继承PartitionAssignor，重写assign()等方法，具体实现由如round,random，sticky等。

## <a id="KafkaApis">KafkaApis</a>

　　现在来看服务端对客户端发送来的FindCoordinatorRequest、JoinGroupRequest及SyncGroupRequest请求的处理过程。

**FindCoordinatorRequest**

　　KafkaApis对FindCoordinatorRequest的处理过程与在[Kafka事务消息过程分析(一)](https://daleizhou.github.io/posts/startup-of-Kafka.html)中处理过程相似，区别是创建的内部topic不同。如果FindCoordinatorRequest.CoordinatorType为GROUP，则对应的内部topic为*__consumer_offsets*，这里不再赘述，感兴趣可以翻看该篇博文。

**JoinGroupRequest**

**SyncGroupRequest**


## <a id="conclusion">总结</a>

　　本篇介绍了。

## <a id="references">References</a>

* http://matt33.com/2017/10/22/consumer-join-group/
