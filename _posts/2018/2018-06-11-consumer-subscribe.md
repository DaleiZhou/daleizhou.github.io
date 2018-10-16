---
layout: post
category: [Kafka, Source Code]
title: Kafka Consumer(一)
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT, 涉及内容有subscribe, assign, join group

　　从这篇开始从KafkaConsumer为切入点，大概用两篇左右学习一下消息的消费相关的调用的实现，介绍包含客户端与服务端的代码细节。本篇涉及KafkaConsumer订阅主题，加入Group的具体实现。

## <a id="subscribe">subscribe</a>
<!--more-->

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

　　poll()中，如果是subscribe方式订阅并且meta有更新，则调用AbstractCoordinator.ensureActiveGroup()进行加入group，该方法会阻塞等待joinGroupIfNeeded()返回。下面为具体代码。

```java
    // AbstractCoordinator.java
    public void ensureActiveGroup() {
        ensureCoordinatorReady();
        // 如果coordinator已经ready,则开启心跳检测线程
        startHeartbeatThreadIfNeeded();
        // 如果需要join,则轮询join,直到join成功
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
                //join成功后调用的方法，更新缓存，触发负载均衡后的回调
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

　　从代码上看ConsumerCoordinator.poll()发起的join过程，最终构造JoinGroupRequest向GroupCoordinator进行(re)join请求。结果返回后的回调方法中会向Coordinator发回assign策略，细节是如果作为leader加入，需要将返回的menber和topic-partition通过某种策略进行分配，构造SyncGroupRequest放给服务端。如果作为follower加入，则发送回一个空的分配结果给服务端，用于获取本memberId对应的分配信息。

　　作为leader进行分配的实现有很多种方式，都是直接或间接继承PartitionAssignor，重写assign()等方法，具体实现由如round,random，sticky等。

　　Join成功后调用ConsumerCoordinator.onJoinComplete()方法，完成更新TopicPartition的订阅，metadata的更新，及执行用户定义的负载均衡后的回调方法。

　　现在的问题是如果一个状态为Stable的Group,新加入一个member，其它member如何感知到进行rebalance呢。答案就是心跳不仅用于告诉服务端自己活着，每次带着自己的年代记号，用于检测是否已经发生了rebalance。

　　至此Consumer端完成join group的调用。

## <a id="KafkaApis">KafkaApis</a>

　　现在来看服务端对客户端发送来的FindCoordinatorRequest、JoinGroupRequest及SyncGroupRequest请求的处理过程。

**FindCoordinatorRequest**

　　KafkaApis对FindCoordinatorRequest的处理过程与在[Kafka事务消息过程分析(一)](https://daleizhou.github.io/posts/startup-of-Kafka.html)中处理过程相似，区别是创建的内部topic不同。如果FindCoordinatorRequest.CoordinatorType为GROUP，则对应的内部topic为*__consumer_offsets*，这里不再赘述，感兴趣可以翻看该篇博文。

**JoinGroupRequest**

　　下面介绍kafkaApis收到JoinGroupRequest的请求处理过程，入口任然从handle()方法开始。该方法进行校验等处理后调用groupCoordinator.handleJoinGroup()转由groupCoordinator处理，将memberId加入group。

```scala
  //KafkaApis.scala 
  def handle(request: RequestChannel.Request) {
        case ApiKeys.JOIN_GROUP => handleJoinGroupRequest(request)
  }

  def handleJoinGroupRequest(request: RequestChannel.Request) {
    val joinGroupRequest = request.body[JoinGroupRequest]

    // the callback for sending a join-group response
    def sendResponseCallback(joinResult: JoinGroupResult) {
      //该member返回给consumer,用于leader分配tp
      val members = joinResult.members map { case (memberId, metadataArray) => (memberId, ByteBuffer.wrap(metadataArray)) }
      def createResponse(requestThrottleMs: Int): AbstractResponse = {
        // other code...
      }
      // 限速发送回结果
      sendResponseMaybeThrottle(request, createResponse)
    }

    if (!authorize(request.session, Read, new Resource(Group, joinGroupRequest.groupId()))) {
      // 身份校验异常处理
    } else {
      // let the coordinator to handle join-group
      val protocols = joinGroupRequest.groupProtocols().asScala.map(protocol =>
        (protocol.name, Utils.toArray(protocol.metadata))).toList
      //将请求参数传入handleJoinGroup()转由groupCoordinator处理
      groupCoordinator.handleJoinGroup(
        joinGroupRequest.groupId,
        joinGroupRequest.memberId,
        request.header.clientId,
        request.session.clientAddress.toString,
        joinGroupRequest.rebalanceTimeout,
        joinGroupRequest.sessionTimeout,
        joinGroupRequest.protocolType,
        protocols,
        sendResponseCallback)
    }
  }
```

　　GroupCoordinator.handleJoinGroup()做了实际的join group的工作，具体工作大致可以概括为创建新的group,根据状态及group实际内容执行不同的join策略。具体代码如下:

```scala
  // GroupCoordinator.scala
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    // 该事件不可以groupId调用返回错误给客户端
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(joinError(memberId, error))
      return
    }

    // timeout handle...
    else {
      // 如不存在groupId，则创建group,
      // 通过doJoinGroup()将memberId加入已有或刚创建的group
      groupManager.getGroup(groupId) match {
        case None =>
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          } else {
            //新建一个组，状态初始化为Empty
            val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        case Some(group) =>
          // 实际的doJoinGroup  
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group.inLock {
       // 异常处理...
       else {
        group.currentState match {
          // other code ...

          // 各种状态和是否是新加入的menber进行不同的处理
          // 主要的处理方法有
          // 1. 新加入的member:addMemberAndRebalance()
          // 2. 已有member更新：updateMemberAndRebalance()
          case  =>
        }

        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) = {
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)
    member.awaitingJoinCallback = callback
    // update the newMemberAdded flag to indicate that the join group can be further delayed
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true

    //group的add()方法用于将member加入GroupMeta的members缓存中
    // 如果此时没有leader，则现在加入的member就成为了leader
    group.add(member)
    maybePrepareRebalance(group)
    member
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    // 更新缓存信息
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    maybePrepareRebalance(group)
  }

  private def maybePrepareRebalance(group: GroupMetadata) {
    group.inLock {
      // 如果此时状态已经为Stable, CompletingRebalance, Empty则执行prepareRebalance()
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  private def prepareRebalance(group: GroupMetadata) {
    // 如果有member在等待sync操作，返回REBALANCE_IN_PROGRESS，客户端会再次sync获取rebalance后的分配信息
    if (group.is(CompletingRebalance))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    // 根据group状态不同设置不同的DelayedJoin操作
    // 从Empty到PreparingRebalance时设置InitialDelayedJoin
    //
    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    //将状态置为PreparingRebalance
    group.transitionTo(PreparingRebalance)

    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  //DelayedJoin操作成功时会调用onCompleteJoin()完成join操作
  def onCompleteJoin(group: GroupMetadata) {
    group.inLock {
      // remove any members who haven't joined the group yet
      group.notYetRejoinedMembers.foreach { failedMember =>
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        //更新下一个generation值，设置group状态
        group.initNextGeneration()
        if (group.is(Empty)) {
          // other code ...
        } else {
          // 触发所有等待join group的回调方法，将结果返回给所有的member
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)
            val joinResult = JoinGroupResult(
              // 如果当前member为leader，将整个group的member信息返回
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                Map.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocolOrNull,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)

            member.awaitingJoinCallback(joinResult)
            member.awaitingJoinCallback = null
            //rebalance期间心跳功能已经暂停，延长心跳超时检测
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }
  }
```

　　如果Group当前的leader为空，则现在加入的member自动为leader。onCompleteJoin方法为rebalance成功后触发的操作。该方法中，在返回给客户端结果时做了是否为leader的区分，如果是leader则返回所有member的信息，否则member信息为空。而leader收到JoinGroup返回后，则会通过获取到的member信息与topicpartition进行关联分配，通过SyncGroupRequest又发回给服务端。

**SyncGroupRequest**

　　客户端join一个group后会发送SyncGroupRequest进行请求group的分配信息。一个Group中的consumer有两种角色：leader和follow。leader会在SyncGroupRequest时带上经过运算等到的分配情况，而follow发送的SyncGroupReques发送的assign为空信息。Broker端根据是否为leader会进行不同的处理，相同的是最后会返回该group对应的assign信息，具体处理过程如下:

```scala
  //KafkaApis.scala 
  def handle(request: RequestChannel.Request) {
        case ApiKeys.SYNC_GROUP => handleSyncGroupRequest(request)
  }

  def handleSyncGroupRequest(request: RequestChannel.Request) {
    val syncGroupRequest = request.body[SyncGroupRequest]

    def sendResponseCallback(memberState: Array[Byte], error: Errors) {
      sendResponseMaybeThrottle(request, requestThrottleMs =>
        new SyncGroupResponse(requestThrottleMs, error, ByteBuffer.wrap(memberState)))
    }

    if (!authorize(request.session, Read, new Resource(Group, syncGroupRequest.groupId()))) {
      sendResponseCallback(Array[Byte](), Errors.GROUP_AUTHORIZATION_FAILED)
    } else {
      groupCoordinator.handleSyncGroup(
        syncGroupRequest.groupId,
        syncGroupRequest.generationId,
        syncGroupRequest.memberId,
        syncGroupRequest.groupAssignment().asScala.mapValues(Utils.toArray),
        sendResponseCallback
      )
    }
  }
```

　　GroupCoordinator.handleSyncGroup()方法经过一系列的异常条件判断后，正常执行doSyncGroup(),根据传入的参数完成具体的syncGroup实际的工作。

```scala
  //GroupCoordinator.scala
  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      // other code ...

      case None =>
        groupManager.getGroup(groupId) match {
          case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
          case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
        }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    group.inLock {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
      } else {
        group.currentState match {
          // error handle code ...

          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // 由leader去触发，完成CompletingRebalance到Stable状态转换
            if (group.isLeader(memberId)) {

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              //storeGroup()主要是构造主题为 __consumer_offsets 的内部消息，写入log并完成主从同步操作
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // 等待stroeGroup完成回调这个匿名回调方法期间可能有别的consumerjoin进来。因此我们需要检查是否还处于预期的状态和年代内，如果不是则什么都不做，客户端根据不同情况自行处理，重新进行join等
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group)
                    } else {
                      //根据leader的assignment更新缓存并返回leader的assign  
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // 如果状态处于stable，只需要根据menberId返回分配信息给客户端即可
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE)
            //因为在rebalance期间心跳暂停，重设menber对应的过期时间，根据新的过期时间监听该menberId心跳
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }
```

## <a id="conclusion">总结</a>

　　本篇从KafkaConsumer消费消息为切入点，介绍了两种订阅模式：subscribe和assign模式。而subscribe模式会在poll消息时决定是否触发加入group操作，进行rebalance动作。在Join的过程中总共会产生FindCoordinatorRequest，JoinGroupRequest以及SyncGroupRequest的请求。服务端每次发生rebalance会向客户端返回年代记号，客户端如果通过心跳检测活着别的调用发现年代值不一致，就需要重新加入group。本文的后半节介绍了这几个请求客户端与服务端处理的具体过程。

　　下一篇继续从KafkaConsumer.pollOnce()切入，分析学习消息的具体消费过程。

## <a id="references">References</a>

* http://matt33.com/2017/10/22/consumer-join-group/
