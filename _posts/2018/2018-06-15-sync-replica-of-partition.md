---
layout: post
category: Kafka
title: Kafka 副本间的主从同步
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT


　　在深入到副本间的同步的内容是，先来看一下副本间同步的任务是如何从Kafka服务端启动后是如何开始的。KafkaServer启动时，主要调用KafkaServer.startup()方法进行初始化和启动，在startup()方法中初始化KafkaController并启动它。

　　Controller是Kafka的一个重要的组件，用于集群中的meta信息的管理。Controller的具体作用从KafkaController.onControllerFailover()方法中祖册的监听事件可以看出来:

```scala
    private def onControllerFailover() {
        val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
        val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    }
```

　　KafkaController在集群的全局有且仅有一个是活动的，每个Broker都有可能成为Controller。具体的选举过程从KafkaServer的启动部分开始说起。过程如下如下：

```scala
   //KafkaServer.scala
   def startup() {
        // ...
        /* start kafka controller */
        kafkaController = new KafkaController(config, zkClient, time, metrics, brokerInfo, tokenManager, threadNamePrefix)
        kafkaController.startup()
        // ...
    }
```

　　在KafkaController.startup()方法中首先通过zk注册监听事件，监听StateChangeHandler，并将Startup放入ControllerEventManager的事件队列中，调用ControllerEventManager的startup()方法。ControllerEventManager为处理ControllerEvent的后台线程，用于在后台处理Startup,Reelect等事件。

```scala
  //KafkaController.scala
  def startup() = {
    //registerStateChangeHandler用于session过期后触发重新选举
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler
      override def afterInitializingSession(): Unit = {
        eventManager.put(RegisterBrokerAndReelect)
      }
      override def beforeInitializingSession(): Unit = {
        val expireEvent = new Expire
        eventManager.clearAndPut(expireEvent)

        // 阻塞等待时间被处理结束，session过期触发重新选举，必须等待选举这个时间完成Controller才能正常工作
        expireEvent.waitUntilProcessingStarted()
      }
    })
    // 放入Startup并启动eventManager后台线程开始选举
    eventManager.put(Startup)
    eventManager.start()
  }
```
　　这里我们针对性地分析Startup这个事件的具体被执行的过程。在Startup的回调方法process()中，首先在zk中监听**/controller**路径。并且调用elect()进行选举过程。

```scala
 // KafkaController.scala 
 case object Startup extends ControllerEvent {

    def state = ControllerState.ControllerChange

    override def process(): Unit = {
      zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
      elect()
    }

  }

  private def elect(): Unit = {
    val timestamp = time.milliseconds
    // 获得注册/controller成功的brokerId
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    // 开始创建临时节点前检查，如果/controller节点已经存在，说明已经有broker成为controller,
    //因此本broker直接退出controller选举
    if (activeControllerId != -1) {
      return
    }

    try {
      // 创建临时节点，声明本broker成为controller
      zkClient.checkedEphemeralCreate(ControllerZNode.path, ControllerZNode.encode(config.brokerId, timestamp))
      // 未抛异常说明写入创建成功，本broker荣升为controller
      info(s"${config.brokerId} successfully elected as the controller")
      activeControllerId = config.brokerId

      onControllerFailover()
    } catch {
      // 如果其它broker已经创建了该节点，说明controller已经产生，则改写本地activeControllerId，做好自己的臣民本职工作
      case _: NodeExistsException =>
        activeControllerId = zkClient.getControllerId.getOrElse(-1)
        // log


      case e2: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}", e2)
        // 遇到不可知错误，取消zk相关节点的监听注册，并调用删除/controller的zk的node
        triggerControllerMove()
    }
  }

  // 
  private def onControllerFailover() {
    // 从zk读取epoch， epochZkVersion
    readControllerEpochFromZooKeeper()
    //更新zk总control节点，将epoch + 1
    incrementControllerEpoch()
    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    // 在读取zk之前注册brokerchange, topicchange, topicdeletion, logDirEventNoti,isrChange等事件，从这行及下面事件注册也看出control在集群中的作用
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)
    //同样地注册ReplicaElection,partitionReassignment事件
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    // 清理已存在的LogDirEventNotifications在zk上的记录
    zkClient.deleteLogDirEventNotifications()
    // 清理已存在的IsrChangeNotifications在zk上的记录
    zkClient.deleteIsrChangeNotifications()
    // 初始化controller context
    // 在initializeControllerContext()中:
    // 1. 注册监听所有topic的partitionModification事件
    // 2. 从zk中获取所有topic的副本分配信息
    // 3. 在zk中监听所有broker的更新情况
    // 4. 从zk中读取topicPartition的leadership,更新本地缓存
    // 5. 初始化ControllerChannelManager，为每个broker生成一个后台通信线程用于和broker通信，并启动后台线程
    // 6. 从zk的/admin/reassign_partitions路径下读取partition的reassigned信息放入缓存用于后续处理
    initializeControllerContext()
    
    // 获取被删除的topics    
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    //info("Initializing topic deletion manager")
    //初始化通过topicDeletionManager，如果isDeleteTopicEnabled则在zk中直接删除topicsToBeDeleted
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().

    // controller context 初始化结束之后发送请求更新metadata,这是因为需要在brokers能处理LeaderAndIsrRequests前获取哪些brokers是live的
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq)

    replicaStateMachine.startup()
    partitionStateMachine.startup()

    info(s"Ready to serve as the new controller with epoch $epoch")
    maybeTriggerPartitionReassignment(controllerContext.partitionsBeingReassigned.keySet)
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onPreferredReplicaElection(pendingPreferredReplicaElections)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule(name = "delete-expired-tokens",
        fun = tokenManager.expireTokens,
        period = config.delegationTokenExpiryCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }
```






