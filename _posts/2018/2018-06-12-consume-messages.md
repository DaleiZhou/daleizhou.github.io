---
layout: post
category: Kafka
title: Kafka Consumer(二)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　本篇任然从KafkaConsumer.pollOnce()为切入点，学习一下consumer加入一个group之后消费具体的消息背后的实现细节，介绍包含客户端与服务端的代码细节。

## <a id="KafkaConsumer">KafkaConsumer</a>

　　客户端拉取消息的入口是KafkaConsumer.poll(),该方法在过期时间内轮询拉取数据，如果并每次都检查条件，看是否需要更新缓存信息。下面我们看具体的一次轮询拉取消息的具体过程:

```java
    // KafkaConsumer.java
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {

        // other code ...
        // 发送获取消息的请求，不重复发送pending状态的请求
        fetcher.sendFetches();

        client.poll(pollTimeout, nowMs, new PollCondition() {
            @Override
            public boolean shouldBlock() {
                // completedFetches队列不为空，即有后台线程完成了一次拉取获得了结果，这种情况下不阻塞
                return !fetcher.hasCompletedFetches();
            }
        });

        // other code ...
        
        return fetcher.fetchedRecords();
    }

```

　　这里将KafkaConsumer.pollOnce()中关于获取消息部分的代码抽出来。从代码上可以看到consumer通过fetcher.sendFetches()来拉取数据。该方法用于从客户端被分配的topicpartition中拉取数据，且保证如果一个tp对应的node同时只有一个拉取动作。

```java
    // Fetcher.java
    public int sendFetches() {
        // 创建fetch请求的数据准备，每个topicPartition对应node生成至多一个FetchRequestData，不生成请求的情况有：
        // node为null, 有pending中的请求，client不可用等
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            // 根据FetchRequestData构造FetchRequest，填充参数
            final FetchRequest.Builder request = FetchRequest.Builder
                    .forConsumer(this.maxWaitMs, this.minBytes, data.toSend())
                    .isolationLevel(isolationLevel)
                    .setMaxBytes(this.maxBytes)
                    .metadata(data.metadata())
                    .toForget(data.toForget());
            if (log.isDebugEnabled()) {
                log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
            }
            //放入unsent队列,后台线程会检测这个队列进行发送
            client.send(fetchTarget, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        // 返回成功时的回调
                        public void onSuccess(ClientResponse resp) {
                            FetchResponse response = (FetchResponse) resp.responseBody();
                            FetchSessionHandler handler = sessionHandlers.get(fetchTarget.id());
                            // 异常处理...

                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            // other code ...

                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                long fetchOffset = data.sessionPartitions().get(partition).fetchOffset;
                                FetchResponse.PartitionData fetchData = entry.getValue();

                                log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                        isolationLevel, fetchOffset, partition, fetchData);
                                // 取出partition, fetchOffset, fetchData放入completedFetches缓存中，用于后续的fetcher.fetchedRecords()方法的获取
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
                                        resp.requestHeader().apiVersion()));
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            FetchSessionHandler handler = sessionHandlers.get(fetchTarget.id());
                            if (handler != null) {
                                handler.handleError(e);
                            }
                        }
                    });
        }
        return fetchRequestMap.size();
    }

    // 每个分配了tp的node创建一个请求队列，跳过那些正在发送请求的node
    private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        Cluster cluster = metadata.fetch();
        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();
        for (TopicPartition partition : fetchablePartitions()) {
            Node node = cluster.leaderFor(partition);
            // exception handle ...

            // 如果该node有正在进行中的请求则跳过
            else if (client.hasPendingRequests(node)) {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            } else {
                // 该leader无in-fight请求，则新建一个请求
                FetchSessionHandler.Builder builder = fetchable.get(node);
                if (builder == null) {
                    FetchSessionHandler handler = sessionHandlers.get(node.id());
                    if (handler == null) {
                        handler = new FetchSessionHandler(logContext, node.id());
                        sessionHandlers.put(node.id(), handler);
                    }
                    builder = handler.newBuilder();
                    fetchable.put(node, builder);
                }

                long position = this.subscriptions.position(partition);
                // 填充tp, fetchoffset等信息用于fetch
                builder.add(partition, new FetchRequest.PartitionData(position, FetchRequest.INVALID_LOG_START_OFFSET,
                    this.fetchSize));
            }
        }
        Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
            //调用build()方法生成FetchRequestData
            reqs.put(entry.getKey(), entry.getValue().build());
        }
        return reqs;
    }
```

　　Fetcher.sendFetches()方法根据自己被分配的tp，为每个无in-fight的leader构造一个FetchRequest，并放入client的unsent队列。Fetcher，client运行机制与[Kafka事务消息过程分析(一)](https://daleizhou.github.io/posts/startup-of-Kafka.html)中描述的Sender,client运行机制相似，都是后台线程轮询发送，这里具体的线程工作过程不再赘述。

## <a id="KafkaApis">KafkaApis</a>

　　Kafka处理FetchRequest的入口在KafkaApis.handle()方法中。其实Kafka的主从同步也是通过FetchRequest来完成，与consumer拉取消息的过程相似,都在handleFetchRequest()中进行处理，不过broker对他们的处理在身份验证上做了区分，下面我们看具体的FetchRequest处理过程:

```scala
  //KafkaApis.scala 
  def handle(request: RequestChannel.Request) {
        case ApiKeys.FETCH => handleFetchRequest(request)
  }

  def handleFetchRequest(request: RequestChannel.Request) {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val fetchContext = fetchManager.newContext(fetchRequest.metadata(),
          fetchRequest.fetchData(),
          fetchRequest.toForget(),
          fetchRequest.isFromFollower())

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponse.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower()) {
      // 处理followers发来的FetchRequest ...
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      fetchContext.foreachPartition((topicPartition, data) => {
        // 单独的每个tp分别做身份校验
        // 不在tp缓存中的tp请求处理
        else
          // 正常情况
          interesting += (topicPartition -> data)
      })
    }

    // 根据magic兼容结果
    def convertedPartitionData(tp: TopicPartition, data: FetchResponse.PartitionData) = {
        // Down-conversion 结果兼容
    }

    // 用于处理成功后回调给客户返回结果
    def processResponseCallback(responsePartitionData: Seq[(TopicPartition, FetchPartitionData)]) {
      val partitions = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]
      responsePartitionData.foreach{ case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.map(_.asJava).orNull
        val lastStableOffset = data.lastStableOffset.getOrElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        partitions.put(tp, new FetchResponse.PartitionData(data.error, data.highWatermark, lastStableOffset,
          data.logStartOffset, abortedTransactions, data.records))
      }
      erroneous.foreach{case (tp, data) => partitions.put(tp, data)}
      val unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)

      // fetch response callback invoked after any throttling
      def fetchResponseCallback(bandwidthThrottleTimeMs: Int) {
        def createResponse(requestThrottleTimeMs: Int): FetchResponse = {
          // 兼容结果code...

          // 构造返回结果
          val response = new FetchResponse(unconvertedFetchResponse.error(), convertedData,
            bandwidthThrottleTimeMs + requestThrottleTimeMs, unconvertedFetchResponse.sessionId())
          // other code ...
          }
          response
        }

        else
          // 限速返回结果给客户端
          sendResponseMaybeThrottle(request, requestThrottleMs => createResponse(requestThrottleMs))
      }

      // When this callback is triggered, the remote API call has completed.
      // Record time before any byte-rate throttling.
      request.apiRemoteCompleteTimeNanos = time.nanoseconds

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        val responseSize = sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        fetchResponseCallback(bandwidthThrottleTimeMs = 0)
      } else {
        // 用兼容前的数据的大小来做限流，虽然大小上有出入，但是unconvertedFetchResponse已经被载入到内存里，直接使用，减少OOM出现几率
        val responseStruct = unconvertedFetchResponse.toStruct(versionId)
        quotas.fetch.maybeRecordAndThrottle(request.session, clientId, responseStruct.sizeOf,
          fetchResponseCallback)
      }
    }

    if (interesting.isEmpty)
      processResponseCallback(Seq.empty)
    else {
      // 调用replicaManager从本地副本中获取消息(kafka读写和leader交互，因此本地副本认为是leader副本)
      replicaManager.fetchMessages(
        fetchRequest.maxWait.toLong,
        fetchRequest.replicaId,
        fetchRequest.minBytes,
        fetchRequest.maxBytes,
        versionId <= 2,
        interesting,
        replicationQuota(fetchRequest),
        processResponseCallback,
        fetchRequest.isolationLevel)
    }
  }
```

　　handleFetchRequest()的处理过程主要的过程是调用ReplicaManager.fetchMessages()方法，从本地副本中获取数据，并等待足够多的数据进行返回，其中传入的responseCallback方法在超时或者是满足fetch条件时将会被调用，将结果返回给客户端。

```scala
  //ReplicaManager.scala
  def fetchMessages(timeout: Long,
                    replicaId: Int,
                    fetchMinBytes: Int,
                    fetchMaxBytes: Int,
                    hardMaxBytesLimit: Boolean,
                    fetchInfos: Seq[(TopicPartition, PartitionData)],
                    quota: ReplicaQuota = UnboundedQuota,
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,
                    isolationLevel: IsolationLevel) {
    val isFromFollower = Request.isValidBrokerId(replicaId)
    val fetchOnlyFromLeader = replicaId != Request.DebuggingConsumerId && replicaId != Request.FutureLocalReplicaId
    val fetchOnlyCommitted = !isFromFollower && replicaId != Request.FutureLocalReplicaId

    // 从调用readFromLocalLog()方法从log实际读取数据，并返回结果
    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        readOnlyCommitted = fetchOnlyCommitted,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        isolationLevel = isolationLevel)
      if (isFromFollower) updateFollowerLogReadResults(replicaId, result)
      else result
    }

    val logReadResults = readFromLog()

    // other code ...
    
    // 检查是否满足立即返回的条件，当如下任一条件满足时：
    // 1. timeout 2.  fetchInfos列表为空 3. 读取到最小要求的字节数 4 读取结果中有error
    // 满足上述任一情况时立即返回给客户端
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
      // 拼装结果立即触发回调方法返回结果
    } else {
        // 根据读取的结果，构建fetchMetadata用于构建一个DelayedFetch的延迟操作
        // 当下列任一条件满足时：
        // 1. 当前broker不再是要读取的tp的leader
        // 2. 当前broker失去了对某个tp的感知
        // 3. fetch的offset不在最后一个segment上
        // 4. 累计的读取字节数超过最小要求字节数
        // 5. tp是在一个离线的日志目录下
        // 当任一满足时，完成延迟操作，延迟操作结束方法中通过replicaManager.readFromLocalLog()读取log,并直接出发callback返回给客户

      // code ...
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, isolationLevel, responseCallback)
      // code ...
    }
  }

  // 从log中读取消息
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       isolationLevel: IsolationLevel): Seq[(TopicPartition, LogReadResult)] = {
    // 每个tp的实际读取方法
    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      try {

        // decide whether to only fetch from leader
        val localReplica = if (fetchOnlyFromLeader)
          getLeaderReplicaIfLocal(tp)
        else
          getReplicaOrException(tp)

        val initialHighWatermark = localReplica.highWatermark.messageOffset
        val lastStableOffset = if (isolationLevel == IsolationLevel.READ_COMMITTED)
          Some(localReplica.lastStableOffset.messageOffset)
        else
          None

        // 根据隔离级别决定是否只读取已经commit的消息, 决定读取的最大偏移量
        val maxOffsetOpt = if (readOnlyCommitted)
          Some(lastStableOffset.getOrElse(initialHighWatermark))
        else
          None

        /* Read the LogOffsetMetadata prior to performing the read from the log.
         * We use the LogOffsetMetadata to determine if a particular replica is in-sync or not.
         * Using the log end offset after performing the read can lead to a race condition
         * where data gets appended to the log immediately after the replica has consumed from it
         * This can cause a replica to always be out of sync.
         */
        val initialLogEndOffset = localReplica.logEndOffset.messageOffset
        val initialLogStartOffset = localReplica.logStartOffset
        val fetchTimeMs = time.milliseconds
        val logReadInfo = localReplica.log match {
          case Some(log) =>
            // fetchInfo.maxBytes与本地最多读取字节数取一个最小值作为本次读取的最大字节数
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)
            // 通过log.read()方法读取数据
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel)

            // 该partition发送太快，需要限流，填充空结果
            if (shouldLeaderThrottle(quota, tp, replicaId))
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // 如果是versionId >=3 , hardMaxBytesLimit为false
            // 如果是hardMaxBytesLimit为false && fetch.firstEntryIncomplete
            // 则返回空的结果，不需要发送RecordTooLargeException
            else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            else fetch

          case None =>
            error(s"Leader for partition $tp does not have a local log")
            FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY)
        }

        LogReadResult(info = logReadInfo,
                      highWatermark = initialHighWatermark,
                      leaderLogStartOffset = initialLogStartOffset,
                      leaderLogEndOffset = initialLogEndOffset,
                      followerLogStartOffset = followerLogStartOffset,
                      fetchTimeMs = fetchTimeMs,
                      readSize = partitionFetchSize,
                      lastStableOffset = lastStableOffset,
                      exception = None)
      } catch {
        // exception handle code ...
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    // 如果minOneMessage为true,则在segment中读取消息时第一个消息(如果有)将会被读取出来，即便是超过了maxSize
    var minOneMessage = !hardMaxBytesLimit
    // 根据传入的readPartitionInfo遍历读取tp对应的log
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }
```

　　ReplicaManager.readFromLocalLog()方法中主要地调用了log.read()方法从日志中读取消息。该方法从不大于startOffset的那个segment开始读取数据，如果未读满并且还有未读取的segment,依次向前遍历读取，最后拼接结果给客户端返回。

## <a id="conclusion">总结</a>

　　本文主要介绍了KafkaConsumer.pollOnce()中拉取消息的实现细节。客户端为每个被分配的topic-partition对应的node构建一个FetchRequest请求，而对应地Broker端收到这个消息做一些检查之后从本地副本中读取消息。如果消息为达到最小字节数且未超时，则产生延迟fetch操作继续读取，直到满足条件结束。Broker将读取的消息封装好返回给客户端，至此完成了拉取消息的整个过程。