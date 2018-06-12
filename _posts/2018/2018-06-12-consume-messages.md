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

    def convertedPartitionData(tp: TopicPartition, data: FetchResponse.PartitionData) = {
      // Down-conversion of the fetched records is needed when the stored magic version is
      // greater than that supported by the client (as indicated by the fetch request version). If the
      // configured magic version for the topic is less than or equal to that supported by the version of the
      // fetch request, we skip the iteration through the records in order to check the magic version since we
      // know it must be supported. However, if the magic version is changed from a higher version back to a
      // lower version, this check will no longer be valid and we will fail to down-convert the messages
      // which were written in the new format prior to the version downgrade.
      replicaManager.getMagic(tp).flatMap { magic =>
        val downConvertMagic = {
          if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1 && !data.records.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V0))
            Some(RecordBatch.MAGIC_VALUE_V0)
          else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3 && !data.records.hasCompatibleMagic(RecordBatch.MAGIC_VALUE_V1))
            Some(RecordBatch.MAGIC_VALUE_V1)
          else
            None
        }

        downConvertMagic.map { magic =>
          trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
          val converted = data.records.downConvert(magic, fetchContext.getFetchOffset(tp).get, time)
          updateRecordsProcessingStats(request, tp, converted.recordsProcessingStats)
          new FetchResponse.PartitionData(data.error, data.highWatermark, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            data.logStartOffset, data.abortedTransactions, converted.records)
        }

      }.getOrElse(data)
    }

    // the callback for process a fetch response, invoked before throttling
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
          val convertedData = new util.LinkedHashMap[TopicPartition, FetchResponse.PartitionData]
          unconvertedFetchResponse.responseData().asScala.foreach { case (tp, partitionData) =>
            if (partitionData.error != Errors.NONE)
              debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
                s"on partition $tp failed due to ${partitionData.error.exceptionName}")
            convertedData.put(tp, convertedPartitionData(tp, partitionData))
          }
          val response = new FetchResponse(unconvertedFetchResponse.error(), convertedData,
            bandwidthThrottleTimeMs + requestThrottleTimeMs, unconvertedFetchResponse.sessionId())
          response.responseData.asScala.foreach { case (topicPartition, data) =>
            // record the bytes out metrics only when the response is being sent
            brokerTopicStats.updateBytesOut(topicPartition.topic, fetchRequest.isFromFollower, data.records.sizeInBytes)
          }
          response
        }

        trace(s"Sending Fetch response with partitions.size=${unconvertedFetchResponse.responseData().size()}, " +
          s"metadata=${unconvertedFetchResponse.sessionId()}")

        if (fetchRequest.isFromFollower)
          sendResponseExemptThrottle(request, createResponse(0))
        else
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
        // Fetch size used to determine throttle time is calculated before any down conversions.
        // This may be slightly different from the actual response size. But since down conversions
        // result in data being loaded into memory, it is better to do this after throttling to avoid OOM.
        val responseStruct = unconvertedFetchResponse.toStruct(versionId)
        quotas.fetch.maybeRecordAndThrottle(request.session, clientId, responseStruct.sizeOf,
          fetchResponseCallback)
      }
    }

    if (interesting.isEmpty)
      processResponseCallback(Seq.empty)
    else {
      // call the replica manager to fetch messages from the local replica
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

    // check if this fetch request can be satisfied right away
    val logReadResultValues = logReadResults.map { case (_, v) => v }
    val bytesReadable = logReadResultValues.map(_.info.records.sizeInBytes).sum
    val errorReadingData = logReadResultValues.foldLeft(false) ((errorIncurred, readResult) =>
      errorIncurred || (readResult.error != Errors.NONE))

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        tp -> FetchPartitionData(result.error, result.highWatermark, result.leaderLogStartOffset, result.info.records,
          result.lastStableOffset, result.info.abortedTransactions)
      }
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = logReadResults.map { case (topicPartition, result) =>
        val fetchInfo = fetchInfos.collectFirst {
          case (tp, v) if tp == topicPartition => v
        }.getOrElse(sys.error(s"Partition $topicPartition not found in fetchInfos"))
        (topicPartition, FetchPartitionStatus(result.info.fetchOffsetMetadata, fetchInfo))
      }
      val fetchMetadata = FetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit, fetchOnlyFromLeader,
        fetchOnlyCommitted, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, isolationLevel, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       isolationLevel: IsolationLevel): Seq[(TopicPartition, LogReadResult)] = {

    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      try {
        trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
          s"remaining response limit $limitBytes" +
          (if (minOneMessage) s", ignoring response/partition size limits" else ""))

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

        // decide whether to only fetch committed data (i.e. messages below high watermark)
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
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

            // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel)

            // If the partition is being throttled, simply return an empty set.
            if (shouldLeaderThrottle(quota, tp, replicaId))
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
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
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderForPartitionException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        highWatermark = -1L,
                        leaderLogStartOffset = -1L,
                        leaderLogEndOffset = -1L,
                        followerLogStartOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        lastStableOffset = None,
                        exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()
          error(s"Error processing fetch operation on partition $tp, offset $offset", e)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
                        highWatermark = -1L,
                        leaderLogStartOffset = -1L,
                        leaderLogEndOffset = -1L,
                        followerLogStartOffset = -1L,
                        fetchTimeMs = -1L,
                        readSize = partitionFetchSize,
                        lastStableOffset = None,
                        exception = Some(e))
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
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

## <a id="conclusion">总结</a>

## <a id="references">References</a>

#### TBD