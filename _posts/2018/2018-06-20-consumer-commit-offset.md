---
layout: post
category: Kafka
title: Kafka Consumer(三)
---

## 内容 
>Status: Draft

## TODO

  代码版本: 2.0.0-SNAPSHOT

　　在[Kafka Consumer(一)](https://daleizhou.github.io/posts/consumer-subscribe.html)中介绍到通过KafkaConsumer.pollOnce()获取结果前会调用coordinator.poll()方法，在该方法中完成Coordinator,加入group等操作，该方法在最后还会调用maybeAutoCommitOffsetsAsync()来决定是否异步提交offset。本篇博文就从该方法讲起，追踪一下Offset的提交过程，算是对*Kafka Consumer*的补全。

我们带着如下问题去看这部分的代码：
1. 什么时候提交offset
2. 服务端将处理过程
3. 客户端收到返回之后如何进行处理

```java
    //ConsumerCoordinator.java
    public void maybeAutoCommitOffsetsAsync(long now) {
        // 如果设置了自动提交而且距离上一次提交时间超过了intervalMs
        if (autoCommitEnabled && now >= nextAutoCommitDeadline) {
            // 更新deadline并且异步提交offset
            this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
            doAutoCommitOffsetsAsync();
        }
    }

    private void doAutoCommitOffsetsAsync() {
        // 对每个topicPartition获取待发送offset
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        // 调用commitOffsetsAsync()方法具体执行，并实现OffsetCommitCallback
        // onComplete()方法在commitOffsetsAsync()方法中用于coordinator未知的情况下查找完毕重新处理被pending的offset时调用，
        // completedOffsetCommits取出来的请求中的异常信息如果是RetriableException，则修改下一次的重试时间等待重试
        commitOffsetsAsync(allConsumedOffsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    // 如果有异常，并且是RetriableException则调整下一次重试时间等待重试
                    // log code ...
                } else {
                    // log code ...
                }
            }
        });
    }

    // 
    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        // 如果有coordinator，则通过doCommitOffsetsAsync()进行同步offset
        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.

            // 如果coordinator未知，则通过lookupCoordinator()进行查找，
            // 并设置Listerner,用于coordinator正确查找到之后进行补发提交offset
            // 如果查找失败，则放入completedOffsetCommits中，设置RetriableCommitFailedException异常等待重试

            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                // 查找成功则doCommitOffsetsAsync()进行提交
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                // 如果查找失败则放入completedOffsetCommits
                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }

        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException)
                    commitException = new RetriableCommitFailedException(e);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
            }
        });
    }

    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = checkAndGetCoordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        final Generation generation;
        if (subscriptions.partitionsAutoAssigned())
            generation = generation();
        else
            generation = Generation.NO_GENERATION;

        // if the generation is null, we are not part of an active group (and we expect to be).
        // the only thing we can do is fail the commit and let the user rejoin the group in poll()
        if (generation == null)
            return RequestFuture.failure(new CommitFailedException());

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(this.groupId, offsetData).
                setGenerationId(generation.generationId).
                setMemberId(generation.memberId).
                setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets));
    }
```

## <a id="handleOffsetCommitRequest">handleOffsetCommitRequest</a>

　　提交Offset的流程中，Consumer端做的工作比较简单，我们主要看kafkaApis处理OffsetCommit的具体流程。

```scala
  //KafkaApis.scala 
  def handle(request: RequestChannel.Request) {
        case ApiKeys.OFFSET_COMMIT => handleOffsetCommitRequest(request)
  }

  // 具体的处理提交offset方法
  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val header = request.header
    val offsetCommitRequest = request.body[OffsetCommitRequest]

    // 未授权处理
    if (!authorize(request.session, Read, new Resource(Group, offsetCommitRequest.groupId))) {
      val error = Errors.GROUP_AUTHORIZATION_FAILED
      val results = offsetCommitRequest.offsetData.keySet.asScala.map { topicPartition =>
        (topicPartition, error)
      }.toMap
      sendResponseMaybeThrottle(request, requestThrottleMs => new OffsetCommitResponse(requestThrottleMs, results.asJava))
    } else {
      val unauthorizedTopicErrors = mutable.Map[TopicPartition, Errors]()
      val nonExistingTopicErrors = mutable.Map[TopicPartition, Errors]()
      val authorizedTopicRequestInfoBldr = immutable.Map.newBuilder[TopicPartition, OffsetCommitRequest.PartitionData]

      // 对tp进行逐个分类，分为为授权，不存在topic partition， 正常等几类
      for ((topicPartition, partitionData) <- offsetCommitRequest.offsetData.asScala) {
        if (!authorize(request.session, Read, new Resource(Topic, topicPartition.topic)))
          unauthorizedTopicErrors += (topicPartition -> Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicPartition))
          nonExistingTopicErrors += (topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          authorizedTopicRequestInfoBldr += (topicPartition -> partitionData)
      }

      val authorizedTopicRequestInfo = authorizedTopicRequestInfoBldr.result()

      def sendResponseCallback(commitStatus: immutable.Map[TopicPartition, Errors]) {
        val combinedCommitStatus = commitStatus ++ unauthorizedTopicErrors ++ nonExistingTopicErrors
        if (isDebugEnabled)
          combinedCommitStatus.foreach { case (topicPartition, error) =>
            if (error != Errors.NONE) {
              // error code ...
            }
          }
        // 返回结果给客户端
        sendResponseMaybeThrottle(request, requestThrottleMs =>
          new OffsetCommitResponse(requestThrottleMs, combinedCommitStatus.asJava))
      }

      // 对于正常的tp offset提交请求集合如果为空，则直接调用回调返回结果
      if (authorizedTopicRequestInfo.isEmpty)
        sendResponseCallback(Map.empty)
      else if (header.apiVersion == 0) {
        // 兼容以前版本，当版本为0时，offset是存储在zk上
        val responseInfo = authorizedTopicRequestInfo.map {
          case (topicPartition, partitionData) =>
            try {
              // 客户端收到OFFSET_METADATA_TOO_LARGE会直接raise错误返回给用户
              if (partitionData.metadata != null && partitionData.metadata.length > config.offsetMetadataMaxSize)
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
              else {
                // zookeeper上存储offset的路径为:
                //path: /consumers/${group}/offset/${topic}/${partition}
                zkClient.setOrCreateConsumerOffset(offsetCommitRequest.groupId, topicPartition, partitionData.offset)
                (topicPartition, Errors.NONE)
              }
            } catch {
              case e: Throwable => (topicPartition, Errors.forException(e))
            }
        }
        sendResponseCallback(responseInfo)
      } else {
        // 客户端版本 >= 1,则通过offsetManager进行存储offset

        // 基于版本进行设置offsetRetention， 如果是版本小于1的或者是请求中未设置，则使用offsetConfig.offsetsRetentionMs
        // 否则使用请求中的retentionTime
        val offsetRetention =
          if (header.apiVersion <= 1 ||
            offsetCommitRequest.retentionTime == OffsetCommitRequest.DEFAULT_RETENTION_TIME)
            groupCoordinator.offsetConfig.offsetsRetentionMs
          else
            offsetCommitRequest.retentionTime

        // 提交的时间戳以收到请求的现在为准
        // 默认过期时间是 now + offsetRetention

        val currentTimestamp = time.milliseconds
        val defaultExpireTimestamp = offsetRetention + currentTimestamp
        // 每个tp分别处理，构造map, key为tp, value 为OffsetMetadata
        val partitionData = authorizedTopicRequestInfo.mapValues { partitionData =>
          // 如果为提供metadata,设置默认metadata
          val metadata = if (partitionData.metadata == null) OffsetMetadata.NoMetadata else partitionData.metadata
          new OffsetAndMetadata(
            offsetMetadata = OffsetMetadata(partitionData.offset, metadata),
            commitTimestamp = currentTimestamp,
            // 如果api版本为v1, 并且传入的请求中timestamp未设置 则过期时间为defaultExpireTimestamp
            // 其他情况 设置为offsetRetention + partitionData.timestamp
            expireTimestamp = {
              if (partitionData.timestamp == OffsetCommitRequest.DEFAULT_TIMESTAMP)
                defaultExpireTimestamp
              else
                offsetRetention + partitionData.timestamp
            }
          )
        }

        // 通过handleCommitOffsets()方法将tp对应的offset信息写入到log中并更新groupCoordinator的缓存信息
        groupCoordinator.handleCommitOffsets(
          offsetCommitRequest.groupId,
          offsetCommitRequest.memberId,
          offsetCommitRequest.generationId,
          partitionData,
          sendResponseCallback)
      }
    }
  }
```

　　// TODO

```scala
  //groupCoordinator.scala
  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    // OFFSET_COMMIT 情况下要求groupId不为空，如果校验不通过直接返回错误给客户端
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              // group的管理不通过groupManager来协调，generationId < 0且不在已保存的group缓存中，生成GroupMetadata加入group缓存
              val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              // 如果本地Group缓存中没有该group信息，但generationId >=0 则是一个前代留下的，因此直接拒绝请求，客户端收到ILLEGAL_GENERATION会进行resetGeneration
              responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
            }

          case Some(group) =>
            // 已加入group, 通过doCommitOffsets()提交写入offset信息
            doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    group.inLock {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // The group is only using Kafka to store offsets.
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (group.is(CompletingRebalance)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

  def storeOffsets(group: GroupMetadata,
                   consumerId: String,
                   offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicPartition, Errors] => Unit,
                   producerId: Long = RecordBatch.NO_PRODUCER_ID,
                   producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH): Unit = {
    // first filter out partitions with offset metadata size exceeding limit
    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    group.inLock {
      if (!group.hasReceivedConsistentOffsetCommits)
        warn(s"group: ${group.groupId} with leader: ${group.leaderOrNull} has received offset commits from consumers as well " +
          s"as transactional producers. Mixing both types of offset commits will generally result in surprises and " +
          s"should be avoided.")
    }

    val isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID
    // construct the message set to append
    if (filteredOffsetMetadata.isEmpty) {
      // compute the final error codes for the commit response
      val commitStatus = offsetMetadata.mapValues(_ => Errors.OFFSET_METADATA_TOO_LARGE)
      responseCallback(commitStatus)
      None
    } else {
      getMagic(partitionFor(group.groupId)) match {
        case Some(magicValue) =>
          // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
          val timestampType = TimestampType.CREATE_TIME
          val timestamp = time.milliseconds()

          val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
            val key = GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition)
            val value = GroupMetadataManager.offsetCommitValue(offsetAndMetadata)
            new SimpleRecord(timestamp, key, value)
          }
          val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType, records.asJava))

          if (isTxnOffsetCommit && magicValue < RecordBatch.MAGIC_VALUE_V2)
            throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception("Attempting to make a transaction offset commit with an invalid magic: " + magicValue)

          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L, time.milliseconds(),
            producerId, producerEpoch, 0, isTxnOffsetCommit, RecordBatch.NO_PARTITION_LEADER_EPOCH)

          records.foreach(builder.append)
          val entries = Map(offsetTopicPartition -> builder.build())

          // set the callback function to insert offsets into cache after log append completed
          def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
            // the append response should only contain the topics partition
            if (responseStatus.size != 1 || !responseStatus.contains(offsetTopicPartition))
              throw new IllegalStateException("Append status %s should only have one partition %s"
                .format(responseStatus, offsetTopicPartition))

            // construct the commit response status and insert
            // the offset and metadata to cache if the append status has no error
            val status = responseStatus(offsetTopicPartition)

            val responseError = group.inLock {
              if (status.error == Errors.NONE) {
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.onTxnOffsetCommitAppend(producerId, topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                    else
                      group.onOffsetCommitAppend(topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                  }
                }
                Errors.NONE
              } else {
                if (!group.is(Dead)) {
                  if (!group.hasPendingOffsetCommitsFromProducer(producerId))
                    removeProducerGroup(producerId, group.groupId)
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.failPendingTxnOffsetCommit(producerId, topicPartition)
                    else
                      group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }

                debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
                  s"with generation ${group.generationId} failed when appending to log due to ${status.error.exceptionName}")

                // transform the log append error code to the corresponding the commit status error code
                status.error match {
                  case Errors.UNKNOWN_TOPIC_OR_PARTITION
                       | Errors.NOT_ENOUGH_REPLICAS
                       | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    Errors.COORDINATOR_NOT_AVAILABLE

                  case Errors.NOT_LEADER_FOR_PARTITION
                       | Errors.KAFKA_STORAGE_ERROR =>
                    Errors.NOT_COORDINATOR

                  case Errors.MESSAGE_TOO_LARGE
                       | Errors.RECORD_LIST_TOO_LARGE
                       | Errors.INVALID_FETCH_SIZE =>
                    Errors.INVALID_COMMIT_OFFSET_SIZE

                  case other => other
                }
              }
            }

            // compute the final error codes for the commit response
            val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
              if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
                (topicPartition, responseError)
              else
                (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE)
            }

            // finally trigger the callback logic passed from the API layer
            responseCallback(commitStatus)
          }

          if (isTxnOffsetCommit) {
            group.inLock {
              addProducerGroup(producerId, group.groupId)
              group.prepareTxnOffsetCommit(producerId, offsetMetadata)
            }
          } else {
            group.inLock {
              group.prepareOffsetCommit(offsetMetadata)
            }
          }

          appendForGroup(group, entries, putCacheCallback)

        case None =>
          val commitStatus = offsetMetadata.map { case (topicPartition, _) =>
            (topicPartition, Errors.NOT_COORDINATOR)
          }
          responseCallback(commitStatus)
          None
      }
    }
  }
```

## TODO
