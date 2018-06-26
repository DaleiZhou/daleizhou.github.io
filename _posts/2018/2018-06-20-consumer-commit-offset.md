---
layout: post
category: Kafka
title: Kafka Consumer(三)
---

## 内容 
>Status: Draft

## TODO

  代码版本: 2.0.0-SNAPSHOT

　　在[Kafka Consumer(一)](https://daleizhou.github.io/posts/consumer-subscribe.html)中介绍到通过KafkaConsumer.pollOnce()获取结果前会调用coordinator.poll()方法，在该方法中完成Coordinator,加入group等操作，该方法在最后还会调用maybeAutoCommitOffsetsAsync()来决定是否异步提交offset。本篇博文就从该方法讲起，追踪一下Offset的提交过程，算是对*Kafka Consumer*系列的补全。

## <a id="maybeAutoCommitOffsetsAsync">maybeAutoCommitOffsetsAsync</a>

　　当KafkaConsumer启动设置了自动提交offset,maybeAutoCommitOffsetsAsyncKafka()方法才会真正起作用。当然如果用户有特殊需求，自己管理offset的提交也是可行的，Kafka同样提供了自行提交的调用方法。

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

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        // 如果有coordinator，则通过doCommitOffsetsAsync()进行同步offset
        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
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

　　提交Offset的流程中，Consumer端做的工作比较简单，我们主要看kafkaApis处理OffsetCommit的具体流程。

## <a id="handleOffsetCommitRequest">handleOffsetCommitRequest</a>

　　与其它指令处理入口一样，OffsetCommit请求的处理入口也是KafkaApis.handle()方法。

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
      // 逐个设置error后返回给客户端结果
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

　　在正常流程的情况下，根据不同的客户端版本号对传入的partitionData进行一些修改，如更新时间戳等，最后会调用GroupCoordinator.handleCommitOffsets()方法将topicPartition的offset信息写入到本地log中并进行缓存更新。

```scala
  //GroupCoordinator.scala
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
              // group的管理不通过groupManager来协调，generationId < 0且不在已保存的group缓存中，生成GroupMetadata加入group缓存
              val group = groupManager.addGroup(new GroupMetadata(groupId, initialState = Empty))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
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
        // 异常情况处理... 
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

```

　　GroupMetaManager.storeOffsets()方法进行offset持久化工作，该方法构建MemoryRecords后通过ReplicaManager进行append操作，将offset记录写入到log文件进行持久化。

```scala
  //GroupMetaManager.scala
  def storeOffsets(group: GroupMetadata,
                   consumerId: String,
                   offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                   responseCallback: immutable.Map[TopicPartition, Errors] => Unit,
                   producerId: Long = RecordBatch.NO_PRODUCER_ID,
                   producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH): Unit = {
    // 验证metadata数据的合法性，为null或过大的offsetMetadata都过滤掉
    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    group.inLock {
      // 当JoinGroup时GroupMeta.initNextGeneration()初始化，将receivedConsumerOffsetCommits， 
      // receivedTransactionalOffsetCommits都设置为false 条件不成立,不会warn
      // 而当 group接受两种类型的offset提交混用时，可能会产生异常，给一个warn提示
      // 可能Kafka的作者不建议混用
      if (!group.hasReceivedConsistentOffsetCommits)
        warn(s"group: ${group.groupId} with leader: ${group.leaderOrNull} has received offset commits from consumers as well " +
          s"as transactional producers. Mixing both types of offset commits will generally result in surprises and " +
          s"should be avoided.")
    }

    // 非事务型offset提交传入的ProducerId为默认的NO_PRODUCER_ID
    val isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID
    // construct the message set to append
    if (filteredOffsetMetadata.isEmpty) {
      // compute the final error codes for the commit response
      // 如果offsetMetadata所有数据都没通过大小限制的检查则直接报错给用户提示OFFSET_METADATA_TOO_LARGE
      val commitStatus = offsetMetadata.mapValues(_ => Errors.OFFSET_METADATA_TOO_LARGE)
      responseCallback(commitStatus)
      None
    } else {
      getMagic(partitionFor(group.groupId)) match {
        case Some(magicValue) =>
          // We always use CREATE_TIME, like the producer. The conversion to LOG_APPEND_TIME (if necessary) happens automatically.
          val timestampType = TimestampType.CREATE_TIME
          val timestamp = time.milliseconds()

          // 每个tp分别生成一个record,组合成一个recordBatch记录
          val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
            // key: [Group Topic Partition]
            val key = GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition)
            // value: [offset metadata commitTimestamp expireTimestamp]
            val value = GroupMetadataManager.offsetCommitValue(offsetAndMetadata)
            // Record: [timestamp, key, value]
            new SimpleRecord(timestamp, key, value)
          }

          // 根据groupId获得该group存储offset对应的topicPartition
          // 这是专门存储offset的内部消息，topic名为：__consumer_offsets
          val offsetTopicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partitionFor(group.groupId))
          val buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(magicValue, compressionType, records.asJava))

          // 版本为2以下的客户端不支持提交事务offset
          if (isTxnOffsetCommit && magicValue < RecordBatch.MAGIC_VALUE_V2)
            throw Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT.exception("Attempting to make a transaction offset commit with an invalid magic: " + magicValue)
          // 生成MemoryRecordsBuilder用于后续的消息的append
          val builder = MemoryRecords.builder(buffer, magicValue, compressionType, timestampType, 0L, time.milliseconds(),
            producerId, producerEpoch, 0, isTxnOffsetCommit, RecordBatch.NO_PARTITION_LEADER_EPOCH)

          //逐条append到MemoryRecordsBuilder中
          records.foreach(builder.append)
          val entries = Map(offsetTopicPartition -> builder.build())

          // putCacheCallback()方法用于appendLog操作结束之后的回调，作用是
          def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
            // the append response should only contain the topics partition
            // 有且仅有一个tp能写入成功，且tp为上文中的通过groupId获取的内部topic的一个topicPartition，否则直接抛异常退出
            if (responseStatus.size != 1 || !responseStatus.contains(offsetTopicPartition))
              throw new IllegalStateException("Append status %s should only have one partition %s"
                .format(responseStatus, offsetTopicPartition))

            val status = responseStatus(offsetTopicPartition)
            val responseError = group.inLock {
              if (status.error == Errors.NONE) {
                // append log无错误且group状态不为Dead情况下，根据offset提交类型分别进行更新缓存信息
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      // 如果是TxnOffset的提交的完成操作
                      // 在pendingTransactionalOffsetCommits缓存中通过ProducerId找到对应的(tp, commitRecordMetadataAndOffset) key value对，
                      //更新value为 CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata)
                      // 即更新了appendedBatchOffset
                      group.onTxnOffsetCommitAppend(producerId, topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                    else
                      // 在pendingOffsetCommits缓存中移除，并将新的offset写入offsets缓存
                      group.onOffsetCommitAppend(topicPartition, CommitRecordMetadataAndOffset(Some(status.baseOffset), offsetAndMetadata))
                  }
                }
                Errors.NONE
              } else {
                if (!group.is(Dead)) {
                  // 异常处理，当group没有pending状态的offset提交时从openGroupsForProducer移除ProducerId对应的这个groupId
                  if (!group.hasPendingOffsetCommitsFromProducer(producerId))
                    removeProducerGroup(producerId, group.groupId)
                  // 根据类型从各自的缓存中移除pending的请求
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    if (isTxnOffsetCommit)
                      group.failPendingTxnOffsetCommit(producerId, topicPartition)
                    else
                      group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }

                // some log code ...
                // 将append log返回的code处理成client的error code...
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

            // 最终返回给客户的回调
            responseCallback(commitStatus)
          }

          if (isTxnOffsetCommit) {
            group.inLock {
              // 如果是Txn类型的offset提交，则先在缓存openGroupsForProducer中根据producerId添加groupId
              // openGroupsForProducer用于commit/abort marker秦秋接收到之后快速根据定位group
              addProducerGroup(producerId, group.groupId)
              // prepareTxnOffsetCommit()中往pendingTransactionalOffsetCommits里写入(TopicPartition -> CommitRecordMetadataAndOffset)
              group.prepareTxnOffsetCommit(producerId, offsetMetadata)
            }
          } else {
            group.inLock {
              // pendingOffsetCommits 缓存中加入offsetMetadata
              group.prepareOffsetCommit(offsetMetadata)
            }
          }

          // 调用replicamanager的appendRecords()方法，进行往本地的消息日志中写入records
          appendForGroup(group, entries, putCacheCallback)

        // 如果tp不是本broker上缓存里的分区，返回NOT_COORDINATOR交给客户端，让其进行重新查找COORDINATOR
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

　　GroupCoordinator.storeOffsets()方法的处理过程大概归纳为：

* 生成record, 
* 进行更新pending缓存进行prepare
* 进行log append
* 根据结果进行pending缓存更新

　　在较新的版本的Kafka中，Offset这些元信息已经不用Zookeer进行存储，而是作为拥有一个内部Topic的消息，与普通消息一样存储在消息日志中，并且通过Kafka本身的主从同步机制做到一致性的维护，这样Kafka的元信息与普通的用户消息就统一起来了。

　　另外，特别地Kafka支持同一个Group组合混合提交Tnx和普通的消费offset的提交，目前实现上Kafka只是打出了一个Warn信息进行提示。Kafka的作者并不建议这么混合使用，在使用时可以尽量避免，否则容易产生未预期的异常情况。

## <a id="conclusion">总结</a>

　　本文从KafkaComsumer.pollOnce()方法每次都会触发的maybeAutoCommitOffsetsAsync()方法讲起，如果客户端配置了自动提交offset的配置，客户端此时会进行异步提交offset信息。在Broker端，Kafka将Offset信息与普通的业务消息抽象成同样的处理方法，都写入broker的消息log中，并通过Kafka本身的同步机制进行主从同步。

