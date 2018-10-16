---
layout: post
category: [Kafka, Source Code]
title: Kafka log的读写分析
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　前面几篇中对于Fetch,Produce请求的日志的读写都只是泛泛的略过，本篇介绍Kafka的日志相关的设计细节。通过本章的学习也可以重新回过头去将前几篇完善一下。

　　为了对某个具体Topic的读写的负载均衡，Kafka的一个Topic可以分为多个Partition，不同的Partition可以分布在不同的broker，方便的实现水平拓展，减轻读写瓶颈。通过前面几篇博文的分析我们知道正常情况下Kafka保证一条消息只发送到一个分区，并且一个分区的一条消息只能由Group下的唯一一个Consumer消费，如果想重复消费则可以加入一个新的组。
<!--more-->

　　熟悉Kafka的同学都知道，Kafka的消息的读写都是存放在log文件中。一个broker的log文件放在一个目录下，而不同的Partition对应一个子目录，发送到broker上的消息将会顺序的append到对应的Partition对应的log文件中。每个Partition对应的log文件可以看成是无限长、可以在文件末尾进行append数据的文件，加速写入速度。实际实现中，每个Partition对应的日志文件又被切分成多个Segment，这种切分的设计可以在数据的清理，控制索引文件大小等方面带来优势。

　　因为分布式环境下的任何一个Broker都有宕机的风险，所以Kafka上每个Partition有可以设置多个副本，通过副本的主从选举，副本的主从同步等手段，保证数据的高可用，降低由于部分broker宕机带来的影响，当然为了达到这个目的，同一个Partition副本应该分布在不同的Broker、机架上，通过一定的分配算法来使得分布尽量分散。这部分不是本文的重点，后面有专门章节介绍主从同步。

## <a id="Kafka Log">Kafka Log</a>

**Segment**
　　下面我们来看一下TopicPartition的示意图。

<div align="center">
<img src="/assets/img/2018/06/13/Anatomy_of_a_topic.jpeg" width="60%" height="60%"/>
</div>

　　由上图我们可以看到，每个TopicPartition由一系列的Segment组成。这些Segment会在日志文件夹中有对应的日志文件、索引文件等。下面我们看某个Partition对应的log文件夹内容的示意文件列表:

```sh
log git:(master) ✗ ls
00000000000000000000.index
00000000000000000000.log
00000000000000043023.index
00000000000000043023.log
00000000000000090023.index
00000000000000090023.log
```

　　每个Segment都对应着base_offset.index,base_offset.log文件。这个base_offset代表这个Segment消息在整个消息中的基准偏移量，他会小于等于这个Segment中所有的消息的偏移，也严格大于前一个Segment中所有消息的偏移量。

　　因为Kafka对数据的处理是抽象为在一个无限长的日志文件后进行追加操作。因此为了能迅速检索到某个指定offset对应的消息，Kafka对日志文件都进行了索引。每个日志的Segment相应地对应一个索引文件OffsetIndex。下面来看索引及消息在某个具体Segment的示意结构图:

<div align="center">
<img src="/assets/img/2018/06/13/SegmentIndexAndLog.jpeg" width="40%" height="40%"/>
</div>

　　从图上看每个日志的segment对应一个index文件。index文件是稀疏的，即并不是每一个Record都会对应index文件里的一条，这样的设计可以有效的减小index文件的大小，使得可以载入内存，在内存中进行比较运算，虽然可能不能直接根据index直接找到某一个record,但是可以先通过二分的形式找到不大于要检索的offset的那个index记录，然后再往后顺序遍历即可找到。较新版本的Kafka还给每个Segment会配置TimeStampIndex，与OffsetIndex结构类似，区别是TimeStampIndex组成为8字节的时间戳和4字节的location。

　　Index的格式为8个字节组成一条记录，其中前4个字节标识消息在该Segment中的相对offset,后4个字节标识该消息在该Segment中的相对位置。

**Record**

　　从Kafka 0.11开始，改变了原来Message的称呼，现在log中写入的数据称为Record。以RecordBatch为单位写入，每个Batch中至少有一个Record。下图是根据源码中描绘的log中RecordBatch的数据结构:

<div align="center">
<img src="/assets/img/2018/06/13/RecordStruct.jpeg" width="30%" height="30%"/>
</div>

　　细心的话会留意到图中例如Record.length的类型为Varint，还有TimeStampDelta用的是Varlong。这是借鉴了Google Protocol Buffers的*zigzag*编码。有效的降低Batch的空间占用。当日志压缩开启时，会有后台线程定时进行日志压缩清理，用于减少日志的大小和提升系统速度。RecordBatch中的Record有可能会被压缩，而Header会保留未压缩的状态。

## <a id="Produce">Produce</a>

　　由上述的介绍我们对Kafka的log有了一个直观的印象，前几篇博文对日志的读写部分都一带而过。现在结合Kafka处理的fetch和produce请求最后日志的读写具体的代码细节来进行源码分析。

```scala
  //Log.scala
  private def append(records: MemoryRecords, isFromClient: Boolean, assignOffsets: Boolean, leaderEpoch: Int): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
      //对消息进行校验，每条消息校验CRC，size,
      //并且该方法返回LogAppendInfo，其中包含第一条和最后一条record的offset,records数目，validBytesCount，offset是否是单调递增等信息
      val appendInfo = analyzeAndValidateRecords(records, isFromClient = isFromClient)

      // 没有可以append的信息直接返回
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // 将未通过验证的消息数据trim掉
      var validRecords = trimInvalidBytes(records, appendInfo)

      // 将验证通过的消息插入日志中
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (assignOffsets) {
          // 需要为record进行设置offset和MaxTimestamp
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          //得到第一条消息的offset
          appendInfo.firstOffset = Some(offset.value)
          //服务器当前时间作为时间戳
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.recordVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              isFromClient)
          } catch {
            case e: IOException =>
              throw new KafkaException(s"Error validating messages while appending to log $name", e)
          }

          //根据重新赋值过的offset和timestamp更新appendInfo成实际的值
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordsProcessingStats = validateAndOffsetAssignResult.recordsProcessingStats
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // 如果存在压缩或者是格式转换，消息的大小需要重新验证
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // record && throw RecordTooLargeException()
                // other code ...
              }
            }
          }
        } else {
          // offset非单调增加或者是appendInfo的firstOffset or lastOffset < 已存在的offset 则抛异常
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOrLastOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException(s"Out of order offsets found in append to $topicPartition: " +
              records.records.asScala.map(_.offset))
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        validRecords.batches.asScala.foreach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
            _leaderEpochCache.assign(batch.partitionLeaderEpoch, batch.baseOffset)
        }

        // 消息大小超过了配置的segment的大小，数据大，一个装不下，抛异常
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
            s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
        }

        // analyzeAndValidateProducerState方法中过滤一遍validRecords，
        // 将重复的，可更新状态的、可完成的txn分别取出来
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(validRecords, isFromClient)
        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = Some(duplicate.firstOffset)
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        //LogSegment.shouldRoll()方法判断是否需要生成一个新的Segment: 当前Segment剩余空间不足以容纳 or 当前segment不为空但等待时间已到
        // or offsetIndex/timeIndex索引文件满了，or offset相对于base_offset超过int32.max_value
        val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOrLastOffset,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        //在当前的segment中append消息
        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // 更新Producer的状态，并且将transactions放入ongoingTxns队列中
        for ((producerId, producerAppendInfo) <- updatedProducers) {
          producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
          producerStateManager.update(producerAppendInfo)
        }

        // 完成事务
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.completeTxn(completedTxn)
          segment.updateTxnIndex(completedTxn, lastStableOffset)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // increment the log end offset
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        updateFirstUnstableOffset()

        // 上一次检查点到现在累加的数据超过flushInterval则调用flush将缓存中的数据写入磁盘持久化
        if (unflushedMessages >= config.flushInterval)
          flush()

        appendInfo
      }
    }
  }
```
　　ReplicaManager.append()方法中首先验证消息的合法性，并将没有通过检验的部分tirm掉。并根据得到的Record集合更新offset,timestamp等信息。如果保留的数据大小超过当前Segment剩余的空间或者是其它如offset过大等都会触发日志的roll行为。即生成一个新的Segment并作为当前Segment。Record的append操作都是在当前Segment中进行，经过上述一系列的操作之后调用LogSegment.append()方法将内存Record写入Segment中。

```scala
  //LogSegment.scala
  def append(largestOffset: Long,
             largestTimestamp: Long,
             shallowOffsetOfMaxTimestamp: Long,
             records: MemoryRecords): Unit = {
    if (records.sizeInBytes > 0) {
      val physicalPosition = log.sizeInBytes()

      // 如果当前log中数据大小为0，设置rollingBasedTimestamp 为largestTimestamp
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)
      // append the messages
      require(canConvertToRelativeOffset(largestOffset), "largest offset in message set can not be safely converted to relative offset.")
      // 将数据写入log中
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file()} at end offset $largestOffset")


      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
      }

      //bytesSinceLastIndexEntry记录插入的数据的大小的累计，当累计超过indexIntervalBytes时，
      // 在offsetIndex中增加一条索引记录，以(largestOffset, physicalPosition)作为索引
      // 并清空累加值，同时尝试添加一条timeIndex记录，当maxTimestampSoFar, offsetOfMaxTimestamp都大于timeIndex中的最后一条记录对应的值
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }
```

　　至此，我们将Produce请求日志写入部分介绍完了。下面来看Fetch请求处理过程中Broker对日志的读取过程的实现。

## <a id="Fetch">Fetch</a>

　　为了研读从日志读取数据的源码，这里再一次地贴一下[Kafka Consumer(二)](https://daleizhou.github.io/posts/consume-messages.html)中贴过的ReplicaManager.readFromLocalLog()方法。当时未能结合日志具体实现来深入分析，这里结合日志部分的内容重新讲解一遍。

```scala
  // ReplicaManager.scala
  def readFromLocalLog(replicaId: Int,
                       fetchOnlyFromLeader: Boolean,
                       readOnlyCommitted: Boolean,
                       fetchMaxBytes: Int,
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)],
                       quota: ReplicaQuota,
                       isolationLevel: IsolationLevel): Seq[(TopicPartition, LogReadResult)] = {
    // 单个topicPartition分别调用一次read()方法
    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      // fetch开始的偏移量
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      // 统计相关的
      brokerTopicStats.topicStats(tp.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      try {

        // decide whether to only fetch from leader
        val localReplica = if (fetchOnlyFromLeader)
          getLeaderReplicaIfLocal(tp)
        else
          getReplicaOrException(tp)

        //当前messageOffset作为初始化的HW，因为messageOffset在读取过程中会发生变化，这里复制出来
        val initialHighWatermark = localReplica.highWatermark.messageOffset
        // 根据隔离级别设定lastStableOffset变量
        // 如果是READ_COMMITTED级别，则lastStableOffset设定为lastStableOffset.messageOffset，否则为None
        val lastStableOffset = if (isolationLevel == IsolationLevel.READ_COMMITTED)
          Some(localReplica.lastStableOffset.messageOffset)
        else
          None

        // 根据是否只读committed来设置读取的最大offset,否则没有上限，后面具体读取时会被
        val maxOffsetOpt = if (readOnlyCommitted)
          Some(lastStableOffset.getOrElse(initialHighWatermark))
        else
          None

        val initialLogEndOffset = localReplica.logEndOffset.messageOffset
        val initialLogStartOffset = localReplica.logStartOffset
        val fetchTimeMs = time.milliseconds
        val logReadInfo = localReplica.log match {
          case Some(log) =>
            // 调整读取大小的上限
            val adjustedFetchSize = math.min(partitionFetchSize, limitBytes)

            // 从log中读取数据
            val fetch = log.read(offset, adjustedFetchSize, maxOffsetOpt, minOneMessage, isolationLevel)

            // 限速处理
            if (shouldLeaderThrottle(quota, tp, replicaId))
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            // 异常处理
            else if (!hardMaxBytesLimit && fetch.firstEntryIncomplete)
              FetchDataInfo(fetch.fetchOffsetMetadata, MemoryRecords.EMPTY)
            else fetch

          case None =>
            // 异常处理 ...
        }

        // 单次对TopicPartiton读取结果
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
        // 异常处理 ...
      }
    }

    var limitBytes = fetchMaxBytes
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    var minOneMessage = !hardMaxBytesLimit
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // 如果一旦读到至少一条消息，minOneMessage这个标志已经没有作用，因此设置为false,后续的Partition就不需要至少读取一条
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }
```

　　ReplicaManager遍历需要读取的TopicPartiton调用内部read()方法逐一对Tp对应的log进行读取。如果读取到的数据量大小达到了获取的最小限制，则返回结构，否则继续读取。每一轮读取过程中实际会调用Log.read()方法根据其实位置，最长大小等信息读取记录，代码如下:

```scala
  //Log.scala
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false,
           isolationLevel: IsolationLevel): FetchDataInfo = {
    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading $maxLength bytes from offset $startOffset of length $size bytes")

      // 因为读不加锁，所以讲offsetMetadata复制出来避免成为临界资源
      val currentNextOffsetMetadata = nextOffsetMetadata
      val next = currentNextOffsetMetadata.messageOffset
      // 如果读取开始的offset == 日志最后的偏移量，即没有消息可读，立即返回结果
      if (startOffset == next) {
        val abortedTransactions =
          if (isolationLevel == IsolationLevel.READ_COMMITTED) Some(List.empty[AbortedTransaction])
          else None
        return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false,
          abortedTransactions = abortedTransactions)
      }

      // 获取到base_offset小于等于startOffset的那个segment，将读取的开始位置定位到具体的segment
      var segmentEntry = segments.floorEntry(startOffset)

      // 如果开始读取的startOffset > log最后的offset or
      // segmentEntry不存在 or
      // 读取的开始 < logStartOffset (日志清理等造成了起始位置不可读)
      // 复核上述情况之一则抛异常提示读取范围超过有效范围
      if (startOffset > next || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $logStartOffset to $next.")

      // 在segment中读取，如果当前segment根据传入的offset, size等参数无法读取数据，则向前获取下一个segment读取
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        // 如果当前segment为active segment, 则需要考虑在读取时有数据进行append,但是在更新nextOffsetMetadata之前有两个fetch请求，这样可能会导致OffsetOutOfRangeException， 因此缓存当前的位置，只读到现在的位置就结束
        val maxPosition = {
          if (segmentEntry == segments.lastEntry) {
            val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong

            // 可能在此时已经发生了roll动作，当前segment已经不为active segment，则直接将log.sizeInBytes() 作为maxPosition，
            // 否则将刚刚缓存缓存的日志的相对结尾位置作为maxPosition
            if (segmentEntry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
              segment.size
            else
              exposedPos
          } else {
            // 如果不是最后一个segment,即不存在读写竞争则maxPosition = log.sizeInBytes()
            segment.size
          }
        }
        val fetchInfo = segment.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
        if (fetchInfo == null) {
          // 读取结果为null，取下一个Segment进行读取，直到读到数据 or 直到没有segment可读
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          // 如果隔离级别为读未提交，则直接返回读到的结果，
          // 如果隔离级别为读提交，则addAbortedTransactions
          return isolationLevel match {
            case IsolationLevel.READ_UNCOMMITTED => fetchInfo
            case IsolationLevel.READ_COMMITTED => addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          }
        }
      }

      // 如果执行到这里，没有读取到数据， 返回empty
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }
```

　　定位到具体的开始读取的Segment之后调用LogSegment.read()方法进行record的读取。在必要的offset边界条件检查后，因为读取未加锁，如果读取是在ActiveSegment上读取，缓存当前Segment的最大位置作为读取的边界。在指定的Segment上通过LogSegment.read()方法可以进行数据的读取，如果当前Segment未能读取则依次读取下一个Segment，如果读取成功则再根据隔离级别做结果的调整。

```scala
  //LogSegment.scala
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int, maxPosition: Long = size,
           minOneMessage: Boolean = false): FetchDataInfo = {
    // 参数检查，要求maxSize不小于0，否则抛参数异常
    // other code ...

    // 读取过程中log的size会随着别的线程而改变，因此缓存下来
    val logSize = log.sizeInBytes

    // 通过translateOffset方法找到第一个offset >= startOffset的record
    // 这个方法下面会详细讲解
    val startOffsetAndSize = translateOffset(startOffset)

    // 如果start的位置超过了end,则startOffsetAndSize为null，直接返回null，让外层读取下一个Segment
    if (startOffsetAndSize == null)
      return null

    val startPosition = startOffsetAndSize.position
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition)

    // 如果设置了minOneMessage， 则adjustedMaxSize尽量的大，尽量至少读取一个record,即便超过maxSize
    // 否则读取数据量大小上限设置为maxSize
    val adjustedMaxSize =
      if (minOneMessage) math.max(maxSize, startOffsetAndSize.size)
      else maxSize

    // 如果空segment,则直接返回空结果
    if (adjustedMaxSize == 0)
      return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY)

    // 获取本次读取的size
    val fetchSize: Int = maxOffset match {
      // 如果maxSize没有指定，则读取消息直到最大的position
      case None =>
        min((maxPosition - startPosition).toInt, adjustedMaxSize)
      case Some(offset) =>
        // 可能由于主从切换，当前HM小于前Leader的HM,会导致offset < startOffset发生，最大位置小于读取的起始位置，则返回空结果
        if (offset < startOffset)
          return FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, firstEntryIncomplete = false)
        // 生成结束位置的position信息
        val mapping = translateOffset(offset, startPosition)
        val endPosition =
          if (mapping == null)
            logSize // the max offset is off the end of the log, use the end of the file
          else
            mapping.position
        // min(maxPosition, endPosition) - startPosition 与adjustedMaxSize 的最小值作为本次读取的大小
        min(min(maxPosition, endPosition) - startPosition, adjustedMaxSize).toInt
    }
    // 根据startPosition和fetchsize调用log.read()方法从日志中读取数据
    FetchDataInfo(offsetMetadata, log.read(startPosition, fetchSize),
      firstEntryIncomplete = adjustedMaxSize < startOffsetAndSize.size)
  }

  // 找到第一条offset >= 传入的offset的record
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): LogOffsetPosition = {
    // 在当前LogSegment对应的offsetIndex中，调用offsetIndex.lookup()方法通过二分查找，找到offset <= 传入的offset，该方法返回Pair(offset, 该消息在log中相对的物理地址)
    // 如果offsetIndex找不到符合条件的记录，则返回(base_offset,0)
    val mapping = offsetIndex.lookup(offset)

    // startingFilePosition用于控制跳过不必要的起始检索位置，如果缺省默认取0，
    // 与mapping.position去较大值作为真正的起始检索位置
    // log.searchForOffsetWithSize()从真正其实位置开始找到第一个大于等于offset的recotrBatch，返回相关信息作为读取消息的LogOffsetPosition
    log.searchForOffsetWithSize(offset, max(mapping.position, startingFilePosition))
  }
```

　　至此，Kafka的Broker端对消息的读取的过程也分析完毕。

## <a id="conclusion">总结</a>

　　本文从Kafka的日志的实际结构切入，介绍了一个正常的Topic的不同Partition的分布及Segment的结构和每个Segment中具体的消息的结构示意。在这些概念的基础上，本文后半部分对前面的ProduceRequest，FetchRequest的日志处理部分做了源码分析，对于前面的几篇博文也是一个补充分析。

## <a id="references">References</a>

* https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
* http://code.google.com/apis/protocolbuffers/docs/encoding.html
* http://matt33.com/2018/03/18/kafka-server-handle-produce-request/#%E6%97%A5%E5%BF%97%E5%86%99%E5%85%A5





