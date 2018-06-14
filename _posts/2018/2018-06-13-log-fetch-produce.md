---
layout: post
category: Kafka
title: Kafka log的读写分析
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　前面几篇中对于Fetch,Produce请求的日志的读写都只是泛泛的略过，本篇介绍Kafka的日志相关的设计细节。通过本章的学习也可以重新回过头去将前几篇完善一下。

　　熟悉Kafka的同学都知道，Kafka的消息的读写都是存放在log文件中。一个broker的log文件放在一个目录下，而不同的Partition对应一个子目录，发送到broker上的消息将会顺序的append到对应的Partition对应的log文件中。每个Partition对应的log文件可以看成是无限长、可以在文件末尾进行append数据的文件，加速写入速度。实际实现中，每个Partition对应的日志文件又被切分成多个Segment，这种切分的设计可以在数据的清理，控制索引文件大小等方面带来优势。

　　为了对某个具体Topic的读写的负载均衡，Kafka的一个Topic可以分为多个Partition，不同的Partition可以分布在不同的broker，方便的实现水平拓展，减轻读写瓶颈。通过前面几篇博文的分析我们知道正常情况下Kafka保证一条消息只发送到一个分区，并且一个分区的一条消息只能由Group下的唯一一个Consumer消费，如果想重复消费则可以加入一个新的组。

　　因为分布式环境下的任何一个Broker都有宕机的风险，所以Kafka上每个Partition有可以设置多个副本，通过副本的主从选举，副本的主从同步等手段，保证数据的高可用，降低由于部分broker宕机带来的影响，当然为了达到这个目的，同一个Partition副本应该分布在不同的Broker、机架上。这部分不是本文的重点，后面有专门章节介绍主从同步。

## <a id="Kafka Log">Kafka Log</a>

**Segment**
　　下面我们来看一下TopicPartition的示意图。

<div align="center">
<img src="/assets/img/2018/06/13/Anatomy_of_a_topic.png" />
</div>

　　由上图我们可以看到，每个TopicPartition由一系列的Segment组成。这些Segment会在日志文件夹中有对应的日志文件、索引文件等。下面我们看笔者运行的集群的某个Partition对应的log文件夹内容:

```sh
ll

TODO
```

　　每个Segment都对应着base_offset.index,base_offset.log文件。这个base_offset代表这个Segment消息在整个消息中的基准偏移量，他会小于等于这个Segment中所有的消息的偏移，也严格大于前一个Segment中所有消息的偏移量。

　　因为Kafka对数据的处理是抽象为在一个无限长的日志文件后进行追加操作。因此为了能迅速检索到某个指定offset对应的消息，Kafka对日志文件都进行了索引。每个日志的Segment相应地对应一个索引文件OffsetIndex。下面来看索引及消息在某个具体Segment的示意结构图:

<div align="center">
<img src="/assets/img/2018/06/13/SegmentIndexAndLog.png" />
</div>

　　从图上看每个日志的segment对应一个index文件。index文件是稀疏的，即并不是每一个Record都会对应index文件里的一条，这样的设计可以有效的减小index文件的大小，使得可以载入内存，在内存中进行比较运算，虽然可能不能直接根据index直接找到某一个record,但是可以先通过二分的形式找到不大于要检索的offset的那个index记录，然后再往后顺序遍历即可找到。

　　Index的格式为8个字节组成一条记录，其中前4个字节标识消息在该Segment中的相对offset,后4个字节标识该消息在该Segment中的相对位置。

**Record**

　　从Kafka 0.11开始，改变了原来Message的称呼，现在log中写入的数据称为Record。以RecordBatch为单位写入，每个Batch中至少有一个Record。下图是根据源码中描绘的log中RecordBatch的数据结构:

<div align="center">
<img src="/assets/img/2018/06/13/RecordStruct.png" />
</div>

　　细心的话会留意到图中例如Record.length的类型为Varint，还有TimeStampDelta用的是Varlong。这是借鉴了Google Protocol Buffers的*zigzag*编码。有效的降低Batch的空间占用。当日志压缩开启时，会有后台线程定时进行日志压缩清理，用于减少日志的大小和提升系统速度。RecordBatch中的Record有可能会被压缩，而Header会保留未压缩的状态。


　　由上述的介绍我们对Kafka的log有了一个直观的印象，前几篇博文对日志的读写部分都一带而过。现在结合Kafka处理的fetch和produce请求最后日志的读写具体的代码细节来进行源码分析。

## <a id="Produce">Produce</a>

```scala
//ReplicaManager.scala
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

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        // analyzeAndValidateProducerState方法中过滤一遍validRecords，
        // 将重复的，可更新状态的，可完成的txn分别取出来
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

        // update the producer state
        for ((producerId, producerAppendInfo) <- updatedProducers) {
          producerAppendInfo.maybeCacheTxnFirstOffsetMetadata(logOffsetMetadata)
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
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

        trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
          s"first offset: ${appendInfo.firstOffset}, " +
          s"next offset: ${nextOffsetMetadata.messageOffset}, " +
          s"and messages: $validRecords")

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
      trace(s"Inserting ${records.sizeInBytes} bytes at end offset $largestOffset at position ${log.sizeInBytes} " +
            s"with largest timestamp $largestTimestamp at shallow offset $shallowOffsetOfMaxTimestamp")
      val physicalPosition = log.sizeInBytes()
      if (physicalPosition == 0)
        rollingBasedTimestamp = Some(largestTimestamp)
      // append the messages
      require(canConvertToRelativeOffset(largestOffset), "largest offset in message set can not be safely converted to relative offset.")
      val appendedBytes = log.append(records)
      trace(s"Appended $appendedBytes to ${log.file()} at end offset $largestOffset")
      // Update the in memory max timestamp and corresponding offset.
      if (largestTimestamp > maxTimestampSoFar) {
        maxTimestampSoFar = largestTimestamp
        offsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp
      }
      // append an entry to the index (if needed)
      if(bytesSinceLastIndexEntry > indexIntervalBytes) {
        offsetIndex.append(largestOffset, physicalPosition)
        timeIndex.maybeAppend(maxTimestampSoFar, offsetOfMaxTimestamp)
        bytesSinceLastIndexEntry = 0
      }
      bytesSinceLastIndexEntry += records.sizeInBytes
    }
  }
```


## <a id="Fetch">Fetch</a>




## TODO

## <a id="references">References</a>

* https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
* http://code.google.com/apis/protocolbuffers/docs/encoding.html





