---
layout: post
category: Kafka
title: Kafka Log Compaction
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　LogManager的另一个重要组成部分为日志的压缩清理线程。在具体跟踪源码进行分析前，先来根据官方文档来理解一下日志的压缩清理，以便于有一个宏观的印象。

## <a id="summary">简介</a>

　　Kafka在设计上，新的数据是往顺序日志文件中添加来写入，只往日志末尾进行append操作，这种设计可以带来很好的写入性能，在写入时不需要进行查询操作。对于同一个key,后续如果客户端更新了它对应的value，Kakfa保留对应所有版本的旧的数据值，而不直接修改替换相同key对应的value。为了让客户端在某个时刻获取最新版本的值，Kafka在后台会有压缩线程，定期进行日志的压缩清理工作。跟着官方文档我们来看一下具体的工作过程。
<!--more-->

<div align="center">
<img src="/assets/img/2018/06/28/LogCompaction.jpeg" width="60%" height="60%"/>
</div>

　　上图为Kafka日志压缩过程的示意图。从图上可以看出，Kafka日志压缩的过程是保留相同key的最新版本的值，而旧版本的记录则从日志中删除。在日志压缩过程中，Kafka任然保持数据的顺序，保持和写入时的顺序是一致的。并且压缩过程中也不会修改消息的offset值，仍然标识为该消息在整个消息中的绝对偏移量。因为日志压缩过程中会删除数据，因此Consumer在消费过程中很容易看到offset不连续的情况。

<div align="center">
<img src="/assets/img/2018/06/28/HeadTailClean.jpeg" width="60%" height="60%"/>
</div>

　　上图从更宏观的角度来看Kafka的日志压缩。Kafka是以Segment为单位进行压缩，因此ActiveSegment是不参与压缩的。上图中弱化了Segment，并没有画Segment的分界。除了ActiveSegment部分，日志分为Tail和Head部分，而Tail和Head部分为CleanerPoint,标识第一条未进行清理的位置。Tail为已经压缩清理完的部分，而Head则表示未压缩清理的部分。

　　有一点需要说明的地方，消费者进行消费信息的时候，因为消费顺序和写入顺序一致，Kafka可以保证消费者看到记录按照他们写入顺序的最终版本值。当然读取到某一位置时，某个key对应的value值在更后面可能有更新，但是在当前位置下能看到的版本为当前位置的最终版本，并不是指的是全局最终版本。

　　在上述宏观印象基础上我们一起来看日志压缩清理部分的源码。



```scala
  // LogCleanerManager.scala 

  // 返回tp对应log可以被清理的区间
  def cleanableOffsets(log: Log, topicPartition: TopicPartition, lastClean: immutable.Map[TopicPartition, Long], now: Long): (Long, Long) = {

    // 获得该tp的对应上一次的checkoutpoint
    val lastCleanOffset: Option[Long] = lastClean.get(topicPartition)

    val logStartOffset = log.logSegments.head.baseOffset

    // 找到第一个可以被压缩清理的offset
    val firstDirtyOffset = {
      // offset标识日志应该要开始清理的起始offset，如果没有上一次clean的检查点信息，则取现存所有segment中第一个segment的起始offset
      val offset = lastCleanOffset.getOrElse(logStartOffset)
      // 如果offset<logStartOffset,则说明发生过日志的截断
      if (offset < logStartOffset) {
        // 则 截断后的起始offset作为第一条未清理的offset
        logStartOffset
      } else {
        offset
      }
    }

    val compactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // 找到第一个不可以被压缩清理的segment
    val firstUncleanableDirtyOffset: Long = Seq(

      // 未完成的事务不压缩清理
      log.firstUnstableOffset.map(_.messageOffset),

      // activeSegment不压缩清理
      Option(log.activeSegment.baseOffset),

      if (compactionLagMs > 0) {
        val dirtyNonActiveSegments = log.logSegments(firstDirtyOffset, log.activeSegment.baseOffset)
        // 找到第一个最大timestamp 距离现在已经超过压缩延迟时间的那个Segment的baseOffset
        dirtyNonActiveSegments.find { s =>
          val isUncleanable = s.largestTimestamp > now - compactionLagMs
          isUncleanable
        }.map(_.baseOffset)
      } else None
    ).flatten.min // 从中选取最小的那个offset作为第一个不能被清理的offset

    (firstDirtyOffset, firstUncleanableDirtyOffset)
  }

  def grabFilthiestCompactedLog(time: Time): Option[LogToClean] = {
    inLock(lock) {
      val now = time.milliseconds
      this.timeOfLastRun = now
      // 获取broker上所有log对应的最后的checkpoint
      val lastClean = allCleanerCheckpoints
      // 获取未压缩过并且不在inProgress队列中的日志大小不为空的那些日志
      // 为每个满足条件的分区日志创建LogToClean实例
      val dirtyLogs = logs.filter {
        case (_, log) => log.config.compact  
      }.filterNot {
        case (topicPartition, _) => inProgress.contains(topicPartition) 
      }.map {
        case (topicPartition, log) => 
          // 取出第一条可被清理的offset，和第一条不可被清理的offset,作为清理的区间
          val (firstDirtyOffset, firstUncleanableDirtyOffset) = LogCleanerManager.cleanableOffsets(log, topicPartition,
            lastClean, now)
          // 每个TopicPartition, log 生成一个LogToClean, 标识出第一条dirty的offset，和清理的上界
          LogToClean(topicPartition, log, firstDirtyOffset, firstUncleanableDirtyOffset)
      }.filter(ltc => ltc.totalBytes > 0) // skip any empty logs

      // 得到最大压缩比例日志的压缩比例
      // 压缩比例为：待压缩的size / (已压缩size + 待压缩的size)
      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
      // 过滤掉那些待压缩清理的日志比例低于minCleanableRatio的那些日志
      val cleanableLogs = dirtyLogs.filter(ltc => ltc.cleanableRatio > ltc.log.config.minCleanableRatio)
      if(cleanableLogs.isEmpty) {
        None
      } else {
        // 更新inProgress队列，将压缩比例最大的那个放入队列
        val filthiest = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }
```

　　上述代码的作用是挑选一个压缩比例最高的，并且高于一定阈值的日志的待压缩区间，并生成LogClean对象用于控制起止步位置，计算压缩率等。每个TopicPartition对应日志的压缩终止位置是由三个值共同决定。首先压缩区域不能包含未完成的事务等，第二压缩区域不能包含activeSegment，第三找到第一个非activeSegment并且最大的时间戳已经超过清理等待时间。这三个值中取最小值来作为清理压缩的截止位置。

```scala
  // LogCleaner.scala
  private def cleanOrSleep() {
      // 获取压缩比例最高的那个log进行清理
      val cleaned = cleanerManager.grabFilthiestCompactedLog(time) match {
        case None =>
          false
        case Some(cleanable) =>
          var endOffset = cleanable.firstDirtyOffset
          try {
            // cleaner.clean()进行清理日志，并返回第一个未被清理的offset和统计信息
            val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
            recordStats(cleaner.id, cleanable.log.name, cleanable.firstDirtyOffset, endOffset, cleanerStats)
            endOffset = nextDirtyOffset
          } catch {
            // 异常处理 ...
          } finally {
            cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.dir.getParentFile, endOffset)
          }
          true
      }
      val deletable: Iterable[(TopicPartition, Log)] = cleanerManager.deletableLogs()
      deletable.foreach{
        case (topicPartition, log) =>
          try {
            log.deleteOldSegments()
          } finally {
            cleanerManager.doneDeleting(topicPartition)
          }
      }
      if (!cleaned)
        pause(config.backOffMs, TimeUnit.MILLISECONDS)
    }

  private[log] def clean(cleanable: LogToClean): (Long, CleanerStats) = {
    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    val deleteHorizonMs =
      cleanable.log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - cleanable.log.config.deleteRetentionMs
    }

    doClean(cleanable, deleteHorizonMs)
  }

  // 构建（key_hash -> offset） 这样的map用于日志的压缩清理
  private[log] def buildOffsetMap(log: Log,
                                  start: Long,
                                  end: Long,
                                  map: OffsetMap,
                                  stats: CleanerStats) {
    map.clear()
    // 得到待压缩清理的segment
    val dirty = log.logSegments(start, end).toBuffer

    // 收集起止范围内的被中断的事务
    val abortedTransactions = log.collectAbortedTransactions(start, end)
    // 由abortedTransactions生成CleanedTransactionMetadata用于追踪压缩清理日志过程中事务的状态
    // CleanedTransactionMetadata由abortedTransactions，及对应的事务index初始化，
    // 在日志压缩清理过程中，事务的状态会发生变化。
    //CleanedTransactionMetadata决定什么时候删除事务标记和相应地更新事务索引
    val transactionMetadata = CleanedTransactionMetadata(abortedTransactions)

    // full用于标识offset是否已经满，满的标准是map.slot * factor
    // full 为true时，则终止了下面的for循环
    var full = false 
    for (segment <- dirty if !full) {
      // 判断TopicPartition对应的日志有没有被标记为LogCleaningAborted
      // 如果是，则这里会抛出异常
      checkDone(log.topicPartition)

      // 逐个Segment的消息加入offset map中
      full = buildOffsetMapForSegment(log.topicPartition, segment, map, start, log.config.maxMessageSize,
        transactionMetadata, stats)
    }
  }

  // 单个Segment的消息加入offset map中
  private def buildOffsetMapForSegment(topicPartition: TopicPartition,
                                       segment: LogSegment,
                                       map: OffsetMap,
                                       startOffset: Long,
                                       maxLogMessageSize: Int,
                                       transactionMetadata: CleanedTransactionMetadata,
                                       stats: CleanerStats): Boolean = {
    // 获取开始读取的位置
    var position = segment.offsetIndex.lookup(startOffset).position
    // 是否已满，map如果已经定义为full,则这一轮的日志压缩清理工作就此结束
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    while (position < segment.log.sizeInBytes) {
      // 再次判断TopicPartition对应的日志有没有被标记为LogCleaningAborted
      // 如果是，则这里会抛出异常
      checkDone(topicPartition)
      readBuffer.clear()
      try {
        //读满buffer或者读到文件结束
        segment.log.readInto(readBuffer, position)
      } catch {
        // 异常处理
      }
      val records = MemoryRecords.readableRecords(readBuffer)
      throttler.maybeThrottle(records.sizeInBytes)

      val startPosition = position
      for (batch <- records.batches.asScala) {
        if (batch.isControlBatch) {
          // 更新事务状态， 如果该controlBatch可以丢弃则返回true
          // 如果batch对应的type为abort, 且
          transactionMetadata.onControlBatchRead(batch)
          stats.indexMessagesRead(1)
        } else {
          val isAborted = transactionMetadata.onBatchRead(batch)
          if (isAborted) {
            // 如果是丢弃结果，则不修改map,只记录统计信息
            stats.indexMessagesRead(batch.countOrNull)
          } else {
            // 不丢弃，则将batch中offset>startOffset消息逐条写入map中
            for (record <- batch.asScala) {
              if (record.hasKey && record.offset >= startOffset) {
                if (map.size < maxDesiredMapSize)
                  // map记录key和offset
                  map.put(record.key, record.offset)
                else
                  return true
              }
              stats.indexMessagesRead(1)
            }
          }
        }

        // 修改map的LatestOffset，用于记录map清理到消息中的最大offset
        if (batch.lastOffset >= startOffset)
          map.updateLatestOffset(batch.lastOffset)
      }
      val bytesRead = records.validBytes
      position += bytesRead
      stats.indexBytesRead(bytesRead)

      // 一条消息都没读到，扩大buffer
      if(position == startPosition)
        growBuffersOrFail(segment.log, position, maxLogMessageSize, records)
    }
    // 清理buffer
    restoreBuffers()
    false
  }

  // 压缩清理工作实际实现方法
  private[log] def doClean(cleanable: LogToClean, deleteHorizonMs: Long): (Long, CleanerStats) = {

    val log = cleanable.log
    val stats = new CleanerStats()

    // 第一条不能清理的offset作为清理的上界
    val upperBoundOffset = cleanable.firstUncleanableOffset

    // 本次可压缩清理部分的所有消息用于生成一个map,记录保留消息的key，和offset
    buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats)
    val endOffset = offsetMap.latestOffset + 1
    stats.indexDone()

    // determine the timestamp up to which the log will be cleaned
    // this is the lower of the last active segment and the compaction lag
    val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)


    // group the segments and clean the groups
    info("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to %s)...".format(log.name, new Date(cleanableHorizonMs), new Date(deleteHorizonMs)))
    for (group <- groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize, log.config.maxIndexSize, cleanable.firstUncleanableOffset))
      cleanSegments(log, group, offsetMap, deleteHorizonMs, stats)

    // record buffer utilization
    stats.bufferUtilization = offsetMap.utilization

    stats.allDone()

    (endOffset, stats)
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   * @param stats Collector for cleaning statistics
   */
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment],
                                 map: OffsetMap,
                                 deleteHorizonMs: Long,
                                 stats: CleanerStats) {

    def deleteCleanedFileIfExists(file: File): Unit = {
      Files.deleteIfExists(new File(file.getPath + Log.CleanedFileSuffix).toPath)
    }

    // create a new segment with a suffix appended to the name of the log and indexes
    val firstSegment = segments.head
    deleteCleanedFileIfExists(firstSegment.log.file)
    deleteCleanedFileIfExists(firstSegment.offsetIndex.file)
    deleteCleanedFileIfExists(firstSegment.timeIndex.file)
    deleteCleanedFileIfExists(firstSegment.txnIndex.file)

    val baseOffset = firstSegment.baseOffset
    val cleaned = LogSegment.open(log.dir, baseOffset, log.config, time, fileSuffix = Log.CleanedFileSuffix,
      initFileSize = log.initFileSize, preallocate = log.config.preallocate)

    try {
      // clean segments into the new destination segment
      val iter = segments.iterator
      var currentSegmentOpt: Option[LogSegment] = Some(iter.next())
      while (currentSegmentOpt.isDefined) {
        val currentSegment = currentSegmentOpt.get
        val nextSegmentOpt = if (iter.hasNext) Some(iter.next()) else None

        val startOffset = currentSegment.baseOffset
        val upperBoundOffset = nextSegmentOpt.map(_.baseOffset).getOrElse(map.latestOffset + 1)
        val abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset)
        val transactionMetadata = CleanedTransactionMetadata(abortedTransactions, Some(cleaned.txnIndex))

        val retainDeletes = currentSegment.lastModified > deleteHorizonMs
        info(s"Cleaning segment $startOffset in log ${log.name} (largest timestamp ${new Date(currentSegment.largestTimestamp)}) " +
          s"into ${cleaned.baseOffset}, ${if(retainDeletes) "retaining" else "discarding"} deletes.")
        cleanInto(log.topicPartition, currentSegment.log, cleaned, map, retainDeletes, log.config.maxMessageSize,
          transactionMetadata, log.activeProducersWithLastSequence, stats)

        currentSegmentOpt = nextSegmentOpt
      }

      cleaned.onBecomeInactiveSegment()
      // flush new segment to disk before swap
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info(s"Swapping in cleaned segment ${cleaned.baseOffset} for segment(s) ${segments.map(_.baseOffset).mkString(",")} " +
        s"in log ${log.name}")
      log.replaceSegments(cleaned, segments)
    } catch {
      case e: LogCleaningAbortedException =>
        try cleaned.deleteIfExists()
        catch {
          case deleteException: Exception =>
            e.addSuppressed(deleteException)
        } finally throw e
    }
  }
```


## <a id="conclusion">总结</a>

## <a id="references">References</a>

* https://kafka.apache.org/documentation/#compaction
