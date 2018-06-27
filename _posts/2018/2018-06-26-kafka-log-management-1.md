---
layout: post
category: Kafka
title: Kafka Log Management(一)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　在[Kafka log的读写分析](https://daleizhou.github.io/posts/log-fetch-produce.html)一文中有介绍过Kafka日志的结构，并提及Kafka会定期清理Segment。本文来具体看一下后台线程是如何完成清理工作，除此之外还一并涉及Kafka日志管理的其它内容，如：日志刷新，日志检查点设置等管理工作。因为功能点比较多，拆成两篇进行撰写。

## <a id="StartUp">StartUp</a>

　　KafkaServer启动时会初始化一个LogManager并调用startup()方法进行启动。该模块即为Kafka的Broker上其中的一个后台线程，用于日志的管理操作，完成包含日志删除，日志检查点写入文件等工作。

```scala
   //KafkaServer.scala
   def startup() {
        // ...

        // 初始化一个线程池用于处理后台工作，该线程池的前缀为kafka-scheduler-
        // 具体工作线程为KafkaThread
        kafkaScheduler = new KafkaScheduler(config.backgroundThreads)
        kafkaScheduler.startup()

        /* start log manager */
        logManager = LogManager(config, initialOfflineDirs, zkClient, brokerState, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)
        logManager.startup()
        // ...
    }
```

　　KafkaServer启动过程中，构建一个线程池用于后台线程的执行。并将改线程池作为参数注入到LogManager中生成logManager。初始化完毕之后进行启动。LogManager初始化主要调用了createAndValidateLogDirs()方法和loadLogs()方法。下面来逐一进行跟踪源码。

```scala
  //LogManager.scala
  // 用于创建并验证日志目录的合法性
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    // 1. 确保传入的logDirs缓存中无重复目录，如果有则直接抛异常
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + dirs.mkString(", "))

    val liveLogDirs = new ConcurrentLinkedQueue[File]()

    for (dir <- dirs) {
      try {
        // liveLogDirs与offline日志目录不能有重叠情况
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")
        // 2. 如果日志目录不存在，则创建出来
        if (!dir.exists) {
          info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
          val created = dir.mkdirs()
          if (!created)
            throw new IOException("Failed to create data directory " + dir.getAbsolutePath)
        }
        // 3. 确保每个日志目录是个目录且有可读权限
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(dir.getAbsolutePath + " is not a readable log directory.")
        liveLogDirs.add(dir)
      } catch {
        // 异常处理 code ...
      }
    }

    // 工作log目录不能为空，一个都没有则直接退出
    if (liveLogDirs.isEmpty) {
      Exit.halt(1)
    }

    liveLogDirs
  }

```

　　Kafka的LogManager初始化时，通过createAndValidateLogDirs()方法进行创建和验证liveLogDir的正确性，要求liveLogDir不能有重复元素，与Offline不能有重叠，且有刻度权限。当目录不存在时则创建出来。

　　初始化完各种缓存信息，构建完各种目录结构之后就调用loadLogs()方法载入日志，只有载入完毕之后，Broker才能正常对外提供服务，后台的日志管理定时任务也才能开始进行工作。下面看这部分的源码实现:

```scala
  private def loadLogs(): Unit = {
    info("Loading logs.")
    val startMs = time.milliseconds
    val threadPools = ArrayBuffer.empty[ExecutorService]
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    for (dir <- liveLogDirs) {
      try {
        // 对每个日志目录创建numRecoveryThreadsPerDataDir个线程组成的线程池，并加入threadPools中
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
        threadPools.append(pool)

        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

        // 检查.kafka_cleanshutdown文件是否存在
        // 存在则表示Kafka正在经历清理性的停机工作，此时跳过从本文件夹恢复日志
        if (cleanShutdownFile.exists) {
          debug(s"Found clean shutdown file. Skipping recovery for all logs in data directory: ${dir.getAbsolutePath}")
        } else {
          // log recovery itself is being performed by `Log` class during initialization
          brokerState.newState(RecoveringFromUncleanShutdown)
        }
        // 从检查点文件读取Topic对应的恢复点offset信息
        // 文件为recovery-point-offset-checkpoint
        // checkpoint file format:
        // line1 : version
        // line2 : expectedSize
        // nlines: (tp, offset) 
        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          recoveryPoints = this.recoveryPointCheckpoints(dir).read
        } catch {
          // handle exception code ...
        }

        
        // 从检查点文件读取Topic对应的startoffset信息
        // 文件为log-start-offset-checkpoint
        // checkpoint file format:
        // line1 : version
        // line2 : expectedSize
        // nlines: (tp, startoffset) 
        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read
        } catch {
          // handle exception code ...
        }

        // 每个日志子目录生成一个线程池的具体job，并提交到线程池中
        // 每个job的主要任务是通过loadLog()方法加载日志
        val jobsForDir = for {
          dirContent <- Option(dir.listFiles).toList
          logDir <- dirContent if logDir.isDirectory
        } yield {
          CoreUtils.runnable {
            try {
              // 每个日志子目录生成一个job加载log，并且根据读取的recoveryPoints, logStartOffsets进行加载
              loadLog(logDir, recoveryPoints, logStartOffsets)
            } catch {
              // handle exception code ...
            }
          }
        }
        // 提交
        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
      } catch {
        // handle exception code ...
      }
    }

    try {
      // 等待所有日志加载的job完成，删除对应的cleanShutdownFile
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        try {
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            // handle exception code ...
        }
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        // handle exception code ...
    } finally {
      threadPools.foreach(_.shutdown())
    }

    // log code ...
  }

  // 线程池中的job主要工作
  private def loadLog(logDir: File, recoveryPoints: Map[TopicPartition, Long], logStartOffsets: Map[TopicPartition, Long]): Unit = {
    val topicPartition = Log.parseTopicPartitionName(logDir)
    val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    val log = Log(
      dir = logDir,
      config = config,
      logStartOffset = logStartOffset,
      recoveryPoint = logRecoveryPoint,
      maxProducerIdExpirationMs = maxPidExpirationMs,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      scheduler = scheduler,
      time = time,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel)

    // 如果以'-delete'为后缀，加入addLogToBeDeleted队列等待删除的定时任务
    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
      addLogToBeDeleted(log)
    } else {
      val previous = {
        if (log.isFuture)
          this.futureLogs.put(topicPartition, log)
        else
          this.currentLogs.put(topicPartition, log)
      }
      if (previous != null) {
        // handle exception code ...
      }
    }
  }

```

　　loadLog()方法主要是根据一些参数来对每个TopicPartition生成Log对象，用于对每个TopicPartition的日志的操作。主要初始化过程如下：

```scala
  // Log.scala
  class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var logStartOffset: Long,
          @volatile var recoveryPoint: Long,
          scheduler: Scheduler,
          brokerTopicStats: BrokerTopicStats,
          time: Time,
          val maxProducerIdExpirationMs: Int,
          val producerIdExpirationCheckIntervalMs: Int,
          val topicPartition: TopicPartition,
          val producerStateManager: ProducerStateManager,
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

      // 上一次flush的时间，用于日志管理中定期将内存数据刷入磁盘
      private val lastFlushedTime = new AtomicLong(time.milliseconds)

      // highWatermark
      @volatile private var replicaHighWatermark: Option[Long] = None

      // Kafka的log由顺序的Segment组成
      private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

      // 为TopicPartition构建LeaderEpoch => Offset 映射缓存
      @volatile private var _leaderEpochCache: LeaderEpochCache = initializeLeaderEpochCache()

      locally {
        val startMs = time.milliseconds
        // 载入Segment
        val nextOffset = loadSegments()

        /* Calculate the offset of the next message */
        nextOffsetMetadata = new LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

        _leaderEpochCache.clearAndFlushLatest(nextOffsetMetadata.messageOffset)

        logStartOffset = math.max(logStartOffset, segments.firstEntry.getValue.baseOffset)

        // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
        _leaderEpochCache.clearAndFlushEarliest(logStartOffset)

        loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)
      }
  }

  // 从磁盘中log文件载入log的Segment
  private def loadSegments(): Long = {
    // 清理临时文件，收集Swap文件，中断Swap操作。
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // 载入Segment 和Index文件，将Segment依次加入cache中
    loadSegmentFiles()

    // 完成被中断的swap操作。载入SwapSegment并替换对应的Segment,为了保持不crash系统，旧的log文件添加.deleted后缀，后面的定时任务或者下次的系统重启会删除
    completeSwapOperations(swapFiles)

    if (logSegments.isEmpty) {
      // 如果Segment列表为空，则新建第一个Segment作为起始
      addSegment(LogSegment.open(dir = dir,
        baseOffset = 0,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
      0
    } else if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      // 根据snapshot恢复Segment各种缓存，根据record进行事务处理初始化等工作
      val nextOffset = recoverLog()
      activeSegment.resizeIndexes(config.maxIndexSize)
      nextOffset
    } else 0
  }

```

　　经过上述过程，Kafka的Broker在启动时就初始化了一个LogManager用于日志的管理工作。初始化完毕之后，KafkaServer调用LogManager.startup()方法，正式将LogManager模块启动。启动过程设置了五个定时任务，分别有cleanupLogs，flushDirtyLogs，checkpointLogRecoveryOffsets，checkpointLogStartOffsets，deleteLogs等后台任务。

```scala
  // LogManager.scala
  def startup() {
    // 后台线程池不为空的情况下进行进行日志管理后台工作的启动
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      // 定期清理合适的segment
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      // 定期刷日志，将内存中未持久化的脏数据刷到磁盘上
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _,
                         delay = InitialTaskDelayMs,
                         period = flushCheckMs,
                         TimeUnit.MILLISECONDS)
      // 定期设置检查点写入到文件，避免启动时从整个文件进行恢复kafka
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushRecoveryOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      // 定期将当前log的start offset写入检查点文件，避免暴露被DeleteRecordsRequest删除的数据
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _,
                         delay = InitialTaskDelayMs,
                         period = flushStartOffsetCheckpointMs,
                         TimeUnit.MILLISECONDS)
      // 定时删除标记为-delete的日志
      scheduler.schedule("kafka-delete-logs", 
                         deleteLogs _,
                         delay = InitialTaskDelayMs,
                         unit = TimeUnit.MILLISECONDS)
    }
    if (cleanerConfig.enableCleaner)
      cleaner.startup()
  }


```

　　在StartUp中，deleteLogs定时任务比较特殊，并未直接设置调度间隔。执行这部分的后台线程是由前一次后台任务完成时动态进行调度的。下面分节来看具体的日志管理的源码实现。

## <a id="CleanupLogs">CleanupLogs</a>

　　Broker后台中的日志清理过程中，在全局配置了cleanup.policy设置为清理日志选项情况下，且某日志未设置日志压缩选项，则按照日志保留时间和日志的保留大小进行清理旧的Segment。下面看具体的清理过程。

```scala
  // LogManager.scala
  // 清理旧的Segment
  def cleanupLogs() {
    var total = 0
    val startMs = time.milliseconds
    for(log <- allLogs; if !log.config.compact) {
      // 如果log未设置日志压缩选项，定期调用deleteOldSegments()进行清理
      total += log.deleteOldSegments()
    }
    //log code ...
  }
```

　　cleanupLogs()对于那些未设置日志压缩选项的log,逐个调用Log.deleteOldSegments进行清理工作。当然还需要cleanup.policy的配置中设置删除日志，否则在deleteOldSegments()方法中就直接返回0，即不进行清理工作。

```scala
  //Log.scala
  def deleteOldSegments(): Int = {
    // 如果cleanup.policy配置中设置为不删除，则不进行实际的日志清理
    if (!config.delete) return 0

    // 否则通过过期时间，最大保留大小，startOffset等策略进行删除旧的Segment
    deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
  }

  private def deleteRetentionMsBreachedSegments(): Int = {
    // retentionMs过期时间为负，则永久保留，不进行清理
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    // 清理那些最后一条消息的时间戳都超时的Segment
    deleteOldSegments((segment, _) => startMs - segment.largestTimestamp > config.retentionMs,
      reason = s"retention time ${config.retentionMs}ms breach")
  }

  private def deleteRetentionSizeBreachedSegments(): Int = {
    // 如果retentionSize未负，则表示永久保留，如果当前Log的size < retentionSize,则无需清理
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    // 可以清理的最大清理大小
    var diff = size - config.retentionSize
    // deleteOldSegments()方法中用于过滤的callback
    // 如果当前Segment的size <= 剩余可以清理的最大清理大小则执行清理操作
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    deleteOldSegments(shouldDelete, reason = s"retention size in bytes ${config.retentionSize} breach")
  }

  // 清理那些最后一个offset < 当前log的logStartOffset的segment
  // 程序中用nextSegment的baseOffset来简化代码, 且如果nextSegment为Nil，则当前Segment为最后一个Segment，也不进行清理
  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]) =
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)

    deleteOldSegments(shouldDelete, reason = s"log start offset $logStartOffset breach")
  }

  // 从最旧的Segment开始逐个通过传入的偏函数进行验证是否需要清理，直到第一次不符合清理条件 
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean, reason: String): Int = {
    // 获得lock, 防止别的修改操作并行
    lock synchronized {

      val deletable = deletableSegments(predicate)
      if (deletable.nonEmpty)
        info(s"Found deletable segments with base offsets [${deletable.map(_.baseOffset).mkString(",")}] due to $reason")
      deleteSegments(deletable)
    }
  }

  // 从最老的Segment开始，逐个通过传入的偏函数验证决定是否为可删除的Segment 
  // 如果Segment列表为空或者replicaHighWatermark未设置，则不清理
  // kafka不清理那些offset在high watermark上面的那些segment，保证logStartOffset不超过HWM
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    if (segments.isEmpty || replicaHighWatermark.isEmpty) {
      Seq.empty
    } else {
      val highWatermark = replicaHighWatermark.get
      val deletable = ArrayBuffer.empty[LogSegment]
      // 从最老的Segment开始往后遍历
      var segmentEntry = segments.firstEntry
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        // nextSegment 为下一个Segment,用于循环控制及获取upperBoundOffset
        // upperBoundOffset 当前Segment最大偏移量
        // isLastSegmentAndEmpty 是否是最后一个Segment
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
          (null, logEndOffset, segment.size == 0)

        // 如果本segment最大偏移量 <= highWatermark 且在偏函数中返回true 且不是最后一个Segment， 则加入可删除队列中
        // 
        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          segmentEntry = null
        }
      }
      deletable
    }
  }

  // 将可删除Segment队列中的Segment进行逐一清理
  private def deleteSegments(deletable: Iterable[LogSegment]): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // Kafka 保证至少有一个Segment进行活动，因此如果清理的数目与现有segment数目相同，则通过Roll()方法生成一个新的Segment
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          // 逐个删除
          // 会调用Log.asyncDeleteSegment()方法中删除过程通过线程池并设置delay时间通过异步删除的方式移除Segment
          // 而延迟删除具体执行过程调用Segment.deleteIfExists()从文件系统中依次删除Segment对应的log,offsetIndex, timeIndex, TxnIndex等文件
          deletable.foreach(deleteSegment)

          //如果剩余的Segment中第一个Segment的baseOffset > 现在log的StartOffset,则更新Log的StartOffset及相关的缓存信息
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset)
        }
      }
      numToDelete
    }
  }
```

　　我们可以看到日志清理部分的代码比较简单。具体过程就是定期根据最大保留时间，最大保留大小，LogStartOffset清理策略进行清理旧的Segment。当然在具体操作过程中还需要注意不能清理那些高于HWM的日志，因为这样会造成LogStartOffset大于HWM，从而引发系统问题。

## <a id="FlushDirtyLogs">FlushDirtyLogs</a>

　　Kafka日志管理功能中另一个后台定时任务是将内存中的消息刷到磁盘中进行持久化。持久化过程需要考虑用户可能正在在同一台Broker上迁移日志，因此连同FutureLog一并Flush到磁盘。

```scala
  //LogManager.scala
  // 基于超时时间的刷数据到日志文件
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    // futureLogs是用户将副本在同一台Broker上从某个文件夹迁移到另一个文件夹下时被创建，当它赶上currentLogs后会替换掉currentLogs
    // 这里清理过程中一并清理
    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        // 距离上一次刷数据时间超过了配置的刷新间隔则将log中未持久化的数据刷到磁盘
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        // handle exception code ...
      }
    }
  }
```
    
　　距离上一次刷数据时间超过了配置的刷新间隔则将log中未持久化的数据刷到磁盘,通过调用Log.flush()方法，传入期望的最大刷入的偏移量进行持久化工作。

```scala
  // Log.scala
  def flush(): Unit = flush(this.logEndOffset)

  // 传入的Offset参数标识希望这次刷数据的最大偏移量(不包含)，并作为新的recoveryPoint
  def flush(offset: Long) : Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      // this.recoveryPoint 这个参数表示第一个为被刷到磁盘上的偏移量
      // 如果传入的Offset不大于这个值，刷入磁盘的最大offset大于这个预期的上线，所以什么也不做结束这个方法
      if (offset <= this.recoveryPoint)
        return
      // 在[this.recoveryPoint, offset]区间内的所有logSegment逐个持久化
      for (segment <- logSegments(this.recoveryPoint, offset))
        //Segment对应的log, offsetIndex, timeIndex, txnIndex分别进行flush()写入磁盘
        segment.flush()

      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        // 如果刷数据上限 > this.recoveryPoint, 则更新恢复点及更新lastFlushedTime
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset
          lastFlushedTime.set(time.milliseconds)
        }
      }
    }
  }
```

　　这个部分代码就更简单点，只是根据超时时间进行刷入数据。在刷入数据期间维护恢复点信息，标识最后一条刷入磁盘的消息的偏移量。如果该恢复点已经大于期望刷入磁盘消息的偏移量则什么也不做，否则获取符合范围的Segment列表逐个进行持久化，持久化过程依次是Segment对应的log, offsetIndex, timeIndex, txnIndex的持久化。

## <a id="conclusion">总结</a>

　　本篇从KafkaServer的启动讲起，介绍了LogManager的初始化过程，以及介绍了LogManager具体后台定时任务中的日志清理、内存数据持久化两个部分。代码过程比较简单，下一篇我们会将剩下的几个定时任务介绍完毕。
