---
layout: post
category: Kafka
title: Kafka Log Management(二)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　本文接着[Kafka Log Management(一)](https://daleizhou.github.io/posts/kafka-log-management-1.html)讲解LogManager剩下的三个日志管理的后台定时任务。

## <a id="CheckpointLogRecoveryOffsets">CheckpointLogRecoveryOffsets</a>

　　Kafka日志管理后台线程会定时调用checkpointLogRecoveryOffsets(),进行写入恢复检查点文件，避免系统下次启动时从整个日志恢复。下面看具体实现:

```scala
  // LogManager.scala
  // 定期将恢复点写入文本文件，系统启动时避免恢复整个log
  def checkpointLogRecoveryOffsets() {
    liveLogDirs.foreach(checkpointLogRecoveryOffsetsInDir)
  }

  // 将给定文件夹下的所有tp对应的恢复点写入对应的recovery-point-offset-checkpoint文件
  // 文件为recovery-point-offset-checkpoint
  // checkpoint file format:
  // line1 : version
  // line2 : expectedSize
  // nlines: (tp, offset) 
  private def checkpointLogRecoveryOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- recoveryPointCheckpoints.get(dir)
    } {
      try {
        checkpoint.write(partitionToLog.mapValues(_.recoveryPoint))

        // 写入恢复检查点后删除旧的snapshots
        allLogs.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
      } catch {
        // handle exception code ...
      }
    }
  }
```

　　为了应对从ActiveSegment进行的日志截断，另一种不太常发生的情况是需要从倒数第二个Segment进行日志截断，因此至少保留最后两个Segment。因为检查点定期会写入文件进行持久化，系统恢复可以直接从检查点开始载入log,因此可以删除检查点之前的Snapshot。这两种情况取一个较小的值，作为snapshot删除的截止点。

```scala
  //  清理旧的shapshot
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    val minOffsetToRetain = minSnapshotsOffsetToRetain

    // 根据较小的offset, 删除 .snapshot 后缀的snapshot文件，加速系统重启载入速度
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)
    minOffsetToRetain
  }

  private[log] def minSnapshotsOffsetToRetain: Long = {
    lock synchronized {
      // 保留最后两个Segment,如果只有最后一个activeSegment那就只能取最后一个Segment的baseOffset
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset
      // recoveryPointSegment的baseOffset,如果不存在，则直接取recoveryPoint
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }
```

　　删除旧的SnapShot过程中，有个小的trick是根据文件名直接取offset来判断是否需要删除。

## <a id="CheckpointLogStartOffsets">CheckpointLogStartOffsets</a>

```scala
  // LogManager.scala
  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  def checkpointLogStartOffsets() {
    liveLogDirs.foreach(checkpointLogStartOffsetsInDir)
  }

  /**
   * Checkpoint log start offset for all logs in provided directory.
   */
  private def checkpointLogStartOffsetsInDir(dir: File): Unit = {
    for {
      partitionToLog <- logsByDir.get(dir.getAbsolutePath)
      checkpoint <- logStartOffsetCheckpoints.get(dir)
    } {
      try {
        val logStartOffsets = partitionToLog.filter { case (_, log) =>
          log.logStartOffset > log.logSegments.head.baseOffset
        }.mapValues(_.logStartOffset)
        checkpoint.write(logStartOffsets)
      } catch {
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while writing to logStartOffset file in directory $dir", e)
      }
    }
  }
```

## <a id="DeleteLogs">DeleteLogs</a>

```scala
  // LogManager.scala
  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          currentDefaultConfig.fileDeleteDelayMs
      }

      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.dir.getParent}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }
```

## <a id="conclusion">总结</a>

