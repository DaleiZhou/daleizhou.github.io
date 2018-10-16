---
layout: post
category: [Kafka, Source Code]
title: Kafka Log Management(二)
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　本文接着[Kafka Log Management(一)](https://daleizhou.github.io/posts/kafka-log-management-1.html)讲解LogManager剩下的三个日志管理的后台定时任务。
<!--more-->

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

　　删除旧的SnapShot过程中，因为Segment文件命名的实现，因此可以根据文件名直接取offset来判断是否需要删除。

## <a id="CheckpointLogStartOffsets">CheckpointLogStartOffsets</a>

　　LogManager中的另一个后台定时任务为checkpointLogStartOffsets。用于将各个TopicPartition的StartOffset写入到文本文件建立checkpoint，避免暴露被DeleteRecordsRequest请求删除的数据。下面看具体实现过程:

```scala
  // LogManager.scala
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
        // 对文件夹下的log逐个判断，筛选出log的logStartOffset > log现有的第一个Segment的baseOffset
        // 将所有筛选出来的(tp，offset)写入checkpoint文件
        val logStartOffsets = partitionToLog.filter { case (_, log) =>
          log.logStartOffset > log.logSegments.head.baseOffset
        }.mapValues(_.logStartOffset)
        // Topic对应的startoffset信息写入检查点文件
        // 文件为log-start-offset-checkpoint
        // checkpoint file format:
        // line1 : version
        // line2 : expectedSize
        // nlines: (tp, startoffset) 
        checkpoint.write(logStartOffsets)
      } catch {
        // 异常处理 ... 
      }
    }
  }
```

　　CheckPoint文件在系统启动加载日志时使用，避免了暴露那些被删除的数据。在筛选过程中将logStartOffset大于Segment的baseOffset取出来写入文本文件，这个部分的实现也很简单，没有复杂的过程。

## <a id="DeleteLogs">DeleteLogs</a>

　　在一些情况下，如Swap日志替换等情况下需要删除一些log，为了提升系统效率，Kafka内部并不同步删除文件，而是加上.delete这样的后缀和加入待删除的队列中，后台的定时任务会分批统一进行清理。当然并不是加入删除队列的数据立马就会被删除，每次调度时只删除那些标记为删除超过一定时间的日志。下面看具体实现过程。

```scala
  // LogManager.scala
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      // 下一次调度时间计算
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          // 取要删除的日志第一个被设置要删除的系统时间
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          // 被调用要删除的时间 + fileDeleteDelayMs - now()即为下一次调度时间间隔
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          // 如果没有log需要删除，则使用配置中的fileDeleteDelayMs进行调度下一次任务
          currentDefaultConfig.fileDeleteDelayMs
      }

      // 日志删除时间已经超过配置的fileDeleteDelayMs，进行日志删除
      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            // 删除日志对应的Segment，清理相关缓存等
            removedLog.delete()
          } catch {
            // 异常处理 ...
          }
        }
      }
    } catch {
      // 异常处理 ...
    } finally {
      try {
        // 每次任务完成时，根据nextDelayMs动态设定下一次执行后台任务执行的时间
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        // 异常处理 ... 
          }
      }
    }
  }
```

## <a id="conclusion">总结</a>

　　本文继[Kafka Log Management(一)](https://daleizhou.github.io/posts/kafka-log-management-1.html)介绍了剩下的三个定时任务，分别为CheckpointLogRecoveryOffsets,CheckpointLogStartOffsets,DeleteLogs。该三个定时任务过程都比较简单，唯一特殊点的就是DeleteLogs下一次的调度时间是动态的。

　　至此，LogManager的启动过程及五个后台定时任务的具体实现也介绍完毕了。

