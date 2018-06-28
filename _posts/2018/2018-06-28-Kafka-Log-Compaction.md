---
layout: post
category: Kafka
title: Kafka Log Compaction
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

# TODO: 尚未完结

　　LogManager的另一个重要组成部分为日志的压缩清理线程。在具体跟踪源码进行分析前，先来根据官方文档来理解一下日志的压缩清理，以便于有一个宏观的印象。

　　Kafka在设计上，新的数据是往顺序日志文件中添加来写入，只往日志末尾进行append操作，这种设计可以带来很好的写入性能，在写入时不需要进行查询操作。对于同一个key,后续如果客户端更新了它对应的value，Kakfa保留对应所有版本的旧的数据值，而不直接修改替换相同key对应的value。为了让客户端在某个时刻获取最新版本的值，Kafka在后台会有压缩线程，定期进行日志的压缩清理工作。跟着官方文档我们来看一下具体的工作过程。

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




## <a id="conclusion">总结</a>

## <a id="references">References</a>

* https://kafka.apache.org/documentation/#compaction
