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


由上述的介绍我们对Kafka的log有了一个直观的印象，现在结合Kafka处理的fetch和produce请求最后日志的读写具体的代码细节来进行源码分析。

## <a id="Fetch">Fetch</a>




## TODO

## <a id="references">References</a>

* https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
* http://code.google.com/apis/protocolbuffers/docs/encoding.html





