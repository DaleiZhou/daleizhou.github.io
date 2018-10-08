---
layout: post
category: RocketMQ
title: RocketMQ存储实现分析
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　赵家庄王铁柱曾经教过我他看源码的技巧，那就是他习惯先抓住底层的数据结构，然后逐步拓展到围绕数据而展开的一系列系统行为实现过程(他个人习惯)。这篇我们从RocketMQ最基础的存储部分切入，解析一下RocketMQ数据存储结构及索引等实现细节，在熟悉底层存储结构的基础上，我们可以很自然的领会RocketMQ中一部分设计理念。

　　RocketMQ中的消息最终会存储到名为`CommitLog`中，与Kafka中的无限长Log相似，都是只会通过往末尾追加的方式进行写入的Log，读取阶段根据offset检索到具体消息进行读取，Kafka Log的具体设计细节可以参考[Kafka log的读写分析[1]](https://daleizhou.github.io/posts/log-fetch-produce.html)。通过对比我们发现区别也是很明显的，在Kafka中的Log于具体的TopicPartition一一对应，而RocketMQ的CommitLog则在每个Broker上只有一个，这样的结构如何通过Topic为单位去消费呢，带着这样的问题我们具体看CommitLog的设计与实现细节。

## <a id="CommitLog">CommitLog</a>



## <a id="references">References</a>

*https://daleizhou.github.io/posts/log-fetch-produce.html