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




## <a id="conclusion">总结</a>

## <a id="references">References</a>

*https://kafka.apache.org/documentation/#compaction
