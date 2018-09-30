---
layout: post
category: RocketMQ
title: RocketMQ事务消息分析(一)
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　RocketMQ-4.3.0的一个重要更新是对事务型消息的支持[ISSUE-292](https://github.com/apache/rocketmq/issues/292).因为事务消息的一些基础设施在以前的版本里已经部分支持，这里不局限于这次更新的代码，而是直接给出一个全貌的解析。

## <a id="Producer">基本调用</a>

　　从客户端角度而言，完整的事务使用一般有如下几个调用：




## <a id="references">References</a>

* http://rocketmq.apache.org/release_notes/release-notes-4.3.0/

* SnowFlake https://segmentfault.com/a/1190000011282426