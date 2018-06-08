---
layout: post
category: Kafka
title: Kafka事务消息过程分析(三)
---

## 内容 
>Status: Draft

　　上篇介绍完KafkaProducer发送事务消息，但是这些消息对于Consumer是不可见的。只有事务提交完才是可见的。本篇介绍消息事务的commit/abort。