---
layout: post
category: Kafka
title: Kafka Consumer(二)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　本篇任然从KafkaConsumer.pollOnce()为切入点，学习一下consumer加入一个group之后消费具体的消息背后的实现细节，介绍包含客户端与服务端的代码细节。

## <a id="KafkaConsumer">KafkaConsumer</a>

　　客户端拉取消息的入口是KafkaConsumer.poll(),该方法在过期时间内轮询拉取数据，如果并每次都检查条件，看是否需要更新缓存信息。下面我们看具体的一次轮询拉取消息的具体过程:

```java
    // KafkaConsumer.java
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {

        // other code ...
        // 发送获取消息的请求，不重复发送pending状态的请求
        fetcher.sendFetches();

        client.poll(pollTimeout, nowMs, new PollCondition() {
            @Override
            public boolean shouldBlock() {
                // completedFetches队列不为空，即有后台线程完成了一次拉取获得了结果，这种情况下不阻塞
                return !fetcher.hasCompletedFetches();
            }
        });

        // other code ...
        
        return fetcher.fetchedRecords();
    }

```

　　这里将KafkaConsumer.pollOnce()中关于获取消息部分的代码抽出来。从代码上可以看到consumer通过fetcher.sendFetches()来拉取数据。该方法用于从客户端被分配的topicpartition中拉取数据，且保证如果一个tp对应的node同时只有一个拉取动作。

```java
    // Fetcher.java
    public int sendFetches() {
        // 创建fetch请求的数据准备，每个topicPartition对应node生成至多一个FetchRequestData，不生成请求的情况有：
        // node为null, 有pending中的请求，client不可用等
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            // 根据FetchRequestData构造FetchRequest，填充参数
            final FetchRequest.Builder request = FetchRequest.Builder
                    .forConsumer(this.maxWaitMs, this.minBytes, data.toSend())
                    .isolationLevel(isolationLevel)
                    .setMaxBytes(this.maxBytes)
                    .metadata(data.metadata())
                    .toForget(data.toForget());
            if (log.isDebugEnabled()) {
                log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
            }
            //放入unsent队列,后台线程会检测这个队列进行发送
            client.send(fetchTarget, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        // 返回成功时的回调
                        public void onSuccess(ClientResponse resp) {
                            FetchResponse response = (FetchResponse) resp.responseBody();
                            FetchSessionHandler handler = sessionHandlers.get(fetchTarget.id());
                            // 异常处理...

                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            // other code ...

                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                long fetchOffset = data.sessionPartitions().get(partition).fetchOffset;
                                FetchResponse.PartitionData fetchData = entry.getValue();

                                log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                        isolationLevel, fetchOffset, partition, fetchData);
                                // 取出partition, fetchOffset, fetchData放入completedFetches缓存中，用于后续的fetcher.fetchedRecords()方法的获取
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
                                        resp.requestHeader().apiVersion()));
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            FetchSessionHandler handler = sessionHandlers.get(fetchTarget.id());
                            if (handler != null) {
                                handler.handleError(e);
                            }
                        }
                    });
        }
        return fetchRequestMap.size();
    }

    // 每个分配了tp的node创建一个请求队列，跳过那些正在发送请求的node
    private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        Cluster cluster = metadata.fetch();
        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();
        for (TopicPartition partition : fetchablePartitions()) {
            Node node = cluster.leaderFor(partition);
            // exception handle ...

            // 如果该node有正在进行中的请求则跳过
            else if (client.hasPendingRequests(node)) {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            } else {
                // 该leader无in-fight请求，则新建一个请求
                FetchSessionHandler.Builder builder = fetchable.get(node);
                if (builder == null) {
                    FetchSessionHandler handler = sessionHandlers.get(node.id());
                    if (handler == null) {
                        handler = new FetchSessionHandler(logContext, node.id());
                        sessionHandlers.put(node.id(), handler);
                    }
                    builder = handler.newBuilder();
                    fetchable.put(node, builder);
                }

                long position = this.subscriptions.position(partition);
                // 填充tp, fetchoffset等信息用于fetch
                builder.add(partition, new FetchRequest.PartitionData(position, FetchRequest.INVALID_LOG_START_OFFSET,
                    this.fetchSize));
            }
        }
        Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
            //调用build()方法生成FetchRequestData
            reqs.put(entry.getKey(), entry.getValue().build());
        }
        return reqs;
    }
```

　　Fetcher.sendFetches()方法根据自己被分配的tp，为每个无in-fight的leader构造一个FetchRequest，并放入client的unsent队列。Fetcher，client运行机制与[Kafka事务消息过程分析(一)](https://daleizhou.github.io/posts/startup-of-Kafka.html)中描述的Sender,client运行机制相似，都是后台线程轮询发送，这里具体的线程工作过程不再赘述。

## <a id="KafkaApis">KafkaApis</a>

　　Kafka处理FetchRequest的入口在KafkaApis.handle()方法中。其实Kafka的主从同步也是通过FetchRequest来完成，与consumer拉取消息的过程相似，下面我们看具体的FetchRequest处理过程:

```scala
  //KafkaApis.scala 
  def handle(request: RequestChannel.Request) {
        case ApiKeys.FETCH => handleFetchRequest(request)
  }

  def handleFetchRequest(request: RequestChannel.Request) {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val fetchContext = fetchManager.newContext(fetchRequest.metadata(),
          fetchRequest.fetchData(),
          fetchRequest.toForget(),
          fetchRequest.isFromFollower())

    val erroneous = mutable.ArrayBuffer[(TopicPartition, FetchResponse.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower()) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        fetchContext.foreachPartition((topicPartition, data) => {
          if (!metadataCache.contains(topicPartition)) {
            erroneous += topicPartition -> new FetchResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
              FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
              FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
          } else {
            interesting += (topicPartition -> data)
          }
        })
      } else {
        fetchContext.foreachPartition((part, data) => {
          erroneous += part -> new FetchResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
            FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
        })
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      fetchContext.foreachPartition((topicPartition, data) => {
        if (!authorize(request.session, Read, new Resource(Topic, topicPartition.topic)))
          erroneous += topicPartition -> new FetchResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED,
            FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
        else if (!metadataCache.contains(topicPartition))
          erroneous += topicPartition -> new FetchResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION,
            FetchResponse.INVALID_HIGHWATERMARK, FetchResponse.INVALID_LAST_STABLE_OFFSET,
            FetchResponse.INVALID_LOG_START_OFFSET, null, MemoryRecords.EMPTY)
        else
          interesting += (topicPartition -> data)
      })
    }
```

## <a id="conclusion">总结</a>

## <a id="references">References</a>

#### TBD