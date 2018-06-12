---
layout: post
category: Kafka
title: Kafka Consumer(二)
---

## 内容 
>Status: Draft

  代码版本: 2.0.0-SNAPSHOT

　　本篇任然从KafkaConsumer.pollOnce()为切入点，学习一下consumer加入一个group之后消费具体的消息背后的实现细节，介绍包含客户端与服务端的代码细节。

```java
    // KafkaConsumer.java
    private Map<TopicPartition, List<ConsumerRecord<K, V>>> pollOnce(long timeout) {
        // other code ...
        // 发送获取消息的请求，不重复发送pending状态的请求
        fetcher.sendFetches();

        client.poll(pollTimeout, nowMs, new PollCondition() {
            @Override
            public boolean shouldBlock() {
                // since a fetch might be completed by the background thread, we need this poll condition
                // to ensure that we do not block unnecessarily in poll()
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
        Map<Node, FetchSessionHandler.FetchRequestData> fetchRequestMap = prepareFetchRequests();
        for (Map.Entry<Node, FetchSessionHandler.FetchRequestData> entry : fetchRequestMap.entrySet()) {
            final Node fetchTarget = entry.getKey();
            final FetchSessionHandler.FetchRequestData data = entry.getValue();
            final FetchRequest.Builder request = FetchRequest.Builder
                    .forConsumer(this.maxWaitMs, this.minBytes, data.toSend())
                    .isolationLevel(isolationLevel)
                    .setMaxBytes(this.maxBytes)
                    .metadata(data.metadata())
                    .toForget(data.toForget());
            if (log.isDebugEnabled()) {
                log.debug("Sending {} {} to broker {}", isolationLevel, data.toString(), fetchTarget);
            }
            //放入unsent队列
            client.send(fetchTarget, request)
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        @Override
                        public void onSuccess(ClientResponse resp) {
                            FetchResponse response = (FetchResponse) resp.responseBody();
                            FetchSessionHandler handler = sessionHandlers.get(fetchTarget.id());
                            if (handler == null) {
                                log.error("Unable to find FetchSessionHandler for node {}. Ignoring fetch response.",
                                    fetchTarget.id());
                                return;
                            }
                            if (!handler.handleResponse(response)) {
                                return;
                            }

                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);

                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                long fetchOffset = data.sessionPartitions().get(partition).fetchOffset;
                                FetchResponse.PartitionData fetchData = entry.getValue();

                                log.debug("Fetch {} at offset {} for partition {} returned fetch data {}",
                                        isolationLevel, fetchOffset, partition, fetchData);
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

    private Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        Cluster cluster = metadata.fetch();
        Map<Node, FetchSessionHandler.Builder> fetchable = new LinkedHashMap<>();
        for (TopicPartition partition : fetchablePartitions()) {
            Node node = cluster.leaderFor(partition);
            if (node == null) {
                metadata.requestUpdate();
            } else if (client.isUnavailable(node)) {
                client.maybeThrowAuthFailure(node);

                // If we try to send during the reconnect blackout window, then the request is just
                // going to be failed anyway before being sent, so skip the send for now
                log.trace("Skipping fetch for partition {} because node {} is awaiting reconnect backoff", partition, node);
            } else if (client.hasPendingRequests(node)) {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
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
                builder.add(partition, new FetchRequest.PartitionData(position, FetchRequest.INVALID_LOG_START_OFFSET,
                    this.fetchSize));

                log.debug("Added {} fetch request for partition {} at offset {} to node {}", isolationLevel,
                    partition, position, node);
            }
        }
        Map<Node, FetchSessionHandler.FetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, FetchSessionHandler.Builder> entry : fetchable.entrySet()) {
            reqs.put(entry.getKey(), entry.getValue().build());
        }
        return reqs;
    }
```

## <a id="conclusion">总结</a>

## <a id="references">References</a>

#### TBD