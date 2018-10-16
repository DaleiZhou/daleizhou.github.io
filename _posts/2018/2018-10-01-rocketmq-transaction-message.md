---
layout: post
category: [RocketMQ, Source Code]
title: RocketMQ事务消息分析
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　RocketMQ-4.3.0的一个重要更新是对事务型消息的支持[ISSUE-292](https://github.com/apache/rocketmq/issues/292).因为事务消息的一些基础设施在以前的版本里已经部分支持，这里不局限于这次更新的代码，而是直接给出一个全貌的解析。RocketMQ的网络模型，消息存储，index过程等其它细节暂时不详细展开，后面会有专门的篇幅来系统的介绍。这里直接只对事务消息的处理部分做简要的分析，来学习RocketMQ事务消息实现的原理思想和处理手法。
<!--more-->

## <a id="Produce transaction message">Producer发送事务消息</a>
　　我们从example包提供的例子看起, 如下面的代码片段:

```java
	public class TransactionProducer {
	    public static void main(String[] args) throws MQClientException, InterruptedException {
	        TransactionListener transactionListener = new TransactionListenerImpl();
	        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
	        producer.setTransactionListener(transactionListener);
	        producer.start();
	        // other code ...

	        // 发送事务消息
	        SendResult sendResult = producer.sendMessageInTransaction(msg, null);

	        // other code ...
	        producer.shutdown();
	    }
	}
```

　　我们可以看到与发送常规消息的差异是调用的Producer类型为**TransactionMQProducer**, 是继承自**DefaultMQProducer**, 可以传入**TransactionListener**的实现。**TransactionListener**接口定义了两个方法，分别用于发送Half消息成功之后的调用，和收到来自broker的check消息时调用。具体功能结合后面的源码分析逐步解释。在发送事务消息的**sendMessageInTransaction()**方法中，首先检查是否设置了**TransactionMQProducer**，接着调用**DefaultMQProducerImpl.sendMessageInTransaction()**进行发送与对发送结果的进一步操作。


```java
	// DefaultMQProducerImpl.java
	public TransactionSendResult sendMessageInTransaction(final Message msg,
                                                          final TransactionListener tranExecuter, final Object arg)
        throws MQClientException {
        // 1. 是否设置TransactionListener及消息合法性验证

        SendResult sendResult = null;

        // 2. 给消息设置Half属性, 用于后续broker接收到消息判断是否是事务消息的prepare
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
        	// 3. 发送
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;

        // 4. 检查发送结果
        switch (sendResult.getSendStatus()) {
        	// 4.1 发送成功
            case SEND_OK: {
                try {
                	// 4.1.1 从发送Prepare消息结果中获取transactionId并设置本地msg的属性和成员变量
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }

                    // 4.1.2 执行预设的TransactionListener.executeLocalTransaction逻辑，用于获取是否commit/rollback已发送的prepare消息
                    localTransactionState = tranExecuter.executeLocalTransaction(msg, arg);
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;
            // 4.2 发送异常都进行rollback
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        try {
        	// 5. 根据localTransactionState不同，发送EndTransactionRequestHeader进行事务的第二阶段
        	// commit/rollback之前发送的prepare消息
            this.endTransaction(sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        // 6. 返回TransactionSendResult结果给调用者
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }
```

　　从发送方法来看，可以清晰的看到RocketMQ发送事务消息的基本步骤。将一个事务消息的提交分为两阶段，在第一阶段发送消息成功后，获取到事务消息id号，并且调用用户传入的**TransactionListener**用于决定对消息的commit/abort。在实际实践过程中，用户可以在**TransactionListener**里实现更复杂的业务逻辑，例如修改数据库操作，在然后返回commit/rollback等结果用于对prepare消息的最后处理。

　　我们注意到代码里不停的出现TransactionId这个属性，在RocketMQ里用于唯一标识一个事务消息。按照通常的想法，在分布式系统中唯一标识一个事务消息基本需要唯一不重复。并且在实现过程中我们往往还需要它在单台机器上是有序递增的。这样的ID生成方法中其中比较常用的是[SnowFlake[2]](https://segmentfault.com/a/1190000011282426),美团点评在其基础上开发了[Leaf系统[3]](https://blog.csdn.net/u011381576/article/details/79367233)用于满足其业务上id生成。RocketMQ里也采用类似的处理手法进行transactionId的生成。

```java
	// MessageClientIDSetter.java

	public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
        }
    }

	public static String createUniqID() {
        StringBuilder sb = new StringBuilder(LEN * 2);

        // 根据本机的Pid,IP等信息生成的FIX_STRING
        sb.append(FIX_STRING);
        // 根据时间diff, count累加进行设置递增
        sb.append(UtilAll.bytes2string(createUniqIDBuffer()));
        return sb.toString();
    }
```

## <a id="PutMessage">Broker PutMessage</a>

　　接下来我们看Broker端对事务消息的处理过程。Broker端在对事务消息请求的网络处理流程上与其他消息处理过程并无区别。从**SendMEssageProcessor.sendMessage()**开始有差异的处理。RocketMQ处理**produce**请求的基本套路是将客户端请求包装成内部消息，并根据requestHeader设置各种成员变量的值, 内部消息最后被处理存储到CommitLog中。


```java
	// SendMEssageProcessor.java
	private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand request,
                                        final SendMessageContext sendMessageContext,
                                        final SendMessageRequestHeader requestHeader) throws RemotingCommandException {
		// other code ...

		// 根据请求数据，生成内部消息用于后续存储在CommitLog中
		MessageExtBrokerInner msgInner = new MessageExtBrokerInner();

		// handleRetryAndDLQ， 其中特别重要的是在这个方法中会设置msgInner的SystemFlag
		// SystemFlag由客户端放在RequestHeader中传过来，在事务消息发送存储过程中用于区分消息类型
		// prepare阶段客户端设置成 MessageSysFlag.TRANSACTION_PREPARED_TYPE
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return response;
        }
		
		//客户端发送事务消息时设置PROPERTY_TRANSACTION_PREPARED为true
		String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (traFlag != null && Boolean.parseBoolean(traFlag)) {
        	// broker端配置rejectTransactionMessage用于是否接受事务消息
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                    "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                        + "] sending transaction message is forbidden");
                return response;
            }

            // 处理事务消息
            putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
        } else {
            putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        }

        return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);
    }

```

　　**SendMEssageProcessor.sendMessage()**处理过程中会调用私有方法handleRetryAndDLQ()来进行消息重试的相关处理，该方法中会将RequestHeader中传过来的SystemFlag的值设置到内部消息中。在发送事务消息这个场景下，该标识的值会被设置成**MessageSysFlag.TRANSACTION_PREPARED_TYPE**。客户端发送事务消息是有个附加的属性**MessageConst.PROPERTY_TRANSACTION_PREPARED**会被设置为true,因此会走第一个**if**分支。在该分支中我们可以看到**broker**可以配置是否接受事务消息。假如**Broker**端可以处理事务消息，则通过Broker启动时设置的TransactionalMessageService进行事务消息的处理。

　　跟踪代码可以看到内部消息通过**TransactionalMessageBridge.parseHalfMessageInner()**进行处理。处理过程中保存原消息的Topic及queueId，并且重设消息的SystemFlag,Topic和QueueId。该处理的目的很明确，重置是为了无差别保存内部消息到CommitLog中，但不对原Topic的消费组可见。保存一些信息的目的是为了等commit/abort阶段取出half消息存储到原目的消息队列中。**parseHalfMessageInner()**方法处理完毕后通过调用store.putMessage()进行保存half消息。

```java
	// TransactionalMessageBridge.java

	public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner));
    }

    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
            String.valueOf(msgInner.getQueueId()));
        // 重置事务标识，后续的IndexService和
        msgInner.setSysFlag(
            MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        // 设置内部消息的Topic为MixAll.RMQ_SYS_TRANS_HALF_TOPIC对应的值
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }
```

　　RocketMQ在设计上讲所有消息都存储在CommitLog中,在Broker启动时启动**CommitLogDispatcherBuildIndex**和**CommitLogDispatcherBuildConsumeQueue**, 分别用于在后台不断地将CommitLog中新加入的消息进行Index和更新不同Topic的队列的ConsumerQueue。具体细节我们将会在专门分析RocketMQ的存储部分来描述。这里我们只用知道在前述的两个**CommitLogDispatcher**的**dispatch()**方法中，因为之前描述的内部消息的SysFlag被重置为**TRANSACTION_NOT_TYPE**的缘故，不仅后台线程会进行BuildIndex，而且会更新Half消息对应的MessageQueue的内容。

## <a id="EndTransaction">EndTransaction</a>

　　Prepare阶段发送的消息成功被转化为Half消息存储在Broker上之后给客户端返回操作成功结果。如第一节描述，客户端在接收到成功的结果后，执行预设的TransactionListener.executeLocalTransaction逻辑，用于获取是否commit/rollback已发送的prepare消息。根据localTransactionState不同，发送EndTransactionRequestHeader进行事务的第二阶段。

```java
// DefaultMQProducerImpl.java
public void endTransaction(
        final SendResult sendResult,
        final LocalTransactionState localTransactionState,
        final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        final MessageId id;
        // 从发送的结果中获取消息在CommitLog中断额Offset
        // 该offset后续会回传给Borker，让Borker知道去处理哪一个half消息
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        String transactionId = sendResult.getTransactionId();
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTransactionId(transactionId);
        // Broker会根据该Offset在Commit/abort阶段取出half消息进行处理
        requestHeader.setCommitLogOffset(id.getOffset());
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        // 通过OneWay的方式发送EndTransactionRequest
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
            this.defaultMQProducer.getSendMsgTimeout());
    }
```

　　**DefaultMQProducerImpl.endTransaction()**方法里从Broker返回的结果中取出消息Offset,transactionId，连同**localTransactionState**来生成**EndTransactionRequestHeader**，用于Commit/abort本次提交的事务消息。Offset为第一阶段存入的Half消息在CommitLog中的Offset,第二阶段Borker会通过该Offset取出half消息进行处理。这里请求使用OneWay方式进行提交而不关心结果，即便是请求没有得到正确处理，后续也会有Check机制来补偿。

　　下面来看**Broker**接收到**EndTransactionRequest**后的处理。在Commit/abort的处理过程基本相同，大致过程是调用**TransactionalMessageService**进行commit/rollback**消息。虽然调用的方法不同，但是都是根据客户端传入的Offset取出之前存入的Half消息，如果是Commit类型则会将取出的消息进行一些成员设置用于存入实际的Topic对应的实际消息队列中。最后都进将Half消息从Half消息队列中删除，操作成功之后给客户端返回结果，不过因为客户端是通过**Oneway**方式调用，因此结果返回显得可能就不那么重要。当然这里的删除实际并不会执行删除操作，而是一种说法，具体实现如下。

```java
// EndTransactionProcessor.java
public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws
        RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader requestHeader =
            (EndTransactionRequestHeader)request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.info("Transaction request:{}", requestHeader);
        // Slave不处理EndTransactionRequest
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }


        OperationResult result = new OperationResult();
        // 处理TRANSACTION_COMMIT_TYPE类型的EndTransactionRequest
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                	// 根据取出的Half消息生成内部消息，设置目标Topic,queueId，保存到目标消息队列中
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                    
                    // 通过MessageStore().putMessage()将消息存入目标Topic的目标消息队列中
                    RemotingCommand sendResult = sendFinalMessage(msgInner);
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {
                    	// 操作成功删除half消息
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }
        } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
        	// 处理TRANSACTION_ROLLBACK_TYPE类型的EndTransactionRequest
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                	// 操作成功删除half消息
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;
    }
```

　　**Kafka**到**RocketMQ**吞吐量很大的一个重要原因是设计上将**Producer**产生的消息以追加的形式写在**Log**文件的末尾，这样简化写的过程，提高写的效率。上面的代码片段中有调用**deletePrepareMessage()**方法，目的是删除已经没用的Half消息，根据**RocketMQ**的设计，我们应该猜到不会从Log文件里直接删除过期的消息。这里我们详细看RocketMQ是具体怎么做的。

```java
// TransactionalMessageServiceImpl.java
public boolean deletePrepareMessage(MessageExt msgExt) {
	    // 删除消息其实是保存一个删除的Op消息
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }
```

　　从**TransactionalMessageServiceImpl.deletePrepareMessage()**方法中可以看到RocketMQ所谓的删除Half消息的操作的实现是写入一个Op消息实现的。具体实现看**TransactionalMessageBridge.putOpMessage()**的代码片段。

```java
// TransactionalMessageBridge.java
public boolean putOpMessage(MessageExt messageExt, String opType) {
	// 生成一个MessageQueue，该MessageQueue重写了hashCode,equals方法，如果成员变量值相同，则会被认为是相等的关系。
        MessageQueue messageQueue = new MessageQueue(messageExt.getTopic(),
            this.brokerController.getBrokerConfig().getBrokerName(), messageExt.getQueueId());
        if (TransactionalMessageUtil.REMOVETAG.equals(opType)) {
            return addRemoveTagInTransactionOp(messageExt, messageQueue);
        }
        return true;
    }

    private boolean addRemoveTagInTransactionOp(MessageExt messageExt, MessageQueue messageQueue) {
        // 这里要看到opMessage的body为原来halfMessage的offset
        Message message = new Message(TransactionalMessageUtil.buildOpTopic(), TransactionalMessageUtil.REMOVETAG,
            String.valueOf(messageExt.getQueueOffset()).getBytes(TransactionalMessageUtil.charset));
        writeOp(message, messageQueue);
        return true;
    }

    private void writeOp(Message message, MessageQueue mq) {
        MessageQueue opQueue;
        if (opQueueMap.containsKey(mq)) {
            opQueue = opQueueMap.get(mq);
        } else {
            opQueue = getOpQueueByHalf(mq);
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }
        putMessage(makeOpMessageInner(message, opQueue));
    }
```

　　**TransactionalMessageBridge**里维护一个Map类型的缓存变量opMap，key为Topic+BrokerName+QueueId的组合，value为对应的OpQueue信息，同样也是记录了Topic+BrokerName+QueueId的组合。根据传入的Message信息及OpQueue的信息生成一个**OpMessageInner**，这个op消息的body部分记录着原half消息在queue里的offset,后面**check**部分会利用起来。这个消息通过**MessageStore.putMessage()**服务写入**CommitLog**。后续的如Index,ConsumerOffset更新等和普通消息过程一致。

## <a id="Check">Check</a>

　　删除过程写入Op消息有什么用，Half消息久未commit/abort的情况的处理都和本节有关。Broker在启动时初始化**TransactionalMessageCheckService**后台线程，在Broker角色为非**SLAVE**情况下调用此service的**start()**方法启动该后台线程。该后台线程用于消费Op队列及Half队列，做一些Check的工作。大致的作用就是消费Op/Half等内部消息队列，逐个取出分析处理，话不多说先上代码。

```java
public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.info("Check topic={}, queues={}", topic, msgQueues);
            // 每次check过程都遍历Half消息的所有消息队列
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                MessageQueue opQueue = getOpQueue(messageQueue);

                // 通过ConsumerOffsetManager().queryOffset()获得Group+Topic+queueId的消费offset
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                List<Long> doneOpOffset = new ArrayList<>();
                HashMap<Long, Long> removeMap = new HashMap<>();

                // fillOpRemoveMap从OpQueue里拉取op消息并逐个处理，
                // 对于那些offset小于half消息现在消费到的offset的放入doneOpOffset
                // 否则放到removeMap中，该mapkey为op消息对应的half消息的offset,value为该op消息在op队列中的offset
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;

                // 从当前half队列消费的offset开始逐个遍历
                long i = halfOffset;
                while (true) {
                	// 该队列处理的太久了切换下一个队列进行check， MAX_PROCESS_TIME_LIMIT = 1min
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) { 
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }

                    // 如果当前消息已经被标记为删除，则从removeMap里删除该offset,处理下一条消息，本条什么也不做
                    if (removeMap.containsKey(i)) {
                        log.info("Half offset {} has been committed/rolled back", i);
                        removeMap.remove(i);
                    } else {
                    	// 不在removeMap里，则需要进行check处理
                    	// a) 取出HalfMsg
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.info("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        // b) 检查是否需要丢弃或者需要跳过
                        // needDiscard每次都会修改消息的PROPERTY_TRANSACTION_CHECK_TIMES属性，用于记录被check次数
                        // check次数超过transactionCheckMax或者消息太旧，比Message最长保存时间还久 则都需要丢弃跳过不处理
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }

                        // msgExt写入时间大于startTime，则说明是本轮启动check后存入的消息，则中断对应队列的check,换下一个queue
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.info("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        long checkImmunityTime = transactionTimeout;
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            // 消息有PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS属性，且在checkImmunityTime时间内
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                            	// 如果该消息产生时间大于checkImmunityTime，则跳过本条消息，进行下一条消息处理
                            	// 如果还在checkImmunityTime时间内，则检查消息的PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET属性
                            	// 如果没有该属性，或者该属性对应的值不在removeMap里，说明还不能跳过本条消息，
                            	// 这时根据half消息重新生成一个消息append到half消息队尾，并跳过当前位置，即延后处理
                            	// 如果有该属性，则说明被renew过，如果removeMap包含该offset,则跳过本条消息
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTime)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                        	// 如果消息产生到现在的时间小于checkImmunityTime， 则当前队列check太早，切换到下一个队列
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.info("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                            || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                        	// 重新写入失败则重复本次循环，写入成功则向客端户发送check状态请求
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            // 发送CheckTransactionStateRequest给客户端用于check本条消息的状态
                            // 客户端收到请求后根据TransactionId,MsgId等信息提交EndTransaction请求，这样如果久未主动commit/abort的事务消息又可以进行第二阶段的提交
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                messageQueue, pullResult);
                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                // 如果half的消费offset有更新则提交新的offset
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                // op队列消费进度更新为最后一个连续的offset
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Check error", e);
        }
    }
```

　　如上述在**check()**方法中添加的说明注释描述的那样，check过程定时对每个Half消息队列依次处理。对于每个Half消息队列，从对应的op队列中拉取一些消息，逐个对这些消息进行处理。处理过程中维护removeMap,凡是在这个缓存中的都是确定要删除的数据，因此half队列中有吻合的消息则就跳过不进行处理。如果一个消息被检查过多次或消息太旧，旧到超过了RocketMQ保存消息最长时限，则也需要跳过消息。**Check**具体执行过程是向客户端发送一个CheckTransactionState请求，包含了消息id,事务id等信息。客户端根据这些信息决定提交EndTransaction的请求。这样因为一些原因Broker上没有正确收到EndTransaction请求的事务消息可以得到最终的commit/abort。

　　RocketMQ在逐个消息检查过程中并不阻塞等待。而是将那些后续需要检查的消息重新追加到消息队列的尾部，而正常的移动消费的offset。在每个队列检查过程中，处理持续的时间不能过长，如果过长则切换下一个队列进行处理。如果该队列处理的太早也需要让出时间，切换到别的队列。

## <a id="conclusion">Conclusion</a>

　　至此，RocketMQ事务消息的提交、Broker的处理及容错check部分都分析完毕。RocketMQ对事务处理的基本思路放入一个half消息队列中暂存起来，直到客户端发送commit请求才最终写到目标Topic对应的消息队列中。从Half消息队列中删除没用的消息也只是写入一个Op消息到Op消息队列中。利用后台check线程逐一对Half中的消息进行内部消费，如果是属于已经写入删除Op的Half消息则跳过，如果是那些悬而未决的消息则进行check处理，check处理的目的是Broker对没有正常收到End请求的事务消息的一种容错处理，向客户端发送check请求，通知客户端进行处理，如再次提交等。在实现过程中，RocketMQ还会将需要延迟处理的消息重新追加到Half消息队尾，这样会在未来的时间片中进行处理。

## <a id="references">References</a>

* http://rocketmq.apache.org/release_notes/release-notes-4.3.0/

* https://segmentfault.com/a/1190000011282426

* https://blog.csdn.net/u011381576/article/details/79367233