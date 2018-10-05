---
layout: post
category: RocketMQ
title: RocketMQ事务消息分析(一)
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　RocketMQ-4.3.0的一个重要更新是对事务型消息的支持[ISSUE-292](https://github.com/apache/rocketmq/issues/292).因为事务消息的一些基础设施在以前的版本里已经部分支持，这里不局限于这次更新的代码，而是直接给出一个全貌的解析。RocketMQ的网络模型，消息存储，index过程等其它细节暂时不详细展开，后面会有专门的篇幅来系统的介绍。这里直接只对事务消息的处理部分做简要的分析，来学习RocketMQ事务消息实现的原理思想和处理手法。


## <a id="Produce transaction message">0 Producer发送事务消息</a>
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

　　跟踪代码可以看到内部消息通过**TransactionalMessageBridge.parseHalfMessageInner()**进行处理。处理过程中保存原消息的Topic及queueId，并且重设消息的SystemFlag,Topic和QueueId。该处理的目的很明确，重置是为了无差别保存内部消息到CommitLog中，但不对原Topic的消费组可见。保存一些信息的目的是为了等commit/abort阶段取出half消息存储到原目的消息队列中。


```java
	// TransactionalMessageBridge.java

	public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner));
    }

    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
            String.valueOf(msgInner.getQueueId()));
        msgInner.setSysFlag(
            MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }
```

　　
## <a id="Check">Check</a>
### TODO: prepare消息发送成功但是久未commit/rollback

## <a id="references">References</a>

* http://rocketmq.apache.org/release_notes/release-notes-4.3.0/

* https://segmentfault.com/a/1190000011282426

* https://blog.csdn.net/u011381576/article/details/79367233