---
layout: post
category: RocketMQ
title: RocketMQ事务消息分析(一)
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　RocketMQ-4.3.0的一个重要更新是对事务型消息的支持[ISSUE-292](https://github.com/apache/rocketmq/issues/292).因为事务消息的一些基础设施在以前的版本里已经部分支持，这里不局限于这次更新的代码，而是直接给出一个全貌的解析。


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

        // 2. 给消息设置Half属性
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


# TODO

## <a id="references">References</a>

* http://rocketmq.apache.org/release_notes/release-notes-4.3.0/

* https://segmentfault.com/a/1190000011282426

* https://blog.csdn.net/u011381576/article/details/79367233