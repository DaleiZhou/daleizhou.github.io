---
layout: post
category: [RocketMQ, Source Code]
title: RocketMQ网络通信分析
excerpt_separator: <!--more-->
---

## 内容 
>Status: Draft

  代码版本: 4.3.0

　　看过RocketMQ源码的都知道RocketMQ的网络请求是通过Netty进行实现的，之前一直关注RocketMQ本身的功能，并没涉及介绍Netty。Netty是一个开源的提供异步的事件驱动的编程框架，使用者可以快速的在上面开发高性能的网络服务。这篇我们来介绍RocketMQ是如何使用Netty提供一套远程调用框架进行通信的。因为笔者水平的问题，这里只讨论RocketMQ是如何用Netty的，而不涉及Netty实现细节。
<!--more-->
## <a id="RocketMQ">RocketMQ</a>

　　如果抽象的更粗一点，RocketMQ的主体可以分成3个部分：NameServer, Broker, Client。具体抽象如下：

<div align="center">
<img src="/assets/img/2018/10/16/rocketmq-arch.png" width="50%" height="50%"/>
</div>

　　其中Nameserver用于管理meta信息，主要包括Topic,Broker等管理；Broker主要用于消息的管理，存储及消费等。图中Client与Broker之间的关联关系画的是虚线，主要是客户端启动时只配置RocketMQ集群的NameServer地址列表，Broker，Topic信息都是从NameServer中获取得到，得到这些信息后Client就可以直接连接Broker对目标消息队列进行生产或者消费。服务端的NameServer与Broker在底层网络来看没有什么差别，为了方便这里直接只说Broker。下面我们通过Broker来看RocketMQ是如何搭建在Netty对外提供服务的。

　　Broker的启动的main函数在org.apache.rocketmq.broker.BrokerStartup类中，里面做了两件事：初始化BrokerController实例，并调用BrokerController的start()方法启动Broker。而创建BrokerController过程中会调用BrokerController.initialize()方法进行初始化。在该方法中实例化了NettyRemotingServer两个实例分别命名为remotingServer，fastRemotingServer。这两个实例的功能相同，这里体现的区别就是绑定的端口相差2，fastRemotingServer绑定的端口也就是RocketMQ里的vip通道，提供vip服务。

```java
    // BrokerController.java
    public boolean initialize() throws CloneNotSupportedException {
        // other code ...
        if (result) {
            this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
            NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
            // vip通道
            fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
            this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);
        }
        // other code ...
    }
```

## <a id="NettyRemotingServer">NettyRemotingServer</a>

　　这里引出今天的正文，开始介绍今天的主角**NettyRemotingServer**。NettyRemotingServer继承自NettyRemotingAbstract，并实现了RemotingServer的接口。下面是NettyRemotingServer的构造函数。

```java
    // NettyRemotingServer.java
    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
        final ChannelEventListener channelEventListener) {
        // OnewaySemaphoreValue默认 256
        // AsyncSemaphoreValue 默认64
        // 支持的OneWay,Async最大并发数，控制系统内存占用
        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
        // ServerBootstrap即Netty的启动类
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        // 主要用于回调方法处理
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            // more code ...
        });

        // bossGroup用于处理Accept请求
        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            // more code ...
        });

        // workGroup用于处理连接的Read/Write事件，这里如果系统支持epoll，则实例化为EpollEventLoopGroup
        if (useEpoll()) {
            this.eventLoopGroupSelector = new EpollEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                // more code ...
            });
        } else {
            this.eventLoopGroupSelector = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                // more code ...
            });
        }

        loadSslContext();
    }
```

　　在NettyRemotingServer构造方法中，初始化了NettyRemotingAbstract,设置并发参数等，并实例化了几个EventLoop用于Netty处理客户端连接或读写事件的请求。当服务端要监听本地端口，则将NioServerSocketChannel注册到BossGroupEventLoop来处理Accept请求时，而已经Accept的SocketChannel会被注册到EventLoopGroupSelector用于处理读写事件。BrokerController.start()被调用时会调用NettyRemotingServer.start()方法，用于启动NettyRemotingServer。下面看start()方法具体细节。

```java
    // NettyRemotingServer.java
    public void start() {
        // 处理耗时任务的线程池
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            nettyServerConfig.getServerWorkerThreads(),
            new ThreadFactory() {
                // more code ...
            });

        ServerBootstrap childHandler =
            this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupSelector)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                .childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(defaultEventExecutorGroup, HANDSHAKE_HANDLER_NAME,
                                new HandshakeHandler(TlsSystemConfig.tlsMode))
                            .addLast(defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),
                                new NettyConnectManageHandler(),
                                new NettyServerHandler()
                            );
                    }
                });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            //Netty里是异步的世界，通过Feature同步等待bind结果
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        if (this.channelEventListener != null) {
            this.nettyEventExecutor.start();
        }

        // 定时任务扫描清理过期的请求
        this.timer.scheduleAtFixedRate(new TimerTask() {
            // more code...
        }, 1000 * 3, 1000);
    }
```

　　这段方法是使用Netty的一般姿势。在配置好各种线程池之后进行bind端口，用于监听接受客户端连接请求，进而提供服务。这是使用Netty的基本套路，下面我们跟着请求从客户端到服务端返回到客户端看RocketMQ在Netty通信的具体实现。

## <a id="Client">Client</a>

　　我们从一个具体的例子看起，客户端通过MQClientAPIImpl.pullMessageSync去同步拉取消息调用如下，在默认实现的MQClientAPIImpl中会实例化有RemotingClient，负责具体的通信。

```java
    // MQClientAPIImpl.java
    private PullResult pullMessageSync(
        final String addr,
        final RemotingCommand request,
        final long timeoutMillis
    ) throws RemotingException, InterruptedException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response);
    }
```

　　RemotingClient提供了三个调用远程方法的入口：`invokeAsync`,`invokeSync`,`invokeOneway`，分别是。
```java
    // RemotingClient.java
    public interface RemotingClient extends RemotingService {

    void updateNameServerAddressList(final List<String> addrs);

    List<String> getNameServerAddressList();

    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
        final long timeoutMillis) throws InterruptedException, RemotingConnectException,
        RemotingSendRequestException, RemotingTimeoutException;

    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException,
        RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException,
        RemotingTimeoutException, RemotingSendRequestException;

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
        final ExecutorService executor);

    void setCallbackExecutor(final ExecutorService callbackExecutor);

    ExecutorService getCallbackExecutor();

    boolean isChannelWritable(final String addr);
}
```






## <a id="References">References</a>

