package com.itsz.flink.netty.source;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class TcpPublisherSink extends RichSinkFunction<String> {

    private final int port;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private Channel channel;

    public TcpPublisherSink(int port) {
        this.port = port;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            ChannelPipeline pipeline = socketChannel.pipeline();
                            pipeline.addLast(new NettyServerHandler());
                        }
                    });
            ChannelFuture channelFuture = bootstrap.bind(port);
            channelFuture.addListener(future -> {
                if (future.isSuccess()) {
                    System.out.println("绑定成功");
                } else {
                    System.out.println("绑定失败");
                }
            });
            channel = channelFuture.channel();
           // channel.closeFuture().syncUninterruptibly();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        channel.writeAndFlush(value);
        System.out.println(value);
    }
}
