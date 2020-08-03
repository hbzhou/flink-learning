package com.itsz.flink.netty.source;


import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class NettyClientHandler extends ChannelInboundHandlerAdapter {

    private  SourceFunction.SourceContext<String> sourceContext;

    public NettyClientHandler(SourceFunction.SourceContext<String> sourceContext) {
        this.sourceContext = sourceContext;
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        sourceContext.collect(byteBuf.toString(CharsetUtil.UTF_8));
        System.out.println("receive message from server:" + ctx.channel().remoteAddress() + " message: " + byteBuf.toString(CharsetUtil.UTF_8));
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(Unpooled.copiedBuffer("Hello Server", CharsetUtil.UTF_8));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
