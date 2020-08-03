package com.itsz.flink.netty.source;


import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;

public class NettyServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.out.println(Thread.currentThread().getName() + "handle client ");
        ByteBuf byteBuf = ((ByteBuf) msg);
        System.out.println("receive message from client:" + ctx.channel().remoteAddress() + "  msg:" + byteBuf.toString(CharsetUtil.UTF_8));

        ctx.channel().eventLoop().execute(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
            ctx.writeAndFlush(Unpooled.copiedBuffer("delay task", CharsetUtil.UTF_8));
        });

        ctx.channel().eventLoop().execute(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ctx.writeAndFlush(Unpooled.copiedBuffer("delay task 2", CharsetUtil.UTF_8));
        });

        ctx.channel().eventLoop().schedule(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }, 2, TimeUnit.SECONDS);
        System.out.println("read complete!!");

    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.writeAndFlush(Unpooled.copiedBuffer("Hello,Client", CharsetUtil.UTF_8));
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
