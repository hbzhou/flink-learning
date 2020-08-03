package com.itsz.flink.netty.source;

import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NettySink {

    private final static int port = 9923;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> fileSource = streamExecutionEnvironment.readTextFile("people.txt", CharsetUtil.UTF_8.name());

        fileSource.addSink(new TcpPublisherSink(port));

        streamExecutionEnvironment.execute("Netty Sink");


    }
}
