package com.itsz.flink.netty.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NettySource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> source = executionEnvironment.addSource(new TcpReceiverSource("192.168.33.10",9000));

        SingleOutputStreamOperator<String> response = source.map(line -> "server says that: " + line);

        response.print();

        executionEnvironment.execute("netty sourcing");

    }
}
