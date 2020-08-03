package com.itsz.flink.netty.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TcpReceiverSource extends RichParallelSourceFunction<String> {

    private TcpClient tcpClient;


    public TcpReceiverSource(String host, int port) {
        tcpClient = new TcpClient(host, port);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        tcpClient.setContext(sourceContext);
        tcpClient.start();
    }

    @Override
    public void cancel() {
        tcpClient.close();

    }
}
