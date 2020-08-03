package com.itsz.flink.kafka.boot;

import com.itsz.flink.kafka.boot.config.KafkaFastJsonDeserializer;
import com.itsz.flink.kafka.boot.rtq.RtqCSClient;
import com.itsz.flink.kafka.boot.rtq.RtqTopic;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Properties;

@SpringBootApplication
public class FlinkKafkaBootApplication implements CommandLineRunner {

    @Autowired
    private RtqCSClient rtqCSClient;

    public static void main(String[] args) {
        SpringApplication.run(FlinkKafkaBootApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        rtqCSClient.startup();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");


        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(RtqTopic.RTQ_STREAM_TOPIC, new SimpleStringSchema(), properties);

        DataStreamSource<String> dataStreamSource = env.addSource(myConsumer).setParallelism(1);


        dataStreamSource.print(); //把从 kafka 读取到的数据打印在控制台

        env.execute("Flink add data source");


    }
}
