package com.itsz.flink.mysql;

import com.alibaba.fastjson.JSON;
import com.itsz.flink.mysql.domain.Student;
import com.itsz.flink.mysql.sink.Sink2MySQL;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class FlinkKafkaConsumerMain {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.33.12:9092");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("student", new SimpleStringSchema(), properties);

        SingleOutputStreamOperator<Student> output = env.addSource(myConsumer).setParallelism(1).map(str -> JSON.parseObject(str, Student.class));

        output.addSink(new Sink2MySQL());

        env.execute("Flink add data source");


    }
}
