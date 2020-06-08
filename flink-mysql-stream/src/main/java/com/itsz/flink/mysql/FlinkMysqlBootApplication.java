package com.itsz.flink.mysql;

import com.itsz.flink.mysql.source.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FlinkMysqlBootApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(FlinkMysqlBootApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).print();
        env.execute();
    }

}
