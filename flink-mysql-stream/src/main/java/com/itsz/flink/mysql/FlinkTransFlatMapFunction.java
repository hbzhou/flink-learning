package com.itsz.flink.mysql;

import com.itsz.flink.mysql.domain.Student;
import com.itsz.flink.mysql.source.SourceFromMySQL;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkTransFlatMapFunction {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> dataStreamSource = environment.addSource(new SourceFromMySQL()).setParallelism(1);

        dataStreamSource.flatMap((FlatMapFunction<Student, Object>) (student, collector) -> {
            if (student.getId() % 2 == 0 ){
                collector.collect(student);
            }

        });

        dataStreamSource.filter(student -> student.getId() > 56);

        dataStreamSource.print();

        environment.execute();
    }
}
