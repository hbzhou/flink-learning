package com.itsz.flink.mysql;

import com.itsz.flink.mysql.domain.Student;
import com.itsz.flink.mysql.source.SourceFromMySQL;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransMapFunction {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> dataStreamSource = environment.addSource(new SourceFromMySQL());

        dataStreamSource.map(student -> {
            Student studentNew = new Student();
            studentNew.setId(student.getId());
            studentNew.setAge(student.getAge() + 5);
            studentNew.setPassword(student.getPassword());
            studentNew.setName(student.getName());
            return studentNew;
        });

        dataStreamSource.print();

        environment.execute();
    }
}
