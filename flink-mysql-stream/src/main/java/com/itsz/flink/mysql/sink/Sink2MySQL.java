package com.itsz.flink.mysql.sink;


import com.itsz.flink.mysql.domain.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Sink2MySQL extends RichSinkFunction<Student> {

    private Connection connection;

    private PreparedStatement preparedStatement;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student(name, password, age) values( ?, ?, ?);";
        preparedStatement = this.connection.prepareStatement(sql);
    }

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://47.106.198.217:3306/flink?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (preparedStatement != null) {
            preparedStatement.close();
        }
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
       // preparedStatement.setInt(1, value.getId());
        preparedStatement.setString(1, value.getName());
        preparedStatement.setString(2, value.getPassword());
        preparedStatement.setInt(3, value.getAge());
        preparedStatement.executeUpdate();

    }
}
