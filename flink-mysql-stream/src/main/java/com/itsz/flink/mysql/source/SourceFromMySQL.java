package com.itsz.flink.mysql.source;

import com.itsz.flink.mysql.domain.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class SourceFromMySQL extends RichSourceFunction<Student> {

    private PreparedStatement preparedStatement;

    private Connection connection;


    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            Student student = new Student();
            student.setId(resultSet.getInt("id"));
            student.setName(resultSet.getString("name"));
            student.setPassword(resultSet.getString("password"));
            student.setAge(resultSet.getInt("age"));
            sourceContext.collect(student);
        }

    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from student";
        preparedStatement = connection.prepareStatement(sql);

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
}
