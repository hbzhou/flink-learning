package com.itsz.flink.mysql.domain;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Student {

    private int id;

    private String name;

    private String password;

    private int age;


    public Student(int id, String name, String password, int age) {
        this.id = id;
        this.name = name;
        this.password = password;
        this.age = age;
    }

    public Student() {
    }
}
