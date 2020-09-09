package com.itsz.flink.learning.table.api;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

public class TableApiTest {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamEnv);

        final Schema schema = new Schema()
                .field("Date", DataTypes.STRING())
                .field("Open", DataTypes.STRING())
                .field("High", DataTypes.STRING())
                .field("Low", DataTypes.STRING())
                .field("Close", DataTypes.STRING())
                .field("Volume", DataTypes.STRING());


        tableEnvironment.connect(new FileSystem().path("D:\\git_code\\flink-learning\\flink-table-api\\src\\main\\resources\\1810.HK.csv"))
                .withFormat(new OldCsv())
                .withSchema(schema)
                .createTemporaryTable("CsvSinkTable");

        Table table = tableEnvironment.sqlQuery("select * from CsvSinkTable");

        DataStream<Quotation> stringDataStream = tableEnvironment.toAppendStream(table, Quotation.class);

        stringDataStream.print();

        streamEnv.execute("data set api");


    }
}
