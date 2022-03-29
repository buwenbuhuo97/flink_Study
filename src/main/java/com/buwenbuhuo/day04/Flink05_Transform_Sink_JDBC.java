package com.buwenbuhuo.day04;

import com.buwenbuhuo.bean.WaterSensor;
import com.mysql.jdbc.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Author 不温卜火
 * Create 2022-03-29 10:30
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:往JDBC写入数据
 */
public class Flink05_Transform_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.将读过来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.通过JDBC将数据写入MySQL
        SinkFunction<WaterSensor> sinkFunction = JdbcSink.<WaterSensor>sink(
                "insert into sensor values (?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        // 给占位符赋值
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                    }
                },
                new JdbcExecutionOptions.Builder()
                        //设置来一条写一条与ES中相似
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop01:3306/test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .withDriverName(Driver.class.getName())
                        .build()
        );

        waterSensorDStream.addSink(sinkFunction);

        // TODO 3.启动执行
        env.execute();
    }
}
