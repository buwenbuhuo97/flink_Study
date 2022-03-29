package com.buwenbuhuo.day04;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Author 不温卜火
 * Create 2022-03-29 8:54
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:自定义输出(有缺陷版)
 */
public class Flink04_Transform_Sink_Custom1 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.map的匿名内部类写法
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.自定义sink将数据写入MySQL
        waterSensorDStream.addSink(new MySinkFun());

        // TODO 3.启动执行
        env.execute();
    }

    public static class MySinkFun implements SinkFunction<WaterSensor>{
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            // Writes the given value to the sink. This function is called for every record.
            System.out.println("创建连接");
            // 获取连接
            Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/test?useSSL=false",
                    "root", "123456");
            // 获取语句预执行者
            PreparedStatement pstm = connection.prepareStatement("insert into sensor values(?, ?, ?)");

            // 给占位符赋值
            pstm.setString(1, value.getId());
            pstm.setLong(2,value.getTs());
            pstm.setInt(3,value.getVc());

            pstm.execute();

            pstm.close();

            connection.close();
        }
    }
}
