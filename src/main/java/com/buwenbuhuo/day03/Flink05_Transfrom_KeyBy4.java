package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 10:02
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:KeyBy：通过属性名获取key，常用于pojo(JavaBean)
 */
public class Flink05_Transfrom_KeyBy4 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.使用map将从端口读进来的字符串转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        // 2.使用KeyBy将相同id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        // 2.打印
        keyedStream.print();

        // TODO 3.启动执行
        env.execute();
    }
}
