package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 14:37
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
public class Flink10_Transfrom_Reduce2 {
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
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.先对相同的id进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        // 3.使用聚合算子reduce实现max功能
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            /**
             * 一个分组的第一条数据来的时候，不会进入reduce方法。
             * 输入和输出的 数据类型，一定要保持一致。
             * @param value1 上次聚合后的结果
             * @param value2 当前的数据
             * @return
             * @throws Exception
             */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reduce......");
                return new WaterSensor(value1.getId(), value1.getTs(), Math.max(value1.getVc(), value2.getVc()));
            }
        });

        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}
