package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 10:02
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:生命周期演示
 */
public class Flink02_Transfrom_Map4 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);
        // 3.从文件种读取数据
        // DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        // TODO 2.核心代码
        // 1.map的静态内部类写法
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MyRichMap());
        // 2.打印
        map.print();

        // TODO 3.启动执行
        env.execute();
    }
    public static class MyRichMap extends RichMapFunction<String, WaterSensor> {
        /**
         * 最先被调用（生命周期方法），每个并行度调用一次
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open....");
        }

        /**
         * 最后被调用（生命周期方法），每个并行度调用一次
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close....");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            System.out.println(getRuntimeContext().getTaskName());
            String[] split = value.split(",");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }
    }
}
