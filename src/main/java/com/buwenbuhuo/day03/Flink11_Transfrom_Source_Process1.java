package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-28 15:01
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:Process匿名实现写法实现map的功能
 */
public class Flink11_Transfrom_Source_Process1 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> WaterSensorDStream = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        // 2.打印
        WaterSensorDStream.print();

        // TODO 3.启动执行
        env.execute();
    }
}
