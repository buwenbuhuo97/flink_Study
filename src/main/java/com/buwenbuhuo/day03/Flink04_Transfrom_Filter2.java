package com.buwenbuhuo.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-28 10:02
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:filter的静态内部类写法
 */
public class Flink04_Transfrom_Filter2 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.使用filter算子将奇数获取到
        SingleOutputStreamOperator<String> filter = streamSource.filter(new MyRichfilter());

        // 2.打印
        filter.print();

        // TODO 3.启动执行
        env.execute();
    }
    public static class MyRichfilter extends RichFilterFunction<String>{
        @Override
        public boolean filter(String input) throws Exception {
            return Integer.parseInt(input) % 2 != 0;
        }
    }
}
