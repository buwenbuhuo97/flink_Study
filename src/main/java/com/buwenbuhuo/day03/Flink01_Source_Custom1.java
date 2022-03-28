package com.buwenbuhuo.day03;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

/**
 * Author 不温卜火
 * Create 2022-03-28 9:17
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 自定义source（默认无法使用多并行度）
 */
public class Flink01_Source_Custom1 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);

        // TODO 2.核心代码
        // 1.读取自定义source数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());
        // 2.打印
        streamSource.print();

        // TODO 3.启动执行
        env.execute();
    }

    // 自定义source
    public static class MySource implements SourceFunction<WaterSensor>{
        private Random random = new Random();
        private Boolean isRunning = true;
        /**
         * 生成数据
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning){
                ctx.collect(new WaterSensor("sensor" + random.nextInt(100)
                        ,System.currentTimeMillis(),random.nextInt(1000)));
                Thread.sleep(1000);
            }
        }

        /**
         * 取消生成数据
         * 一般在run方法中会有while循环，通过此方法终止while循环
         */
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
