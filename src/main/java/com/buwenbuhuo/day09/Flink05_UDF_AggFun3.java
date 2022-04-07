package com.buwenbuhuo.day09;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Author 不温卜火
 * Create 2022-04-07 10:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:聚合函数（Aggregate Functions）:SQL写法
 */
public class Flink05_UDF_AggFun3 {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.将数据转为JavaBean，并指定WaterWark
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorDStream);

        // 4.注册自定义函数
        tableEnv.createTemporarySystemFunction("myUDAF", MyUDAF.class);

        // 5.SQL写法
        tableEnv.executeSql("select id,myUDAF(vc) from " + table + " group by id").print();


        // TODO 3.启动执行
        env.execute();

    }


    //定义一个类当做累加器，并声明总数和总个数这两个值
    public static class MyACC {
        public long vcsum;
        public int count;
    }

    // 自定义UDAF函数,求每个WaterSensor中VC的平均值
    public static class MyUDAF extends AggregateFunction<Double, Flink05_UDF_AggFun1.MyACC> {

        // 创建一个累加器
        @Override
        public Flink05_UDF_AggFun1.MyACC createAccumulator() {
            Flink05_UDF_AggFun1.MyACC myACC = new Flink05_UDF_AggFun1.MyACC();
            myACC.vcsum = 0;
            myACC.count = 0;
            return new Flink05_UDF_AggFun1.MyACC();
        }

        // 做累加操作
        public void accumulate(Flink05_UDF_AggFun1.MyACC acc, Integer vc) {
            acc.vcsum += vc;
            acc.count ++;
        }

        // 将计算结果值返回
        @Override
        public Double getValue(Flink05_UDF_AggFun1.MyACC accumulator) {
            return accumulator.vcsum * 1D / accumulator.count;
        }
    }
}
