package com.buwenbuhuo.day09;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


/**
 * Author 不温卜火
 * Create 2022-04-07 10:08
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:聚合函数（Aggregate Functions）:不注册直接使用函数写法
 */
public class Flink06_UDF_TableAggFun1 {
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

        // 4.不注册直接使用函数
        table
                .groupBy($("id"))
                .flatAggregate(call(MyUDTAF.class, $("vc")).as("top", "rank"))
                .select($("id"), $("top"), $("rank"))
                .execute()
                .print();


        // TODO 3.启动执行
        env.execute();

    }


    /**
     * 自定义一个表聚合函数,多进多出，根据VC求Top2
     */
    public static class MyTopACC {
        public Integer first;
        public Integer second;
    }

    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer, String>, MyTopACC> {

        @Override
        public MyTopACC createAccumulator() {
            MyTopACC acc = new MyTopACC();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        // 比较数据，如果当前数据大于累加器中存的数据则替换，并将原累加器中的数据往下（第二）赋值
        public void accumulate(MyTopACC acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // 计算（排名）
        public void emitValue(MyTopACC acc, Collector<Tuple2<Integer, String>> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, "1"));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, "2"));
            }
        }
    }
}
