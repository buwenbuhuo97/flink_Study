package com.buwenbuhuo.day07;

import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * Author 不温卜火
 * Create 2022-04-02 9:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 键控状态的使用:ListState案例写法2
 */
public class Flink02_KeyedState_ListState2 {
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
        // 2.对相同Id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        // 3.案例2：针对每个传感器输出最高的3个水位值
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            // 创建ListState用来存放三个最高的水位值
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state"
                        , Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // 1.先将状态中的数据取出来
                Iterable<Integer> vcIter = listState.get();
                // 2.创建一个list集合用来存放迭代器中的数据
                ArrayList<Integer> vcList = new ArrayList<>();
                for (Integer integer : vcIter) {
                    vcList.add(integer);
                }

                // 3.将当前水位保存到状态中
                vcList.add(value.getVc());

                // 4.对集合中的数据由小到大进行排序
                vcList.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                // 5.判断集合中的元素格式是否大于3，大于3的话删除最后一个(最小的数据)
                if (vcList.size() > 3) {
                    vcList.remove(3);
                }
                // 6.架构list集合中的数据更新至状态中
                listState.update(vcList);
                out.collect(vcList.toString());
            }
        });

        // 4.打印输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}
