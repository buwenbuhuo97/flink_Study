package com.buwenbuhuo.day04;

import com.buwenbuhuo.bean.OrderEvent;
import com.buwenbuhuo.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Author 不温卜火
 * Create 2022-03-29 19:30
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:订单支付实时监控实现类
 */
public class Flink12_Project_Order {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.并行度设置为1
        env.setParallelism(1);
        // 3.分别获取两个流
        DataStreamSource<String> orderDStream = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> txDStream = env.readTextFile("input/ReceiptLog.csv");

        // TODO 2.核心代码
        // 1.分别将两条流的数据转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDStream = orderDStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> txEventDStream = txDStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2]));
            }
        });

        // 2.通过connect将两条流连接起来
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDStream.connect(txEventDStream);

        // 3.将相同交易码的数据聚合到一块
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");

        // 4.实时对账
        SingleOutputStreamOperator<String> result = orderEventTxEventConnectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            // 创建一个Map集合用来存放订单表的数据
            HashMap<String, OrderEvent> orderMap = new HashMap<>();
            // 创建一个Map集合用来存放交易表的数据
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                // 1.去对方缓存中查数据
                if (txMap.containsKey(value.getTxId())) {
                    // 有能够关联上的数据
                    out.collect("订单：" + value.getOrderId() + "对账成功");
                    // 删除已经匹配上的数据
                    txMap.remove(value.getTxId());
                } else {
                    // TODO 没有能关联上的数据
                    // 来早了，将自己存入缓存等待关联
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                // 1.去对方缓存中查数据
                if (orderMap.containsKey(value.getTxId())) {
                    // 有能够关联上的数据
                    out.collect("订单：" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    // 删除已经匹配上的数据
                    orderMap.remove(value.getTxId());
                } else {
                    // TODO 没有能关联上的数据
                    // 来早了，将自己存入缓存等待关联
                    txMap.put(value.getTxId(), value);
                }
            }
        });

        // 5.输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}
