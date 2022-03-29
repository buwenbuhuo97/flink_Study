package com.buwenbuhuo.day04;

import com.buwenbuhuo.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-03-29 19:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:各省份页面广告点击量实时统计实现类
 */
public class Flink11_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.并行度设置为1
        env.setParallelism(1);
        // 3.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        // TODO 2.核心代码
        // 1.将数据转为JavaBean并组成二元组返回
        SingleOutputStreamOperator<Tuple2<String, Integer>> provinceWithAdIDToOneDStream = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
                return Tuple2.of(adsClickLog.getProvince() + "-" + adsClickLog.getAdId(), 1);
            }
        });

        // 2.将相同的key聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = provinceWithAdIDToOneDStream.keyBy(0);

        // 3。累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 4. 输出
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
}
