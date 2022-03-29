package com.buwenbuhuo.day04;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * Author 不温卜火
 * Create 2022-03-29 8:53
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:往Elasticsearch写入数据
 */
public class Flink03_Transform_Sink_elasticsearch {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);

        // TODO 2.核心代码
        // 1.将读过来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        // 2.将数据写入Elasticsearch
        ArrayList<HttpHost> httphosts = new ArrayList<>();
        httphosts.add(new HttpHost("hadoop01",9200));
        httphosts.add(new HttpHost("hadoop02",9200));
        httphosts.add(new HttpHost("hadoop03",9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httphosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                // 指定要插入的索引，类型，名 docID
                IndexRequest indexRequest = new IndexRequest("220329_flink", "_doc", "1001");
                String jsonString = JSONObject.toJSONString(element);
                // 放入数据 显示声明为JSON字符串
                indexRequest.source(jsonString, XContentType.JSON);
                // 添加写入请求
                indexer.add(indexRequest);
            }
        });

        // 因为读的是无界数据流，es会默认将数据先缓存起来，如果要实现来一条写一条，则将这个参数设置为1.注意：生产种不要这样设置
        waterSensorBuilder.setBulkFlushMaxActions(1);
        waterSensorDStream.addSink(waterSensorBuilder.build());

        // TODO 3.启动执行
        env.execute();
    }
}
