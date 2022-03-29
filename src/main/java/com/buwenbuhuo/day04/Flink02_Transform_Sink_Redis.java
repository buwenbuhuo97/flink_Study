package com.buwenbuhuo.day04;

import com.alibaba.fastjson.JSONObject;
import com.buwenbuhuo.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Author 不温卜火
 * Create 2022-03-29 8:48
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:往Redis写入数据
 */
public class Flink02_Transform_Sink_Redis {
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

        waterSensorDStream.print();

        // 2.将数据写入Redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop01")
                .setPort(6379)
                .build();

        waterSensorDStream.addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<WaterSensor>() {
            /**
             * 指定redis的写入命令
             * additionalkey是在使用hash或者sort Set的时候需要指定的，hash类型指的是redis的大key
             * @return
             */
            @Override
            public RedisCommandDescription getCommandDescription() {
                /**
                 * 输入数据：
                 *      s1 1 1
                 *
                 * RedisCommand.SET的输出样式：
                 *  hadoop01:6379> keys *
                 *  1) "s1"
                 *  hadoop01:6379> get s1
                 *  "{\"id\":\"s1\",\"ts\":1,\"vc\":1}"
                 */
                // return new RedisCommandDescription(RedisCommand.SET);
                /**
                 * 输入数据：
                 *      s1 1 1
                 *
                 * RedisCommand.HSET的输出样式
                 *   hadoop01:6379> keys *
                 *   1) "0329"
                 *   hadoop01:6379> HGETALL 0329
                 *   1) "s1"
                 *   2) "{\"id\":\"s1\",\"ts\":1,\"vc\":1}"
                 */
                return new RedisCommandDescription(RedisCommand.HSET, "0329");
            }

            /**
             * 指定redis的key
             * @param data
             * @return
             */
            @Override
            public String getKeyFromData(WaterSensor data) {
                return data.getId();
            }

            /**
             * 指定存入redis的value
             * @param data
             * @return
             */
            @Override
            public String getValueFromData(WaterSensor data) {
                return JSONObject.toJSONString(data);
            }
        }));


        // TODO 3.启动执行
        env.execute();
    }
}
