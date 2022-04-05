package com.buwenbuhuo.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author 不温卜火
 * Create 2022-04-02 11:06
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: 算子状态:列表状态举例
 */
public class Flink07_OperatorState_List {
    public static void main(String[] args) throws Exception {
        // TODO 1.准备工作
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2.如果不想打印出来cpu线程数，可以将并行度设置为1
        env.setParallelism(1);
        // 3.获取流
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop01", 7777);


        // TODO 2.核心代码
        SingleOutputStreamOperator<Long> result = streamSource.map(new MyCountMapper());
        result.print();

        // TODO 3.启动执行
        env.execute();
    }
    private static class MyCountMapper implements MapFunction<String, Long>, CheckpointedFunction {

        private Long count = 0L;
        private ListState<Long> state;

        @Override
        public Long map(String value) throws Exception {
            count++;
            return count;
        }

        // 初始化时会调用这个方法，向本地状态中填充数据. 每个子任务调用一次
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("初始化...");
            state = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("state", Long.class));
            for (Long c : state.get()) {
                count += c;
            }
        }

        // Checkpoint时会调用这个方法，我们要实现具体的snapshot逻辑，比如将哪些本地状态持久化
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            state.clear();
            state.add(count);
        }
    }

}
