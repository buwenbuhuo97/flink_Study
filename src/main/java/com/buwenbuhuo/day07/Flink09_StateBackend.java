package com.buwenbuhuo.day07;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.IOException;

/**
 * Author 不温卜火
 * Create 2022-04-02 11:30
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */
public class Flink09_StateBackend {
    public static void main(String[] args) throws IOException {
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 设置状态后端写法
         */
        // TODO 1.12版本写法
        // 内存级别
        env.setStateBackend(new MemoryStateBackend());

        // 文件级别
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/flink/checkpoints/fs"));

        // RocksDB(本地)级别
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:8020/flink/checkpoints/rocksdb"));



        // TODO 1.13写法
        // 内存级别
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // 文件级别
        env.setStateBackend(new HashMapStateBackend());
        // 写法1
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(
                "hdfs://hadoop01:8020/flink/checkpoints/fs"));
        // 写法2
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/flink/checkpoints/fs");

        // RocksDB(本地)级别
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 写法1
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(
                "hdfs://hadoop01:8020/flink/checkpoints/rocksdb"));
        // 写法2
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop01:8020/flink/checkpoints/rocksdb");
    }
}
