package com.buwenbuhuo.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-23 18:46
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: WordCount的批处理(自定义一个类实现接口)
 */
public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件中读取数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        /**
         * 在spark中：
         *      -> 先对数据使用flatmap算子（将数据按照空格切分，打散成tuple2元组(word,1)）
         *      -> reduceByKey(将相同单词的数据聚合到一起，然后做累加)
         *      -> 打印到控制台
         */
        // 3.转换数据格式：将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = dataSource.flatMap(new MyFlatMap());

        // 4.将相同的单词聚合到一块
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        // 5. 将单词的个数做累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        // 6.结果打印输出
        result.print();
    }

    /**
     * 自定义实现类
     */
    public static class MyFlatMap implements FlatMapFunction<String,Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 将数据按照空格切分
            String[] words = value.split(" ");
            // 遍历出每一个单词
            for (String word : words) {
                // 写法1
                // out.collect(new Tuple2<>(word, 1));
                // 写法2
                out.collect(Tuple2.of(word, 1));
            }
        }
    }

}
