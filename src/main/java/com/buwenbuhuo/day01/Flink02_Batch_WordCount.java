package com.buwenbuhuo.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author 不温卜火
 * Create 2022-03-25 10:20
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:WordCount的批处理（lamda表达式写法）
 */
public class Flink02_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件中读取数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        // 3.转换数据格式：将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = dataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本进行分词
            String[] words = line.split(" ");
            // 将每个单词转换成二元组输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
            // 如果使用lambda表达式，可能因为类型擦除 报错解决： returns（Types.类型）
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> groupBy = wordAndOne.groupBy(0);

        // 5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> result = groupBy.sum(1);

        // 6.结果打印输出
        result.print();
    }
}