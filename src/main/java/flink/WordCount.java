package flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author lk
 * 2021/7/14 21:19
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
//
//        DataSet<String> dataSet = executionEnvironment.readTextFile("test.txt");
//
//        // 按照规则进行数据拆分
//        dataSet.flatMap(new Test()).groupBy(0).sum(1).print();
//
//        System.out.println("========================================");
//
//
////        dataSet.flatMap((String input, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(input.split("\\|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
////                .groupBy(0).sum(1).print();
//
//        // .setParallelism(8) 设置并行度
//        System.out.println("========================================");
//
//
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<String> stringDataSource = executionEnvironment1.readTextFile("test.txt");
//
//        stringDataSource.flatMap(new Test()).keyBy(0).sum(1).print();
//
//        executionEnvironment1.execute();


        StreamExecutionEnvironment executionEnvironment1 = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

//        // 从socket文本流读取数据
        DataStream<String> stringDataSource = executionEnvironment1.socketTextStream(host, port).disableChaining();


//        DataStream<String> stringDataSource = executionEnvironment1.readTextFile("C:\\重要文件\\java 项目\\flink-java\\src\\main\\resources\\test.txt");

//        stringDataSource.flatMap(new Test()).keyBy(0).sum(1).print();
        stringDataSource.flatMap((String input, Collector<Tuple2<String, Integer>> collector) -> Arrays.stream(input.split("\\|")).forEach(s -> collector.collect(new Tuple2<>(s, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1).print();

        stringDataSource.keyBy("").reduce((v1, v2) -> v2).print();

        DataStream<String> string = stringDataSource.map(value -> value.toString()).split(value -> Collections.singleton(value.equals("123") ? "String" : "123")).select("String");
        DataStream<String> select = stringDataSource.map(value -> value.toString()).split(value -> Collections.singleton(value.equals("123") ? "String" : "123")).select("123");



        executionEnvironment1.execute();


        stringDataSource.rebalance().shuffle().forward().broadcast();


    }

    static class TestRich extends RichMapFunction {
        @Override
        public Object map(Object o) throws Exception {
            return null;
        }


    }

    static class Test implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String s : input.split("\\|")) {
                collector.collect(new Tuple2<>(s, 1));
            }
        }
    }

}
