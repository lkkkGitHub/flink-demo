package lk;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lk
 * 2021/7/18 12:33
 */
public class WindowCount {

    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置时间以eventTime为主，即当前是事件发生的时间
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        DataStream<String> stringDataSource = executionEnvironment.readTextFile("C:\\重要文件\\java 项目\\flink-java\\src\\main\\resources\\test.txt");

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();


        stringDataSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(value.split("\\|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1).print();

        // 将所有数据放入到一个窗口中
//        stringDataSource.windowAll();

        // 增量聚合
        stringDataSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(value.split("\\|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).timeWindow(Time.seconds(10)).aggregate(new TestAggregateFunction()).print();

        // 全窗口函数
        stringDataSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(value.split("\\|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).timeWindow(Time.seconds(10)).apply(
                (WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>) (tuple, window, input, out) -> {
                    List<Tuple2<String, Integer>> list = IteratorUtils.toList(input.iterator());
                    list.stream().collect(Collectors.groupingBy(tuple2 -> tuple2.f0, Collectors.summingInt(tuple2 -> tuple2.f1))).forEach((key, value) -> out.collect(new Tuple2<>(key, value)));
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).print();

        // 计数窗口
        stringDataSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(value.split("\\|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).countWindow(10);

    }

    // 增量聚合
    static class TestAggregateFunction implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {

        // 创建初始累加器状态
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        // 累加方法数值
        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + value.f1;
        }

        // 返回累加结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        // 当数据跨分区时，使用该函数将两个分区的计算结果合并。由于此方法被调用前，经历了keyBy操作，所以此方法在这里无效
        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }
}
