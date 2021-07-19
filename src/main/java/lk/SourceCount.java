package lk;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author lk
 * 2021/7/17 10:27
 */
public class SourceCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stringDataSource = executionEnvironment.readTextFile("C:\\重要文件\\java 项目\\flink-java\\src\\main\\resources\\test.txt");

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();


        stringDataSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(value.split("\\|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1).print();

//        stringDataSource.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(value.split("//|")).forEach(s -> out.collect(new Tuple2<>(s, 1))))
//                .returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0).sum(1).addSink(new RedisSink<>(config, new MyRedisMapper()));

        executionEnvironment.execute();
    }

    static class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, null);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return String.valueOf(data.f1);
        }

    }

}
