package flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author lk
 * 2021/7/17 10:27
 */
public class SourceCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment().setStateBackend(new StateBackend() {
            @Override
            public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
                return null;
            }

            @Override
            public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
                return null;
            }

            @Override
            public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @Nonnull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
                return null;
            }

            @Override
            public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
                return null;
            }
        });

        executionEnvironment.setStateBackend(new MemoryStateBackend());
        executionEnvironment.setStateBackend(new FsStateBackend(""));
        executionEnvironment.setStateBackend(new RocksDBStateBackend(""));

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
