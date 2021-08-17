package flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author lk
 * 2021/7/26 8:08
 */
public class KeyedProcessFunctionTest {

    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置状态后端配置
        executionEnvironment.setStateBackend(new MemoryStateBackend());
        executionEnvironment.setStateBackend(new FsStateBackend(""));
        executionEnvironment.setStateBackend(new RocksDBStateBackend(""));

        // 设置时间以eventTime为主，即当前是事件发生的时间
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置精确一次 检查点配置，检查点的间隔时间
        executionEnvironment.enableCheckpointing(1200, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = executionEnvironment.getCheckpointConfig();
        // 设置检查点的模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置检查点保存的超时时间
        checkpointConfig.setCheckpointTimeout(1200);
        // 最大的同时进行checkpoint保存（整个任务流同时进行的CheckPoint保存的数量）
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 设置两次CheckPoint保存时，必须间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(100);
        // 为0表示不允许检查点保存失败，失败即也认为程序执行失败了
        checkpointConfig.setTolerableCheckpointFailureNumber(0);


        // 指定重启策略
        executionEnvironment.setRestartStrategy(RestartStrategies.noRestart());


        DataStream<String> stringDataSource = executionEnvironment.readTextFile("C:\\重要文件\\java 项目\\flink-java\\src\\main\\resources\\test.txt");

        //stringDataSource.keyBy(0).process((in, ctx, out) -> {}).print();

        System.out.println(extracted());

    }

    private static int extracted() {
        int i = 0;

        try {
            System.out.println("try");
            i++;
            return i;
        } catch (Exception e) {
            System.out.println("Exception");
        } finally {
            System.out.println("finally");
            ++i;
        }
        return i;
    }
}
