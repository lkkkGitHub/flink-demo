package lk;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lk
 * 2021/7/26 8:08
 */
public class KeyedProcessFunctionTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置时间以eventTime为主，即当前是事件发生的时间
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



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
