package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author lk
 * 2021/8/15 15:30
 */
public class KafkaProducerTest {

    public static void main(String[] args) {

        Properties properties = new Properties();

        //kafka 集群， broker list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小 每批发送16k数据，不足16k等待linger.ms毫秒之后即进行发送
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小（线程共享变量的大小）
        properties.put("buffer.memory", 33554432);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Arrays.asList("", ""));


        Producer<String, String> producer = new KafkaProducer<>(properties);

        producer.send(new ProducerRecord<>("testTopic", "testProducer"));

        // 指定发送分区，key
        producer.send(new ProducerRecord<>("testTopic", 0, "test", "123"));
        producer.send(new ProducerRecord<>("testTopic", "test123",  "123"));

        producer.send(new ProducerRecord<>("testTopic", "testProducer123"), ((metadata, exception) -> {
            if (exception == null) {
                System.out.println(metadata.topic());
            }
        }));

        producer.close();
    }

}
