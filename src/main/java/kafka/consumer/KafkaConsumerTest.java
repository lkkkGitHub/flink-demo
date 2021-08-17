package kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * @author lk
 * 2021/8/15 19:50
 */
public class KafkaConsumerTest {

    public static void main(String[] args) {

        Properties properties = new Properties();

        //kafka 集群， broker list
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.100:9092");
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 如果当前消费者组记录的offset已经失效，才会从队列中的第一个消息进行消费；默认是latest，就是最新的
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 自动提交offset的延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("testTopic"));

        while (true) {
            // 长轮询拉取消息，延迟时间
            ConsumerRecords<String, String> consumerRecords = consumer.poll(10000);

            if (consumerRecords.isEmpty()) {
                continue;
            }

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {

                System.out.println(consumerRecord.value());

                consumer.commitSync();
                consumer.commitAsync();
            }
        }
    }

    class offset implements ConsumerRebalanceListener, ProducerInterceptor<String, String> {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }


        @Override
        public ProducerRecord onSend(ProducerRecord record) {
            return null;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

}
