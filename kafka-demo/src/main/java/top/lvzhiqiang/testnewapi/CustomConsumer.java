package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * @ClassName CustomConsumer
 * @Description TODO
 * @Author zhiqiang.lv
 * @Date 2020/4/22 15:38
 * @Version 1.0
 **/
@Slf4j
public class CustomConsumer {
    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) {
        //创建配置信息
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "144.34.207.84:9093");
        //消费者组，只要group.id相同，就属于同一个消费者组
        props.put("group.id", "test");
        //关闭自动提交offset
        props.put("enable.auto.commit", "false");
        //Key和Value的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //消费者订阅主题
        consumer.subscribe(Arrays.asList("test"), new ConsumerRebalanceListener() {
            //该方法会在Rebalance之前调用
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            //该方法会在Rebalance之后调用
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    //定位到最近提交的offset位置继续消费
                    consumer.seek(partition, getOffset(partition));
                }
            }
        });
        while (true) {
            //消费者拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
            //消费数据
            for (ConsumerRecord<String, String> record : records) {
                log.info("{},offset={},key={},value={}", sdf.format(new Date()), record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            //异步提交
            commitOffset(currentOffset);
        }
    }

    //获取某分区的最新offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    //提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
    }
}