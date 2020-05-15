package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * @ClassName Consumer
 * @Description TODO
 * @Author zhiqiang.lv
 * @Date 2020/4/20 17:37
 * @Version 1.0
 **/
@Slf4j
public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "144.34.207.84:9093");
        //消费者组，只要group.id相同，就属于同一个消费者组
        props.put("group.id", "test");
        //关闭自动提交offset
        props.put("enable.auto.commit", "false");
        //key,value反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 订阅主题
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            // 拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
            // 消费数据
            for (ConsumerRecord<String, String> record : records) {
                log.info("{},offset={},key={},value={}", sdf.format(new Date()), record.offset(), record.key(), record.value());
            }
            //异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        log.error("Commit failed for {}", offsets, exception);
                    }
                }
            });
        }
    }
}