package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * @ClassName Consumer
 * @Description 自定义存储OFFSET
 * @Author zhiqiang.lv
 * @Date 2020/4/20 17:37
 * @Version 1.0
 **/
@Slf4j
public class CustomOffsetConsumer {
    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    public static void main(String[] args) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 1.读取kafka消费者的配置信息 具体配置参数可参考ConsumerConfig,CommonClientConfigs
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("newConsumer.properties"));
        // 1.1重置消费者的offset,可选earliest(最早的)和latest(最新的,默认)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 1.2关闭自动提交offset
        props.put("enable.auto.commit", "false");
        // 2.创建consumer对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 3.订阅主题
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
            // 4.拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
            // 4.1消费数据
            for (ConsumerRecord<String, String> record : records) {
                log.info("分区:{},偏移量:{},值:{}", record.partition(), record.offset(), record.value());
            }
            // 4.2.b异步提交
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

    //获取某分区的最新offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    //提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {
    }
}