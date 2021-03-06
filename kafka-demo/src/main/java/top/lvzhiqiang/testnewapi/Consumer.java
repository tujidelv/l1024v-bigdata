package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @ClassName Consumer
 * @Description 自动提交offset
 * @Author zhiqiang.lv
 * @Date 2020/4/20 17:37
 * @Version 1.0
 **/
@Slf4j
public class Consumer {
    public static void main(String[] args) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 1.读取kafka消费者的配置信息 具体配置参数可参考ConsumerConfig,CommonClientConfigs
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("newConsumer.properties"));
        // 1.1重置消费者的offset,可选earliest(最早的)和latest(最新的,默认)    换组(没有初始偏移量)或者offset过期(数据被删除)时该属性会生效
        // earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        // latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 2.创建consumer对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 3.订阅主题
        consumer.subscribe(Collections.singletonList("test"));
        while (true) {
            // 4.拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000L));
            // 4.1消费数据
            for (ConsumerRecord<String, String> record : records) {
                log.info("分区:{},偏移量:{},值:{}", record.partition(), record.offset(), record.value());
            }
        }
    }
}