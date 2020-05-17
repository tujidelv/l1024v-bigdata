package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName SyncProcuder
 * @Description 同步发送API
 * @Author zhiqiang.lv
 * @Date 2020/4/20 17:26
 * @Version 1.0
 **/
@Slf4j
public class SyncProcuder {
    // 带回调函数的API
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 1.读取kafka生产者的配置信息 具体配置参数可参考ProducerConfig,CommonClientConfigs
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("newProducer.properties"));
        // 1.1自定义分区拦截器,可选
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "top.lvzhiqiang.testnewapi.CustomPartitioner");
        // 2.创建producer对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 3.发送数据
        for (int i = 0; i < 10; i++) {
            String value = "testnewapi" + i;
            // 每条数据都要封装成一个ProducerRecord对象
            producer.send(new ProducerRecord<>("test", value), new Callback() {
                // 回调函数，该方法会在Producer收到ack时调用，为异步调用。
                // 该方法有两个参数，分别是RecordMetadata和Exception，如果Exception为null，说明消息发送成功，反之说明消息发送失败。
                // 注：消息发送失败会自动重试，不需要我们在回调函数中手动重试。
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        log.info("分区:{},偏移量:{},值:{}", recordMetadata.partition(), recordMetadata.offset(), value);
                    } else {
                        log.error("{}", sdf.format(new Date()), e);
                    }
                }
            }).get();
        }
        // 4.关闭资源 会做一些资源的回收,防止没达到send的要求时数据发送不出去
        producer.close();
    }
}