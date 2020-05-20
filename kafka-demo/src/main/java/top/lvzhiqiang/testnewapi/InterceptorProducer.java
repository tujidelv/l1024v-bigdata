package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName InterceptorProducer
 * @Description 带拦截器的producer
 * @Author zhiqiang.lv
 * @Date 2020/5/20 15:49
 * @Version 1.0
 **/
@Slf4j
public class InterceptorProducer {
    // 带回调函数的API
    public static void main(String[] args) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 1.读取kafka生产者的配置信息 具体配置参数可参考ProducerConfig,CommonClientConfigs
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("newProducer.properties"));
        // 1.1自定义分区器,可选
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "top.lvzhiqiang.testnewapi.CustomPartitioner");
        // 1.2自定义拦截器链,可选
        List<String> interceptors = new ArrayList<>();
        interceptors.add("top.lvzhiqiang.testnewapi.CustomInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
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
            });
        }
        // 4.关闭资源 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}