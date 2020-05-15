package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @ClassName Procuder
 * @Description TODO
 * @Author zhiqiang.lv
 * @Date 2020/4/20 17:26
 * @Version 1.0
 **/
@Slf4j
public class Procuder {
    public static void main(String[] args) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 1.读取kafka生产者的配置信息
        // 具体属性名称可参考ProducerConfig,CommonClientConfigs
        Properties props = new Properties();
        props.load(ClassLoader.getSystemResourceAsStream("newProducer.properties"));
        // 2.创建producer对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 3.发送数据
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            producer.send(new ProducerRecord<>("test", "testnewapi--" + i, Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        log.info("{}->success->{}->{}", finalI, sdf.format(new Date()), recordMetadata.offset());
                    } else {
                        log.warn("{}->fail->{}", finalI, sdf.format(new Date()), e);
                    }
                }
            });
        }
        // 4.关闭资源
        producer.close();
    }
}