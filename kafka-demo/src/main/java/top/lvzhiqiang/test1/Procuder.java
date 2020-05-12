package top.lvzhiqiang.test1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

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
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put("bootstrap.servers", "hexo.lvzhiqiang.top:9093");
        props.put("acks", "all");
        //重试次数
        props.put("retries", 1);
        //批次大小   只有数据积累到batch.size之后，sender才会发送数据
        props.put("batch.size", 16384);
        //等待时间   如果数据迟迟未达到batch.size，sender等待linger.time之后就会发送数据
        props.put("linger.ms", 1);
        //缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)), new Callback() {
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
        producer.close();
    }
}