package top.lvzhiqiang.testnewapi;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @ClassName CustomInterceptor
 * @Description 自定义拦截器{1.时间戳拦截器；2.统计发送消息成功和发送失败消息数，并在producer关闭时打印这两个计数器}
 * @Author zhiqiang.lv
 * @Date 2020/5/20 13:55
 * @Version 1.0
 **/
@Slf4j
public class CustomInterceptor implements ProducerInterceptor<String, String> {
    private int errorCounter = 0;
    private int successCounter = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        // 创建一个新的record，把时间戳写入消息体的最前部
        return new ProducerRecord(producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                System.currentTimeMillis() + "," + producerRecord.value().toString());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 统计成功和失败的次数
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    @Override
    public void close() {
        // 保存结果
        log.error("Successful sent: {}", successCounter);
        log.error("Failed sent: {}", errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}