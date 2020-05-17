package top.lvzhiqiang.testnewapi;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * @author Tujide.lv
 * @version 1.0
 * @description TODO
 * @date 2020/5/17 17:32
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 自定义
        Integer partitions = cluster.partitionCountForTopic(topic);
        if (keyBytes == null) {
            return key.toString().hashCode() % partitions;
        } else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}