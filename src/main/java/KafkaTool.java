import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaTool {

    private String mGroup;
    private String mTopic;
    private KafkaConsumer<String, String> mConsumer;

    public KafkaTool(String host, String topic, String group) {
        Properties props = new Properties();
        props.put("bootstrap.servers", host);
        props.put("zk.connectiontimeout.ms", "5000000");
        props.put("group.id", group);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        mGroup = group;
        mTopic = topic;
        mConsumer = new KafkaConsumer<>(props);
    }

    public void TestAssign() {
        // 自行balance partition，不能和subscribe混用
        // offset需要自己保存，开始的时候通过seek指定开始消费的位置
        List<TopicPartition> topicPartitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = mConsumer.partitionsFor(mTopic);
        for (PartitionInfo p : partitionInfos) {
            //System.out.println(p);
            topicPartitions.add(new TopicPartition(mTopic, p.partition()));
        }

        mConsumer.assign(topicPartitions);
        //mConsumer.seekToBeginning(topicPartitions);
        for (TopicPartition p : topicPartitions) {
            // 最新的offset
            long position = mConsumer.position(p);
            System.out.printf("partion:%d position:%s%n", p.partition(), position);

            mConsumer.seek(p, position - 10L);
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while (true) {
            ConsumerRecords<String, String> records = mConsumer.poll(500);
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partionRecords) {
                    System.out.printf("partition:%d offset:%d value:%s time:%s %d%n",
                        partition.partition(), record.offset(), record.value(),
                        format.format(new Date(record.timestamp() * 1000)), record.timestamp());
                }

                // save offset by self
            }
        }
    }

    public void ResetConsumerGroupOffset(Map<Integer, Long> partition2Offset) {
        mConsumer.subscribe(Arrays.asList(mTopic));

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : partition2Offset.entrySet()) {
            topicPartitions.add(new TopicPartition(mTopic, entry.getKey()));
        }

        // 该分区最旧的记录
        System.out.println("==== 开始校验每个分区最旧的消息的offset ====");
        System.out.println(topicPartitions);
        Map<TopicPartition, Long> beginOffset = mConsumer.beginningOffsets(topicPartitions);
        for (Map.Entry<TopicPartition, Long> entry : beginOffset.entrySet()) {
            Integer p = entry.getKey().partition();
            if (entry.getValue().compareTo(partition2Offset.get(p)) > 0) {
                System.out.printf("Error. partition:%d min offset is %d %n", p, entry.getValue());
                return;
            }
            System.out.printf("partition:%d oldest offset:%d%n", p, entry.getValue());
        }

        // 该分区最新的记录
        System.out.println("==== 开始校验每个分区最新的消息的offset ====");
        Map<TopicPartition, Long> endOffset = mConsumer.endOffsets(topicPartitions);
        for (Map.Entry<TopicPartition, Long> entry : endOffset.entrySet()) {
            Integer p = entry.getKey().partition();
            if (entry.getValue().compareTo(partition2Offset.get(p)) < 0) {
                System.out.printf("Error. partition:%d max offset is %d %n", p, entry.getValue());
                return;
            }
            System.out.printf("partition:%d newest offset:%d%n", p, entry.getValue());
        }

        System.out.println("==== 开始重置offset，每个分区至少要有一条未消费记录 ====");
        Set<Integer> allPatitions = partition2Offset.keySet();
        int retry = 3;
        while (allPatitions.size() > 0) {
            ConsumerRecords<String, String> records = mConsumer.poll(3000);
            for (TopicPartition partition : records.partitions()) {
                if (!allPatitions.contains(partition.partition())) {
                    continue;
                }

                long offset = partition2Offset.get(partition.partition());
                mConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                allPatitions.remove(partition.partition());
                System.out.printf("Ok. set partition:%d offset to:%d %n", partition.partition(), offset);
            }

            if (records.isEmpty()) {
                System.out.println("poll msg from topic timeout.");
            }

            retry --;
            if (retry == 0) {
                break;
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("kafka tool");

        KafkaTool tool = new KafkaTool("106.38.255.199:9092", "test_topic_1", "test_kafka_test_topic_1");

        Map<Integer, Long> p2offset = new HashMap<>();
        p2offset.put(0, 21897L);
        p2offset.put(1, 1300L);
        tool.ResetConsumerGroupOffset(p2offset);
        //tool.TestAssign();
    }
}