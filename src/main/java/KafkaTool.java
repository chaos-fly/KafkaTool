import kafka.api.*;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.common.TopicAndPartition;
import kafka.network.BlockingChannel;
import scala.Option;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

import java.io.IOException;

class KafkaTool {
    public static void main(String[] args) {
        System.out.println("kafka");
        BlockingChannel channel = new BlockingChannel("127.0.0.1", 9092,
            BlockingChannel.UseDefaultBufferSize(),
            BlockingChannel.UseDefaultBufferSize(),
            5000 /* read timeout in millis */);
        channel.connect();

        final String MY_GROUP = "test_kafka_test_topic_1";
        final String MY_CLIENTID = "demoClientId";
        int correlationId = 0;
        final TopicAndPartition testPartition0 = new TopicAndPartition("test_topic_1", 0);
        final TopicAndPartition testPartition1 = new TopicAndPartition("test_topic_1", 1);

        channel.send(new GroupCoordinatorRequest(
            MY_GROUP,
            GroupCoordinatorRequest.CurrentVersion(),
            correlationId++,
            MY_CLIENTID));
        GroupCoordinatorResponse metadataResponse = GroupCoordinatorResponse.readFrom(channel.receive().payload());

        if (metadataResponse.errorCode() == ErrorMapping.NoError()) {

            Option<BrokerEndPoint> coordinatorOpt = metadataResponse.coordinatorOpt();
            BrokerEndPoint endPoint = coordinatorOpt.get();

            System.out.printf("host:%s port:%d", endPoint.host(), endPoint.port());
            if (endPoint.host() != channel.host() || endPoint.port() != channel.port()) {
                // 连接到管理offset的broker
                channel.disconnect();
                channel = new BlockingChannel(endPoint.host(), endPoint.port(),
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
                channel.connect();
            }
        } else {
            // retry (after backoff)
        }

        // How to commit offsets

        long now = System.currentTimeMillis();
        Map<TopicAndPartition, OffsetAndMetadata> offsets = new HashMap<>();
        /*
        offsets.put(testPartition1, new OffsetAndMetadata(
            new OffsetMetadata(200L, "more metadata"),
            now,
            now + 1000 * 1000 * 5));
        //offsets.put(testPartition1, new OffsetAndMetadata(200L, "more metadata"));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
            MY_GROUP,
            offsets,
            (short)1,
            correlationId++,
            MY_CLIENTID);
        channel.send(commitRequest);
        OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().payload());
        if (commitResponse.hasError()) {
            for (partitionErrorCode : commitResponse.error()) {
                if (partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                    // You must reduce the size of the metadata if you wish to retry
                } else if (partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() || partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                    channel.disconnect();
                    // Go to step 1 (offset manager has moved) and then retry the commit to the new offset manager
                } else {
                    // log and retry the commit
                }
            }
        }
    */
        /*
        // How to fetch offsets

        List<TopicAndPartition> partitions = new ArrayList<TopicAndPartition>();
        partitions.add(testPartition0);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
            MY_GROUP,
            partitions,
            (short) 1 , // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
            correlationId,
            MY_CLIENTID);
        try {
            channel.send(fetchRequest.underlying());
            OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer());
            OffsetMetadataAndError result = fetchResponse.offsets().get(testPartition0);
            short offsetFetchErrorCode = result.error();
            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                channel.disconnect();
                // Go to step 1 and retry the offset fetch
            } else if (errorCode == ErrorMapping.OffsetsLoadInProgress()) {
                // retry the offset fetch (after backoff)
            } else {
                long retrievedOffset = result.offset();
                String retrievedMetadata = result.metadata();
            }
        } catch (IOException e) {
            channel.disconnect();
            // Go to step 1 and then retry offset fetch after backoff
        }
        */
    }
}
