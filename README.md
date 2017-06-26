## KafkaTool

从0.8.1开始，kafka可以选择保存consumer offsets 到kafka的__consumer_offsets 队列，这样的话修改一个consumer group的offset不是很方便（原来直接修改zookeeper的值就ok）
官方貌似也没有提供工具，不过在文档里面提供了修改的方法https://cwiki.apache.org/confluence/display/KAFKA/Committing+and+fetching+consumer+offsets+in+Kafka 
不过新的版本的kafka接口改了不少，和这个例子对不上。所以用kafka-clients的api写了个小工具。工具使用有些限制：
* 修改offset之前，要停掉该consumer-group下所有的consumer
* 修改的时候，topic的每个partition必须要有至少一条可消费记录
