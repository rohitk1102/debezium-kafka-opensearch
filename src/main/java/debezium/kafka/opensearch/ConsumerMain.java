package debezium.kafka.opensearch;

import java.util.Properties;

public class ConsumerMain {
    public static void main(String[] args) {
        Config config = Config.getInstance();
        Properties properties = config.getProperties();

        String topic = properties.getProperty("consumer.topic");

        KafkaConsumerService consumerService = new KafkaConsumerService(topic);
        consumerService.consumeMessages();
    }
}