package debezium.kafka.opensearch;

import java.util.Properties;

public class ProducerMain {
    public static void main(String[] args) {
        Config config = Config.getInstance();
        Properties properties = config.getProperties();

        String topic = properties.getProperty("producer.topic");
        String filePath = properties.getProperty("producer.filepath");

        KafkaProducerService producerService = new KafkaProducerService(topic, filePath);
        producerService.produceMessages();
    }
}