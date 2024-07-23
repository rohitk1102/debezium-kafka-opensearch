package debezium.kafka.opensearch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaProducerService {
    private KafkaProducer<String, String> producer;
    private String topic;
    private String filePath;
    private static final Logger LOGGER = Logger.getLogger( KafkaProducerService.class.getName());

    public KafkaProducerService(String topic, String filePath) {
        Config config = Config.getInstance();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperties().getProperty("kafka.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.filePath = filePath;
    }

    public void produceMessages() {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
                producer.send(record);
            }
            LOGGER.info("All records sent to topic: " + topic + " filePath: "+ filePath);
        } catch (IOException e) {
            throw new RuntimeException("Error sending cdc to topic: "+ topic + " "+ e.getMessage());
        } finally {
            producer.close();
        }
    }
}
