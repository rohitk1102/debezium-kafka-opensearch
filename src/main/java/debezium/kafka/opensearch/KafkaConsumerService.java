package debezium.kafka.opensearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.apache.http.HttpHost;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.*;

public class KafkaConsumerService {
    private KafkaConsumer<String, String> consumer;
    private RestHighLevelClient openSearchClient;
    private String topic;
    private Config config;
    private static final Logger LOGGER = Logger.getLogger( KafkaConsumerService.class.getName());

    public KafkaConsumerService(String topic) {
        config = Config.getInstance();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperties().getProperty("kafka.bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperties().getProperty("consumer.group.id"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getProperties().getProperty("consumer.auto.offset.reset"));
        this.topic = topic;
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
        this.openSearchClient = new RestHighLevelClient(RestClient.builder(new HttpHost(config.getProperties().getProperty("opensearch.host"), 9200, "http")));

    }

    public void consumeMessages() {
        LOGGER.info("Starting to consume messages");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error consuming record error: " +e);
            throw  new RuntimeException("Error consuming record error: " +e);
        } finally {
            consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        String value = record.value();
        try {
            JSONObject json = new JSONObject(value);
            String operation = json.getString("op");
            JSONObject after = json.getJSONObject("after");
            if (after != null) {
                String key = after.getString("key");
                JSONObject data = after.getJSONObject("value");
                String id = data.getJSONObject("object").getString("id");
                LOGGER.info("Processing record with id: "+ id + " value: "+ value);
                switch (operation) {
                    case "c":
                        createOrUpdateRecord(id, data); // Create
                        break;
                    case "u":
                        createOrUpdateRecord(id, data); // Update
                        break;
                    case "d":
                        deleteRecord(id); // Delete
                        break;
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error consuming record: " + record + " error: " +e);
        }
    }

    private void createOrUpdateRecord(String id, JSONObject jsonObject) throws IOException {
        IndexRequest indexRequest = new IndexRequest(config.getProperties().getProperty("opensearch.index"))
                .id(id)
                .source(jsonObject.getJSONObject("object").toMap());
        IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
        LOGGER.info("indexed record: " + id + " indexResponse: "+ indexResponse);
    }


    private void deleteRecord(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(config.getProperties().getProperty("opensearch.index"), id);
        try {
            DeleteResponse deleteResponse = openSearchClient.delete(deleteRequest, RequestOptions.DEFAULT);
            LOGGER.info("Deleted record: " + id + " delete Response: "+ deleteResponse);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error deleting record: " + id + " error: " +e);
        }
    }

    public void close() throws IOException {
        consumer.close();
        openSearchClient.close();
    }
}