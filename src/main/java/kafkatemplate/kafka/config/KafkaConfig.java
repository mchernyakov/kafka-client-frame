
package kafkatemplate.kafka.config;

import kafkatemplate.util.GeneralProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author chernyakov
 */
public class KafkaConfig {

    private static final String KAFKA_PROPERTIES_FILE = "./kafka.properties";

    private static Properties kafkaConsumerProperties = new Properties();
    private static Properties kafkaProducerProperties = new Properties();
    private static String kafkaServer;
    private static String topicResult;
    private static int numConsumers = 0;
    private static String groupId;

    private static List<String> topicsTasks = new ArrayList<>();

    static {

        GeneralProperties generalProperties = new GeneralProperties(KAFKA_PROPERTIES_FILE);
        init(generalProperties);
    }

    public static int getNumConsumers() {
        return numConsumers;
    }

    public static Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    public static Properties getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

    public static String getTopicResult() {
        return topicResult;
    }

    public static String getGroupId() {
        return groupId;
    }

    public static List<String> getTopicsTasks() {
        return topicsTasks;
    }

    private static void init(GeneralProperties prop) {

        kafkaServer = prop.getPropertyAsString("server");
        topicResult = prop.getPropertyAsString("topic.result");
        numConsumers = prop.getPropertyAsInt("num.consumer");
        groupId = prop.getPropertyAsString("group.id");
        String[] items = prop.getPropertyAsString("topic.task").split(",");
        topicsTasks = Arrays.asList(items);

        kafkaConsumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
}
