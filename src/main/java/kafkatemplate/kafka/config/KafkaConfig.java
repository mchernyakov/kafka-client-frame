
package kafkatemplate.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author chernyakov
 */
public class KafkaConfig {

    private static Logger log = Logger.getLogger(KafkaConfig.class.getName());

    private static final String KAFKA_PROPERTIES_FILE = "./kafka.properties";

    public static final boolean PROPERTIES_INIT_DONE;

    private static Properties kafkaConsumerProperties = new Properties();
    private static Properties kafkaProducerProperties = new Properties();
    private static String kafkaServer;
    private static String topicResult;
    private static String key;
    private static int numConsumers = 0;
    private static String groupId;

    private static List<String> topicsTasks = new ArrayList<>();

    static {
        boolean result = false;

        try {
            result = init();
        } catch (IOException e) {
            log.error(e.toString());
        }

        PROPERTIES_INIT_DONE = result;
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

    public static String getKey() {
        return key;
    }

    public static String getGroupId() {
        return groupId;
    }

    public static List<String> getTopicsTasks() {
        return topicsTasks;
    }

    private static boolean init() throws IOException {
        InputStream in = KafkaConfig.class.getClassLoader().getResourceAsStream(KAFKA_PROPERTIES_FILE);
        Properties prop = new Properties();

        if (in != null) {
            prop.load(in);
        } else {
            throw new FileNotFoundException(KAFKA_PROPERTIES_FILE);
        }

        kafkaServer = prop.getProperty("server");

        key = prop.getProperty("key");
        topicResult = prop.getProperty("topic.result");

        numConsumers = Integer.parseInt(prop.getProperty("num.consumer"));
        groupId = prop.getProperty("group.id");
        String[] items = prop.getProperty("topic.task").split(",");
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

        return true;
    }
}
