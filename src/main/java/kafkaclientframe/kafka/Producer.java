package kafkaclientframe.kafka;

import kafkaclientframe.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer<K, V> implements AutoCloseable {

    private static Logger logger = LoggerFactory.getLogger(Producer.class.getName());

    private KafkaProducer<K, V> producer;

    public static class ProducerHolder {

        static final Producer HOLDER_INSTANCE = new Producer();
    }

    public static Producer getInstance() {
        return ProducerHolder.HOLDER_INSTANCE;
    }

    private Producer() {
        producer = new KafkaProducer<>(KafkaConfig.getKafkaProducerProperties());
    }

    public void sendMessage(K key, V value, String topic) {
        if (value == null) {
            logger.warn("Bad task format - null. Key =" + key);
        } else {
            ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, key, value);

            producer.send(producerRecord,
                    (metadata, e) -> {
                        if (e == null) {
                            logger.info("Message Delivered Successfully. Key = " + key);
                            logger.debug("Message:value = " + producerRecord.value());
                        } else {
                            logger.warn(e.toString());
                        }
                    });
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
