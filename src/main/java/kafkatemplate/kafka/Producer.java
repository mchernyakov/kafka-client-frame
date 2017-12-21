package kafkatemplate.kafka;

import kafkatemplate.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

public class Producer implements AutoCloseable {

    private static Logger logger = Logger.getLogger(Producer.class.getName());

    private KafkaProducer<String, String> producer;

    public static class ProducerHolder {

        static final Producer HOLDER_INSTANCE = new Producer();
    }

    public static Producer getInstance() {
        return ProducerHolder.HOLDER_INSTANCE;
    }

    private Producer() {
        producer = new KafkaProducer<>(KafkaConfig.getKafkaProducerProperties());
    }


    /**
     * Send messsage
     *
     * @param key
     * @param value
     */
    public void sendMessage(String key, String value) {
        if (value == null) {
            logger.warn("Bad task format - null. Key =" + key);
        } else {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KafkaConfig.getTopicResult(), KafkaConfig.getKey(), value);

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
