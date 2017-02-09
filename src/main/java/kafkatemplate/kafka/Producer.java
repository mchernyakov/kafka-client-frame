package kafkatemplate.kafka;

import kafkatemplate.kafka.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;


/**
 * Producer
 *
 * @author chernyakov
 */
public class Producer {

    private static Logger log = Logger.getLogger(Producer.class.getName());

    private ProducerRecord<String, String> producerRecord;
    private KafkaProducer<String, String> producer;

    public static class ProducerHolder {

        static final Producer HOLDER_INSTANCE = new Producer();
    }

    public static Producer getInstance() {
        return ProducerHolder.HOLDER_INSTANCE;
    }

    private Producer() {

        if (KafkaConfig.PROPERTIES_INIT_DONE) {
            producer = new KafkaProducer<>(KafkaConfig.getKafkaProducerProperties());
        } else {
            throw new IllegalArgumentException("Error in init kafkaProducerPropeties");
        }
    }

    /**
     * Send messsage
     *
     * @param value  сообщение
     * @param taskId
     */
    public void sendMessage(String value, String taskId) {
        try {
            if (value == null) {
                throw new IllegalArgumentException("Bad task format - null. Task id = " + taskId);
            }

            producerRecord = new ProducerRecord<>(KafkaConfig.getTopicResult(), KafkaConfig.getKey(), value);

            producer.send(producerRecord,
                    (metadata, e) -> {
                        if (e == null) {
                            log.info("Message Delivered Successfully. TaskId = " + taskId);
                            log.debug("Message:value = " + producerRecord.value());
                        } else {
                            log.warn(e.toString());
                        }

                    });

        } catch (IllegalArgumentException e) {
            log.warn(e.toString());
        }
    }
}
