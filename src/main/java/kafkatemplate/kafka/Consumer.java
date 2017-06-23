package kafkatemplate.kafka;

import kafkatemplate.process.Processor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Consumer
 *
 * @author chernyakov
 */
public class Consumer implements Runnable {

    private static Logger log = Logger.getLogger(Consumer.class.getName());

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private KafkaConsumer<String, String> consumer;
    private int id;
    private final List<? extends Processor> processors;
    private final List<String> topics;

    /**
     * Constructor of consumer
     *
     * @param id                      id num
     * @param kafkaConsumerProperties properties
     * @param topics                  topics
     * @param processors              object impl processors
     */
    public Consumer(
            int id,
            Properties kafkaConsumerProperties,
            List<String> topics,
            List<? extends Processor> processors
    ) {

        this.processors = processors;
        this.topics = topics;
        this.id = id;
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);

    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            log.info("Start analysis consumer id: " + this.id);

            while (!closed.get()) {

                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

                for (ConsumerRecord<String, String> record : records) {
                    processors.forEach(processor ->
                            processor.process(record.value()));
                }

                try {
                    consumer.commitSync();
                    //consumer.commitAsync();
                } catch (CommitFailedException e) {
                    log.warn(e.toString());
                }
            }

        } finally {
            consumer.close();
        }
    }


    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

}
