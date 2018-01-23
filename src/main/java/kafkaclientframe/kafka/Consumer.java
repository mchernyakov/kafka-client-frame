package kafkaclientframe.kafka;

import kafkaclientframe.process.Processor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer<K, V> implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    private static final int KAFKA_CONNECTION_RETRY_INTERVAL = 30000;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final List<? extends Processor<K, V>> processors;
    private final List<String> topics;
    private final Properties kafkaConfig;

    private KafkaConsumer<K, V> consumer;
    private int id;

    public Consumer(
            int id,
            Properties kafkaConsumerProperties,
            List<String> topics,
            List<? extends Processor<K, V>> processors
    ) {

        this.processors = processors;
        this.topics = topics;
        this.id = id;
        this.kafkaConfig = kafkaConsumerProperties;

    }

    private void connect() {
        boolean connected = false;

        while (!connected) {
            try {
                this.consumer = new KafkaConsumer<>(this.kafkaConfig);
                this.consumer.subscribe(this.topics);

                connected = true;
            } catch (Exception e) {
                logger.error("Failed to connect to kafka", e);
                try {
                    Thread.sleep(KAFKA_CONNECTION_RETRY_INTERVAL);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interruptedException);
                }
            }
        }
    }

    @Override
    public void run() {
        try {
            connect();

            logger.info("Start consumer id: " + this.id);
            while (!closed.get()) {

                ConsumerRecords<K, V> records = consumer.poll(Long.MAX_VALUE);
                records.forEach(
                        record ->
                                processors.forEach(processor ->
                                        processor.process(record.key(), record.value())));

                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.warn("Failed to commit", e);
                }
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        if (consumer != null) {
            consumer.wakeup();
        }
    }
}
