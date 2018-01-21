package kafkatemplate.kafka;

import kafkatemplate.process.Processor;
import kafkatemplate.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ConsumerPool<K, V> implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(ConsumerPool.class.getName());

    private final ExecutorService executorService;
    private final int numConsumers;
    private final List<? extends Processor<K, V>> processors;
    private final List<String> topics;
    private final Properties consumerProperties;
    private final List<Consumer<K, V>> consumers;

    public ConsumerPool(int numConsumers, List<? extends Processor<K, V>> processors, List<String> topics, Properties consumerProperties) {
        this.numConsumers = numConsumers;
        this.processors = processors;
        this.topics = topics;
        this.consumerProperties = consumerProperties;
        this.consumers = new ArrayList<>();

        ThreadFactory threadFactory = ThreadUtil.getThreadFactoryCollection("consumer", false);
        this.executorService = Executors.newFixedThreadPool(numConsumers, threadFactory);
    }

    public void start() {
        logger.info("Start consumers...");
        for (int i = 0; i < numConsumers; i++) {
            Consumer<K, V> consumer = new Consumer<>(
                    i,
                    consumerProperties,
                    topics,
                    processors
            );

            consumers.add(consumer);
            executorService.submit(consumer);
        }
    }

    @Override
    public void close() throws Exception {
        consumers.forEach(Consumer::shutdown);
        executorService.shutdownNow();
        executorService.awaitTermination(2, TimeUnit.SECONDS);
    }
}
