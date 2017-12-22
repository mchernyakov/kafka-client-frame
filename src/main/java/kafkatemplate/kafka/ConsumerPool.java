package kafkatemplate.kafka;

import kafkatemplate.process.Processor;
import kafkatemplate.util.ThreadUtil;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ConsumerPool implements AutoCloseable {
    private static Logger logger = Logger.getLogger(ConsumerPool.class.getName());

    private final ExecutorService executorService;
    private final int numConsumers;
    private final List<? extends Processor> processors;
    private final List<String> topics;
    private final Properties consumerProperties;

    private List<Consumer> consumers;

    public ConsumerPool(int numConsumers, List<? extends Processor> processors, List<String> topics, Properties consumerProperties) {
        this.numConsumers = numConsumers;
        this.processors = processors;
        this.topics = topics;
        this.consumerProperties = consumerProperties;

        ThreadFactory threadFactory = ThreadUtil.getThreadFactoryCollection("consumer", false);
        this.executorService = Executors.newFixedThreadPool(numConsumers, threadFactory);
    }

    public void start() {
        logger.info("Start consumers...");
        for (int i = 0; i < numConsumers; i++) {
            Consumer consumer = new Consumer(
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
