package kafkatemplate.app;

import kafkatemplate.kafka.Consumer;
import kafkatemplate.kafka.config.KafkaConfig;
import kafkatemplate.process.impl.Sample;
import org.apache.log4j.Logger;

import javax.xml.ws.soap.Addressing;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Start class
 *
 * @author chernyakov
 */

public class Main {

    private static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        int numConsumers = KafkaConfig.getNumConsumers();
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        logger.info("Start consumers...");

        List<Consumer> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {

            List<Sample> samples = new ArrayList<>();
            samples.add(new Sample());

            Consumer consumer = new Consumer(
                    i,
                    KafkaConfig.getKafkaConsumerProperties(),
                    KafkaConfig.getTopicsTasks(),
                    samples
            );

            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumers.forEach(Consumer::shutdown);

            executor.shutdown();

            try {
                executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.warn(e.toString());
            }
        }));
    }
}
