package kafkaclientframe.app;

import kafkaclientframe.kafka.ConsumerPool;
import kafkaclientframe.kafka.config.KafkaConfig;
import kafkaclientframe.process.impl.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Start class
 *
 * @author chernyakov
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        List<Sample> samples = new ArrayList<>();
        samples.add(new Sample());

        ConsumerPool consumerPool = new ConsumerPool<>(
                KafkaConfig.getNumConsumers(),
                samples,
                KafkaConfig.getTopicsTasks(),
                KafkaConfig.getKafkaConsumerProperties()
        );

        consumerPool.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                consumerPool.close();
            } catch (Exception e) {
                logger.error("Error while closing consumer pool", e);
            }
        }));
    }
}
