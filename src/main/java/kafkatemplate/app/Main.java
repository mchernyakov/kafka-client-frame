package kafkatemplate.app;

import kafkatemplate.kafka.ConsumerPool;
import kafkatemplate.kafka.config.KafkaConfig;
import kafkatemplate.process.impl.Sample;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Start class
 *
 * @author chernyakov
 */
public class Main {

    private static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        List<Sample> samples = new ArrayList<>();
        samples.add(new Sample());

        ConsumerPool consumerPool = new ConsumerPool(
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
