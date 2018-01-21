package kafkatemplate.process.impl;


import kafkatemplate.process.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by chernyakov on 07.02.17.
 */
public class Sample implements Processor<String, String> {

    private static Logger log = LoggerFactory.getLogger(Sample.class.getName());

    private String message = "Hello world";

    public Sample(String message) {
        this.message = message;
    }

    public Sample() {
    }

    @Override
    public void process(String key, String value) {
        log.info("Receive message: " + key + " " + value);
    }
}
