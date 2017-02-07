package kafkatemplate.process.impl;

import org.apache.log4j.Logger;
import kafkatemplate.process.Processor;

/**
 * Created by chernyakov on 07.02.17.
 */
public class Sample implements Processor {

    private static Logger log = Logger.getLogger(Sample.class.getName());

    private String message = "Hello world";

    public Sample(String message) {
        this.message = message;
    }

    public Sample() {
    }

    @Override
    public void process(String data) {
        log.info(data);
    }
}
