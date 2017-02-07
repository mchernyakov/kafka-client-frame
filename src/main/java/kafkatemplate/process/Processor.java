package kafkatemplate.process;

/**
 * Created by chernyakov on 07.02.17.
 */
public interface Processor {

    void process(String data);
}
