package kafkatemplate.process;

/**
 * Created by chernyakov on 07.02.17.
 */
public interface Processor<K,V> {

    void process(K key, V value);
}
