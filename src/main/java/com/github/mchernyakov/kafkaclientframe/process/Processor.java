package com.github.mchernyakov.kafkaclientframe.process;


public interface Processor<K,V> {

    void process(K key, V value);
}
