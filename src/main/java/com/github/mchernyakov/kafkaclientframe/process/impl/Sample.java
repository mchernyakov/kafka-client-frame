package com.github.mchernyakov.kafkaclientframe.process.impl;


import com.github.mchernyakov.kafkaclientframe.process.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
