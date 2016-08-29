package com.example;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import redis.clients.jedis.Jedis;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyBinder extends AbstractBinder {

    private Jedis jedis;

    public MyBinder(Jedis jedis) {
        this.jedis = jedis;
    }
    @Override
    protected void configure() {
        bind(KafkaProducer.class).in(Singleton.class);
        bind(setupKafka()).to(KafkaProducer.class);
        bind(Jedis.class).in(Singleton.class);
        bind(jedis).to(Jedis.class);
    }

    private static KafkaProducer setupKafka() {
        try {
            KafkaProducer<String, String> producer;
            try (InputStream props = Resources.getResource("producer.props").openStream()) {
                Properties properties = new Properties();
                properties.load(props);
                producer = new KafkaProducer<>(properties);
            }

            return producer;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
