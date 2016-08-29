package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import redis.clients.jedis.Jedis;

import javax.inject.Inject;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URL;
import java.util.*;

public class AnalyzerThread extends Thread {
    private boolean shutdown;

    private Jedis jedis;
    public AnalyzerThread(Jedis jedis) {
        this.setName("thread1");
        this.jedis = jedis;
    }

    public void shutdown() {
        synchronized (this) {
            this.notify();
            this.shutdown = true;
        }
    }

    public void pollKafka(KafkaConsumer consumer, Set<String> ignoreWords) throws IOException {
        ConsumerRecords<String, String> records = consumer.poll(100);

        for (ConsumerRecord<String, String> record : records) {
            String url = record.value();
            System.out.println("Analyzing url: " + url);
            Connection connection = Jsoup.connect(url).timeout(10000);
            Connection.Response resp = null;
            int retries = 500;
            while (resp == null && retries > 0) {
                try {
                    resp = connection.execute();
                } catch (HttpStatusException e) {
                    System.out.println("retry number: " + (500 - retries--));
                }
            }

            Document doc = null;

            if (resp != null && resp.statusCode() == 200) {
                doc = connection.get();
            }

            if (doc == null) {
                throw new RuntimeException("Unable to get doc: " + resp);
            }
            String sentence;
            try {
                sentence = doc.getElementsByClass("productDescriptionWrapper").first().toString().split("\n")[1];
            } catch (Exception e) {
                throw new RuntimeException("Badly formatted doc for URL: "+ url);
            }
            for(String word: sentence.split(" ")) {
                if (word.length() == 0)
                    continue;
                String insertWord;
                try {
                    insertWord = word.toLowerCase();
                } catch (Exception e) {
                    insertWord = word;
                }
                if (!ignoreWords.contains(insertWord)) {
                    if (jedis.exists(insertWord))
                        jedis.incr(insertWord);
                    else {
                        jedis.set(insertWord, "1");
                    }
                }
            }
        }
    }
    public void run() {
        boolean shutdown = false;
        KafkaConsumer<String, String> consumer;
        try (InputStream props = Resources.getResource("consumer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
            }
            consumer = new KafkaConsumer<>(properties);
        } catch (Exception e) {
            System.out.println("setup failed");
            consumer = null;
        }

        if (consumer != null) {
            consumer.subscribe(Collections.singletonList("url_links"));
        }
        DocumentBuilderFactory factory =
                DocumentBuilderFactory.newInstance();

        while (!shutdown) {
            try {
                Set<String> ignoreWords = new HashSet<>();
                // http://nlp.stanford.edu/IR-book/html/htmledition/dropping-common-terms-stop-words-1.html
                ignoreWords.add("the");
                ignoreWords.add("a");
                ignoreWords.add("an");
                ignoreWords.add("and");
                ignoreWords.add("are");
                ignoreWords.add("as");
                ignoreWords.add("at");
                ignoreWords.add("be");
                ignoreWords.add("by");
                ignoreWords.add("for");
                ignoreWords.add("from");
                ignoreWords.add("has");
                ignoreWords.add("he");
                ignoreWords.add("in");
                ignoreWords.add("is");
                ignoreWords.add("it");
                ignoreWords.add("its");
                ignoreWords.add("of");
                ignoreWords.add("on");
                ignoreWords.add("that");
                ignoreWords.add("the");
                ignoreWords.add("to");
                ignoreWords.add("was");
                ignoreWords.add("were");
                ignoreWords.add("will");
                ignoreWords.add("with");
                pollKafka(consumer, ignoreWords);
            } catch (Exception e) {
                System.out.println(e);
            }

            try {
                synchronized (this) {
                    wait(10000);
                    shutdown = this.shutdown;
                }
            } catch (InterruptedException e) {
                System.out.println("thread interrupted.");
            }
        }
        consumer.close();
    }
}