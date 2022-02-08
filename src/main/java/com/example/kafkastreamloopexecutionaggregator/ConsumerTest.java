package com.example.kafkastreamloopexecutionaggregator;

import com.example.kafkastreamloopexecutionaggregator.pojo.AggregatedResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.kafka.support.serializer.JsonDeserializer;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ConsumerTest {

    static final String outputTopic = "AggregatedResult";
    static final String bootstrapServers = "localhost:9092";

    public static void main(String[] args) throws IOException {
        File outfile = new File("output"+args[0]);

        BasicConsumeLoop consumer = new BasicConsumeLoop(getConsumerConfiguration(bootstrapServers, args[0]), outputTopic, outfile);

        consumer.run();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }

    public static class BasicConsumeLoop implements Runnable {
        private final KafkaConsumer<String, AggregatedResult> consumer;
        private final String topic;
        private final AtomicBoolean shutdown;
        private final CountDownLatch shutdownLatch;
        private final PrintWriter writer;

        public BasicConsumeLoop(Properties config, String topic,File outputfile) throws IOException {
            this.consumer = new KafkaConsumer<>(config,new StringDeserializer(),new JsonDeserializer<AggregatedResult>(AggregatedResult.class,false));
            this.topic = topic;
            this.shutdown = new AtomicBoolean(false);
            this.shutdownLatch = new CountDownLatch(1);

            this.writer = new PrintWriter(new FileWriter(outputfile));
        }

        public void process(ConsumerRecord<String, AggregatedResult> record) {

            System.out.printf("Aggregated result is arrived with id %s \n", record.key());
            System.out.printf("Status : %s \n", record.value().getExecutionStatus());
            System.out.printf("payload : %s\n", record.value().getPayloads().stream().map(Object::toString).collect(Collectors.joining(", ")));
            System.out.printf("------------------------\n");
        }

        public void run() {
            try {
                //TopicPartition partition2 = new TopicPartition(topic, 1);
               // consumer.assign(Collections.singletonList(partition2));
                consumer.subscribe(Collections.singletonList(topic));

                while (!shutdown.get()) {
                    ConsumerRecords<String, AggregatedResult> records = consumer.poll(1000);
                    records.forEach(this::process);
                }
            } finally {
                consumer.close();
                writer.close();
                shutdownLatch.countDown();
            }
        }

        public void shutdown(){
            shutdown.set(true);
            try {
                shutdownLatch.await();
            }catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }


    static Properties getConsumerConfiguration(final String bootstrapServers, String num) {

        final Properties consumerConfig = new Properties();

        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "instance" + num);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumers");
        consumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        consumerConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest" );



        return consumerConfig;
    }
}
