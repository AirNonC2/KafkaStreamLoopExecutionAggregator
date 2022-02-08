package com.example.kafkastreamloopexecutionaggregator;

import com.example.kafkastreamloopexecutionaggregator.pojo.Result;
import com.example.kafkastreamloopexecutionaggregator.pojo.ResultReport;
import com.example.kafkastreamloopexecutionaggregator.pojo.ResultTimeout;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.*;


import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProducerTest{

    static final String outputTopic = "Result";
    static final String bootstrapServers = "localhost:9092";

    private final Producer<String, Result> producer;
    final String outTopic;

    public ProducerTest(final Producer<String, Result> producer,
                                    final String topic) {
        this.producer = producer;
        outTopic = topic;
    }

    public Future<RecordMetadata> produce(final Result message, final String key) {

        final ProducerRecord<String, Result> producerRecord = new ProducerRecord<>(outTopic, key, message);
        producerRecord.headers().add("type",message.getClass().getName().getBytes());
        return producer.send(producerRecord);
    }

    public void shutdown() {
        producer.close();
    }


    public static void main(String[] args) throws Exception {

        final Properties producerConf = getProducerConfiguration(bootstrapServers, args[0]);

        final Producer<String, Result> producer = new KafkaProducer<>(producerConf);
        final ProducerTest producerApp = new ProducerTest(producer, outputTopic);

        Random rand = new Random();

        List<Integer> executions = IntStream.range(0,5).mapToObj((i) -> rand.nextInt(9)+1).collect(Collectors.toList());

        int j = 1;

        for (int exec :
                executions) {

            boolean timeout = j % 7 == 0;

            String executionId = args[0] + String.valueOf(j);

            for (int i = 0; i < exec; i++) {

                ResultReport resultReport = new ResultReport();
                resultReport.setSeqNum(i+1);
                resultReport.setPayload("payloaddata " + (i+1));
                resultReport.setUpperBound(exec);
                resultReport.setNextStepId("nextStep"+executionId);
                producerApp.produce(resultReport,executionId);

                if(timeout && i == exec / 2){
                    ResultTimeout resultTimeout = new ResultTimeout();
                    resultTimeout.setPayload("timoutpayloaddata " + (i+1));
                    resultTimeout.setNextStepId("nextStep"+executionId);
                    producerApp.produce(resultTimeout,executionId);
                }
            }

            j++;

        }


            producerApp.shutdown();

    }
    static Properties getProducerConfiguration(final String bootstrapServers, String num) {

        final Properties producerConfiguration = new Properties();

        producerConfiguration.put(ProducerConfig.CLIENT_ID_CONFIG, "client" + num);
        producerConfiguration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return producerConfiguration;
    }
}
