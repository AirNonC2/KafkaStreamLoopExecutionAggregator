package com.example.kafkastreamloopexecutionaggregator;

import com.example.kafkastreamloopexecutionaggregator.pojo.AggregatedResult;
import com.example.kafkastreamloopexecutionaggregator.pojo.Result;
import com.example.kafkastreamloopexecutionaggregator.pojo.ResultReport;
import com.example.kafkastreamloopexecutionaggregator.pojo.ResultTimeout;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class KafkaStreamLoopExecutionAggregatorApplication {


    static final String inputTopic = "Result";
    static final String outputTopic = "AggregatedResult";
    static final String bootstrapServers = "localhost:9092";

    public static void main(String[] args) {

        final Properties streamsConfiguration = getStreamsConfiguration(bootstrapServers, args[0]);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Result> stepResultsStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), new JsonSerde<Result>(Result.class)));

        final KTable<String, AggregatedResult> executionStates = stepResultsStream.groupByKey()
                .aggregate(
                        AggregatedResult::new,
                        KafkaStreamLoopExecutionAggregatorApplication::aggregateResultEvent,
                        Materialized.<String, AggregatedResult, KeyValueStore<Bytes, byte[]>>
                                        as("aggregated-execution-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(AggregatedResult.class)));

        stepResultsStream.print(Printed.<String, Result>toSysOut());

        executionStates.filter((key, aggregatedResult) ->

                aggregatedResult.getExecutionStatus() == AggregatedResult.Status.SUCCESSFUL ||
                        aggregatedResult.getExecutionStatus() == AggregatedResult.Status.TIMEOUT
        ).toStream().to(outputTopic, Produced.with(Serdes.String(), new org.springframework.kafka.support.serializer.JsonSerde<AggregatedResult>(AggregatedResult.class)));


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    static Properties getStreamsConfiguration(final String bootstrapServers, String ins) {

        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG,"loop-execution-aggregator2");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG,  "instance" + ins);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, tempDirectory(ins).getAbsolutePath());

        return streamsConfiguration;
    }

    public static AggregatedResult aggregateResultEvent(String key, Result event, AggregatedResult previousResult) {

        System.out.printf("new event comes with id of %s\n", key);

        if (previousResult.getExecutionStatus() == AggregatedResult.Status.CONTINUE) {
            if (event instanceof ResultTimeout) {

                System.out.printf("timeout comes with id of %s \n", key);


                previousResult.enqueuePayload(event.getPayload());
                previousResult.setNextStepId(event.getNextStepId());

                return previousResult.timeOutExecution(event);


            } else {

                ResultReport resultReport = (ResultReport) event;

                System.out.printf("report comes with id of %s and step id of %d \n", key, resultReport.getSeqNum());


                int currentStep = previousResult.incrementCounter();

                previousResult.enqueuePayload(event.getPayload());

                if (currentStep == resultReport.getUpperBound()) {

                    previousResult.setNextStepId(event.getNextStepId());

                    return previousResult.completeExecution(event);
                }
            }
        } else {
            return previousResult.alreadyFinalized(event);
        }
        return previousResult;
    }

    public static File tempDirectory(String inp) {
        final File file;
        try {
            file = Files.createTempDirectory(inp + " temp").toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Utils.delete(file);
                } catch (IOException e) {
                    System.out.println("Error deleting " + file.getAbsolutePath());
                }
            }
        });

        return file;
    }

}
