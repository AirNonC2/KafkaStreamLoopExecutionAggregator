package com.example.kafkastreamloopexecutionaggregator.pojo;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import org.apache.kafka.common.protocol.types.Field;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ResultReport.class, name = "ResultReport"),
        @JsonSubTypes.Type(value = ResultTimeout.class, name = "ResultTimeout"),
        @JsonSubTypes.Type(value = ResultTimeout.class, name = "Aggregated")
})

@Data
public abstract class Result {

    private String nextStepId;
    private Object payload;


}
