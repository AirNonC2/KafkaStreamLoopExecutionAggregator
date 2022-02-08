package com.example.kafkastreamloopexecutionaggregator.pojo;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Data;

@Data
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonTypeName("ResultReport")
public class ResultReport extends Result{

    private int seqNum;
    private int upperBound;

}
