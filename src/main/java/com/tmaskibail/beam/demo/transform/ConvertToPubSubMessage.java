package com.tmaskibail.beam.demo.transform;


import com.google.gson.Gson;
import com.tmaskibail.beam.demo.model.SensorData;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class ConvertToPubSubMessage extends DoFn<SensorData, PubsubMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertToPubSubMessage.class);

    @ProcessElement
    public void processElement(ProcessContext c) {
        SensorData sensorData = c.element();
        c.output(new PubsubMessage(
                new Gson().toJson(sensorData).getBytes(StandardCharsets.UTF_8),
                sensorData.getAttributes()));
    }
}
