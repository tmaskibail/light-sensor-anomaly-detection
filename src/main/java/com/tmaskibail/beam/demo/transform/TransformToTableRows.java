package com.tmaskibail.beam.demo.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.tmaskibail.beam.demo.model.SensorData;
import org.apache.beam.sdk.transforms.DoFn;

public class TransformToTableRows extends DoFn<SensorData, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row =
                new TableRow()
                        .set("deviceId", c.element().getDeviceId())
                        .set("deviceNumId", c.element().getDeviceNumId())
                        .set("deviceRegistryId", c.element().getDeviceRegistryId())
                        .set("deviceRegistryLocation", c.element().getDeviceRegistryLocation())
                        .set("projectId", c.element().getProjectId())
                        .set("subFolder", c.element().getSubFolder())
                        .set("ts", c.element().getTs())
                        .set("temperature", c.element().getTemperature())
                        .set("pressure", c.element().getPressure())
                        .set("humidity", c.element().getHumidity())
                        .set("ambientLight", c.element().getAmbientLight());
        c.output(row);
    }
}
