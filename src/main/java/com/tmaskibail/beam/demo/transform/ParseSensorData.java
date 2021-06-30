
/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.tmaskibail.beam.demo.transform;

import com.tmaskibail.beam.demo.model.SensorData;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ParseSensorData extends DoFn<PubsubMessage, SensorData> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseSensorData.class);

    private static SensorData extractSensorData(PubsubMessage element) {
        String payload = new String(element.getPayload(), StandardCharsets.UTF_8);

        SensorData sensorData = new SensorData();
        JSONArray jsonArray = new JSONObject(payload).getJSONArray("enviro");
        for (int i = 0; i < jsonArray.length(); i++) {
            sensorData.setTs(jsonArray.getJSONObject(i).getString("ts"));
            sensorData.setTemperature(jsonArray.getJSONObject(i).getDouble("temperature"));
            sensorData.setPressure(jsonArray.getJSONObject(i).getDouble("pressure"));
            sensorData.setHumidity(jsonArray.getJSONObject(i).getDouble("humidity"));
            sensorData.setAmbientLight(jsonArray.getJSONObject(i).getDouble("ambient_light"));
        }

        sensorData.setDeviceId(element.getAttribute("deviceId"));
        sensorData.setDeviceNumId(element.getAttribute("deviceNumId"));
        sensorData.setDeviceRegistryId(element.getAttribute("deviceRegistryId"));
        sensorData.setDeviceRegistryLocation(element.getAttribute("deviceRegistryLocation"));
        sensorData.setProjectId(element.getAttribute("projectId"));
        sensorData.setSubFolder(element.getAttribute("subFolder"));

        LOG.debug("Extracted sensorData : {}", sensorData);
        return sensorData;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        c.output(extractSensorData(Objects.requireNonNull(c.element())));
    }
}
