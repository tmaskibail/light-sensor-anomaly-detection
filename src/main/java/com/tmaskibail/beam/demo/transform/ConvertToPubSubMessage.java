
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
