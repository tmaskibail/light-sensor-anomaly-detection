
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
