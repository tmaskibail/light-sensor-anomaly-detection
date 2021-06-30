
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
package com.tmaskibail.beam.demo.util;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class Util {
    private static final String STRING = "STRING";

    private Util() {
        throw new IllegalStateException("Utility class");
    }

    public static TableSchema tableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("deviceId").setType(STRING));
        fields.add(new TableFieldSchema().setName("deviceNumId").setType(STRING));
        fields.add(new TableFieldSchema().setName("deviceRegistryId").setType(STRING));
        fields.add(new TableFieldSchema().setName("deviceRegistryLocation").setType(STRING));
        fields.add(new TableFieldSchema().setName("projectId").setType(STRING));
        fields.add(new TableFieldSchema().setName("subFolder").setType(STRING));
        fields.add(new TableFieldSchema().setName("ts").setType(STRING));
        fields.add(new TableFieldSchema().setName("temperature").setType("NUMERIC"));
        fields.add(new TableFieldSchema().setName("pressure").setType("NUMERIC"));
        fields.add(new TableFieldSchema().setName("humidity").setType("NUMERIC"));
        fields.add(new TableFieldSchema().setName("ambientLight").setType("NUMERIC"));
        return new TableSchema().setFields(fields);
    }
}
