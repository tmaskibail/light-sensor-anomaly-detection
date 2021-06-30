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
