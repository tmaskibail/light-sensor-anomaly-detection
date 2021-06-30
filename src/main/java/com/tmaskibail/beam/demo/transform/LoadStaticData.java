
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

import com.google.cloud.bigquery.*;
import com.tmaskibail.beam.demo.App;
import org.apache.beam.sdk.transforms.DoFn;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class LoadStaticData extends DoFn<Long, Map<String, BigDecimal>> {

    public static final String LOAD_CRITERIA_QUERY = "SELECT upper_bound, lower_bound FROM `" + App.PROJECT_NAME + ".tm_sandbox_eu.static_data`";

    @ProcessElement
    public void process(@Element Long input, OutputReceiver<Map<String, BigDecimal>> outputReceiver) throws InterruptedException {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                        LOAD_CRITERIA_QUERY)
                        .setUseLegacySql(false) // Use standard SQL syntax for queries.
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        TableResult result = queryJob.getQueryResults();
        Map<String, BigDecimal> map = new HashMap<>();
        for (FieldValueList row : result.iterateAll()) {
            map.put("LOWER_BOUND", row.get("lower_bound").getNumericValue());
            map.put("UPPER_BOUND", row.get("upper_bound").getNumericValue());
        }

        outputReceiver.output(map);
    }
}
