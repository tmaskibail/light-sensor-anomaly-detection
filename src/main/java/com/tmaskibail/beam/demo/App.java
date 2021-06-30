package com.tmaskibail.beam.demo;

import com.google.api.services.bigquery.model.TableRow;
import com.tmaskibail.beam.demo.model.SensorData;
import com.tmaskibail.beam.demo.transform.*;
import com.tmaskibail.beam.demo.util.Util;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;

public class App {
    public static final String PROJECT_NAME = "DUMMY";
    public static final String OUTBOUND_PUBSUB_TOPIC = "projects/" + PROJECT_NAME + "/topics/outbound-telemetry";
    private static final String BQ_TABLE = PROJECT_NAME + ":tm_sandbox_eu.sensor_data";
    private static final String RAW_PUBSUB_SUBSCRIPTION = "projects/" + PROJECT_NAME + "/subscriptions/light-sensor-data-subscription";
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        runPipeline(options);
    }

    private static void runPipeline(PipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        // Side load criteria from BigQuery table and refresh the data every hour
        PCollectionView<Map<String, BigDecimal>> sideInputMap = getSideInputMap(pipeline);

        // Load data from PubSub Topic
        PCollection<SensorData> rawSensorData = pipeline
                .apply("Read from topic", PubsubIO.readMessages().fromSubscription(RAW_PUBSUB_SUBSCRIPTION))
                .apply("Window for 5 seconds", Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply("De-serialise to an object", ParDo.of(new ParseSensorData()));

        // Output 1 - Write to BigQuery
        rawSensorData
                .apply("Create BigQuery tableRows", ParDo.of(new TransformToTableRows()))
                .apply("Insert tableRows into BigQuery", writeToBigQuery());

        // Output 2 - Publish filtered data to PubSub
        rawSensorData
                .apply("Detect anomalies", ParDo.of(new DetectAnomaly(sideInputMap)).withSideInputs(sideInputMap))
                .apply("Format outbound message", ParDo.of(new ConvertToPubSubMessage()))
                .apply("publish message to PubSub", PubsubIO.writeMessages().to(OUTBOUND_PUBSUB_TOPIC));

        pipeline.run();
    }

    private static BigQueryIO.Write<@UnknownKeyFor @NonNull @Initialized TableRow> writeToBigQuery() {
        return BigQueryIO.writeTableRows()
                .withSchema(Util.tableSchema())
                .to(BQ_TABLE)
                .withExtendedErrorInfo()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);
    }

    private static PCollectionView<Map<String, BigDecimal>> getSideInputMap(Pipeline pipeline) {
        return pipeline.apply(GenerateSequence.from(0).withRate(1, Duration.standardHours(1L)))
                .apply(Window.<Long>into(new GlobalWindows()).triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane())).discardingFiredPanes())
                .apply(ParDo.of(new LoadStaticData()))
                .apply(View.asSingleton());
    }
}