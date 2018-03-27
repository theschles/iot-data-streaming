package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;

import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.java2d.pipe.SpanShapeRenderer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PubSubToBigQuery {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    public static interface MyOptions extends DataflowPipelineOptions {
        @Description("This is the input PubSub")
        @Validation.Required
        @Default.String("projects/ocgcp-iot-core/subscriptions/telemetry-dataflow")
        String getInputPubSub();
        void setInputPubSub(String value);

        @Description("This is output data set to use with BigQuery")
        @Validation.Required
        @Default.String("ocgcp-iot-core:shake_data.raw_telemetry")
        String getOutputBigQuery();
        void setOutputBigQuery(String value);

        @Description("This is the window size")
        @Validation.Required
        @Default.Integer(2)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("This is output OS data set")
        @Validation.Required
        @Default.String("ocgcp-iot-core:shake_data.breakdown_os")
        String getOutputOsCount();
        void setOutputOsCount(String value);

    }

    public static class IotPosition implements Serializable {
        public Double latitude;
        public Double longitude;
    }

    public static class IotAcceleration implements Serializable {
        public Double x;
        public Double y;
        public Double z;
    }

    public static class IotData implements Serializable {
        public Long ts;
        public String ua;
        public IotPosition position;
        public IotAcceleration acceleration;
        public String deviceId;
    }

    /*
    public static class IotDataCustom implements Serializable {
        public String ts_bound;
        public String os;
        public String browser;
    }
    */

    public static class MessageToIotDataFormatter extends DoFn<PubsubMessage, IotData> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String input = new String(c.element().getPayload());
            IotData output = new Gson().fromJson(input, IotData.class);
            c.output(output);
            //c.outputWithTimestamp(output, new Instant(output.ts));
        }
    }

    public static class IotToTableFormatter extends SimpleFunction<IotData, TableRow> {
        private final static DateTimeFormatter ISO_8601 = ISODateTimeFormat.dateTime();

        @Override
        public TableRow apply(IotData input) {
            LOG.debug("Inside IotToTableFormatter:apply");
            TableRow tr = new TableRow();
            if (input != null) {
                tr.set("ts", ISO_8601.print(new DateTime(input.ts)));
                if (input.ua != null) {
                    tr.set("ua", input.ua);
                }
                if (input.position != null) {
                    tr.set("latitude", input.position.latitude);
                    tr.set("longitude", input.position.longitude);
                }
                if (input.acceleration != null) {
                    tr.set("accelerationX", input.acceleration.x);
                    tr.set("accelerationY", input.acceleration.y);
                    tr.set("accelerationZ", input.acceleration.z);
                }
                tr.set("deviceId", input.deviceId);
            }
            return tr;
        }

        private static TableSchema RawSchema() {
            LOG.debug("Inside RawSchema");
            List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
            fields.add(new TableFieldSchema().setName("ts").setType("TIMESTAMP").setMode("REQUIRED"));
            fields.add(new TableFieldSchema().setName("ua").setType("STRING"));
            fields.add(new TableFieldSchema().setName("latitude").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("longitude").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationX").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationY").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationZ").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("deviceId").setType("STRING").setMode("REQUIRED"));
            return new TableSchema().setFields(fields);
        }
    }

    public static class ExtractOS extends DoFn<IotData, String> {
        public static UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(parser.parse(c.element().ua).getOperatingSystem().getName());
        }
    }

    public static class CountOS extends PTransform<PCollection<IotData>, PCollection<KV<String, Long>>> {

        public static UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<IotData> input) {
            LOG.debug("Inside CountOS:expand");

            PubSubToBigQuery.timeNow = new Instant();
            PubSubToBigQuery.batch += 1;

            PCollection<String> osList = input.apply(ParDo.of(new ExtractOS()));

            return osList.apply(Count.<String>perElement());
        }
    }

    private static Instant timeNow = new Instant();
    private static Integer batch = 0;

    public static class OSCountsToTable extends SimpleFunction<KV<String, Long>, TableRow> {
        private final static DateTimeFormatter ISO_8601 = ISODateTimeFormat.dateTime();


        @Override
        public TableRow apply(KV<String, Long> input) {
            LOG.debug("Inside OSCountsToTable:apply");
            TableRow tr = new TableRow();
            if (input != null) {
                tr.set("ts_bound", ISO_8601.print(new DateTime(PubSubToBigQuery.timeNow)));
                tr.set("os", input.getKey());
                tr.set("count", input.getValue());
                tr.set("interval", PubSubToBigQuery.batch.toString());
            }
            return tr;
        }

        private static TableSchema RawSchema() {
            LOG.debug("Inside RawSchema");
            List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
            fields.add(new TableFieldSchema().setName("ts_bound").setType("TIMESTAMP"));
            fields.add(new TableFieldSchema().setName("os").setType("STRING"));
            fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("interval").setType("STRING"));
            return new TableSchema().setFields(fields);
        }
    }

    public static void main(String[] args) {
        LOG.debug("Inside main");
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline input = Pipeline.create(options);


        input.apply(PubsubIO.readMessages().fromSubscription(options.getInputPubSub()))
                .apply(ParDo.of(new MessageToIotDataFormatter()))
                .apply(MapElements.via(new IotToTableFormatter()))
                .apply(BigQueryIO.writeTableRows().to(options.getOutputBigQuery())
                        .withSchema(IotToTableFormatter.RawSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );

        PCollection<IotData> inputMessages = input.apply(PubsubIO.readMessages().fromSubscription(options.getInputPubSub()))
                .apply(ParDo.of(new MessageToIotDataFormatter()));

        /*
        inputMessages.apply(MapElements.via(new IotToTableFormatter()))
                .apply(BigQueryIO.writeTableRows().to(options.getOutputBigQuery())
                        .withSchema(IotToTableFormatter.RawSchema())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );
                */

        PCollection<IotData> windowedInput = inputMessages.apply(Window.<IotData>into(
                FixedWindows.of(Duration.standardSeconds(options.getWindowSize())))
        );

        windowedInput.apply(new CountOS())
                .apply(ParDo.of(new DoFn<KV<String, Long>, KV<String, Long>> () {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        // Extract the timestamp from log entry we're currently processing.
                        Instant input = new Instant();
                        // Use outputWithTimestamp to emit the log entry with timestamp attached.
                        c.outputWithTimestamp(c.element(), input);
                    }
                }))
                .apply(MapElements.via(new OSCountsToTable()))
                .apply(BigQueryIO.writeTableRows().to(options.getOutputOsCount())
                .withSchema(OSCountsToTable.RawSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );


        input.run().waitUntilFinish();
    }
}
