package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        @Default.String("ocgcp-iot-core:shake_it.raw_telemetry")
        String getOutputBigQuery();
        void setOutputBigQuery(String value);

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
    }

    public static class MessageToIotDataFormatter extends DoFn<PubsubMessage, IotData> {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String input = new String(c.element().getPayload());
            IotData output = new Gson().fromJson(input, IotData.class);
            c.output(output);
        }
    }

    public static class IotToTableFormatter extends SimpleFunction<IotData, TableRow> {
        private final static DateTimeFormatter ISO_8601 = ISODateTimeFormat.dateTime();

        @Override
        public TableRow apply(IotData input) {
            LOG.debug("Inside IotToTableFormatter:apply");
            TableRow tr = new TableRow();
            if (input != null) {
                tr.set("inputTime", ISO_8601.print(new DateTime(input.ts)));
                if (input.ua != null) {
                    tr.set("userAgent", input.ua);
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
            }
            return tr;
        }

        private static TableSchema RawSchema() {
            LOG.debug("Inside RawSchema");
            List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
            fields.add(new TableFieldSchema().setName("inputTime").setType("TIMESTAMP"));
            fields.add(new TableFieldSchema().setName("userAgent").setType("STRING"));
            fields.add(new TableFieldSchema().setName("latitude").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("longitude").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationX").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationY").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationZ").setType("FLOAT"));
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

        input.run().waitUntilFinish();
    }
}
