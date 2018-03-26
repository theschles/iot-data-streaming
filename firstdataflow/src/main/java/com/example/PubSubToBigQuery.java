package com.example;

import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;

import org.apache.beam.sdk.io.TextIO;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;

import java.io.Serializable;

public class PubSubToBigQuery {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    public static interface MyOptions extends PipelineOptions {
        @Description("This is the input PubSub")
        @Validation.Required
        String getInputPubSub();
        void setInputPubSub(String value);

        @Description("This is output Big Query")
        @Validation.Required
        String getOutputBigQuery();
        void setOutputBigQuery(String value);

    }

    public static class IotPosition implements Serializable {
        public double latitude;
        public double longitude;
    }

    public static class IotAcceleration implements Serializable {
        public double x;
        public double y;
        public double z;
    }

    public static class IotData implements Serializable {
        public DateTime ts;
        public String ua;
        public IotPosition position;
        public IotAcceleration acceleration;
    }

    public static class MessageToIotDataFormatter extends DoFn<PubsubMessage, IotData> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            byte[] inputBytes = c.element().getPayload();
            String s = StringUtf8Coder.of().
            String inputString = StringUtf8Coder.of().decode(new ByteInputStream(inputBytes))
            StringUtf8Coder.of().decode(c.element());
            IotData o = new Gson()..fromJson(c.element(), IotData.class);
            IotData output = new IotData();
            output.ts = new DateTime();
            output.ua = "Mozilla/5.0 (iPhone; CPU OS 11_2_1 like Mac OS X) AppleWebKit/604.4.7 (KHTML, like Gecko) Version/11.0 Mobile/15C153 Safari/604.1";
            output.position = new IotPosition();
            output.position.latitude = 33.65222211122211;
            output.position.longitude = -117.7422211122211;
            output.acceleration = new IotAcceleration();
            output.acceleration.x = 0.012711111111111111;
            output.acceleration.y = 0.011511111111111111;
            output.acceleration.z = 0.152711111111111111;
            c.output(output);
        }
    }

    public static class StringToIotFormatter extends SimpleFunction<String, IotData> {
        @Override
        public IotData apply(String input) {
            LOG.debug("Inside StringToIotFormatter:apply");
            IotData output = new IotData();
            output.ts = new DateTime();
            output.ua = "Mozilla";
            output.position = new IotPosition();
            output.position.latitude = 33.65222211122211;
            output.position.longitude = -117.7422211122211;
            output.acceleration = new IotAcceleration();
            output.acceleration.x = 0.012711111111111111;
            output.acceleration.y = 0.011511111111111111;
            output.acceleration.z = 0.152711111111111111;
            return output;
        }
    }

    public static class IotToTableFormatter extends SimpleFunction<IotData, TableRow> {
        @Override
        public TableRow apply(IotData input) {
            LOG.debug("Inside IotToTableFormatter:apply");
            TableRow tr = new TableRow();
            if (input != null) {
                tr.set("inputTime", input.ts.toString());
                tr.set("userAgent", input.ua);
                tr.set("latitude", input.position.latitude);
                tr.set("longitude", input.position.longitude);
                tr.set("accelerationX", input.acceleration.x);
                tr.set("accelerationY", input.acceleration.y);
                tr.set("accelerationZ", input.acceleration.z);
            }
            return tr;
        }

        private static TableSchema schemaGen() {
            LOG.debug("Inside schemaGen");
            List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
            fields.add(new TableFieldSchema().setName("inputTime").setType("STRING"));
            fields.add(new TableFieldSchema().setName("userAgent").setType("STRING"));
            fields.add(new TableFieldSchema().setName("latitude").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("longitude").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationX").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationY").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("accelerationZ").setType("FLOAT"));
            TableSchema schema = new TableSchema().setFields(fields);
            return schema;
        }
    }

    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline input = Pipeline.create(options);

        input.apply(PubsubIO.readMessages().fromSubscription(options.getInputPubSub()))
                .apply(ParDo.of(new MessageToIotDataFormatter()))
                .apply(MapElements.via(new IotToTableFormatter()))
                .apply(BigQueryIO.writeTableRows().to(options.getOutputBigQuery())
                        .withSchema(IotToTableFormatter.schemaGen())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );
        /*input.apply(TextIO.read().from("gs://words-an/testdata.txt"))
                .apply(MapElements.via(new StringToIotFormatter()))
                .apply(MapElements.via(new IotToTableFormatter()))
                .apply(BigQueryIO.writeTableRows().to(options.getOutputBigQuery())
                        .withSchema(IotToTableFormatter.schemaGen())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );*/

        input.run().waitUntilFinish();
    }


}
