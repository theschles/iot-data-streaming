package com.example;

import com.example.common.ExampleUtils;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import java.io.File;
import java.io.IOException;

//TO TRY
/*
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
*/


public class ReadAndWriteAvroFile
{

    public static void main(String[] args)
    {
        //Start by creating a Pipeline options object
        //Using default options
        PipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create();
        //pipelineOptions.setRunner(DataflowRunner.class);

        //Create the pipeline
        //Nothing fancy - using default pipeline options
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        //Read the data from input file
        //pipeline.apply(TextIO.read().from("gs://avro-file-an/test.txt"))
        //    .apply(TextIO.write().to("gs://avro-file-an/test.txt.output"));

        try {
            System.console().writer().println("Reading schema file");

            Schema schema = new Schema.Parser().parse(new File("/Users/ananth/Dataflow/twitter.avro.schema.avsc"));

            System.console().writer().println("reading avro file");

            PCollection<GenericRecord> records =
                    pipeline.apply(AvroIO.readGenericRecords(schema).from("gs://avro-file-an/twitter.avro"));

            System.console().writer().println("Writing avro file");

            records.apply("WriteToAvro", AvroIO.writeGenericRecords(schema).to("gs://avro-file-an/twitter.output").withSuffix(".avro"));
        }
        catch (IOException exception)
        {
            System.console().writer().println("Exception" + exception);
        }

        //PCollection<GenericRecord> records =
        //        pipeline.apply(AvroIO.read(GenericRecord.class).from("gs://avro-file-an/twitter.avro"));

        //records.apply(AvroIO.write(GenericRecord.class).to("gs://avro-file-an/twitter.output.avro"));

        //Run the pipeline
        pipeline.run().waitUntilFinish();

    }

}
