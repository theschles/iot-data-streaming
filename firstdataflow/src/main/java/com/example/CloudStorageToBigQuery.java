package com.example;

//PIPELINE IMPORTS
import com.example.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.Pipeline;

//INPUT IMPORTS
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;

//TRANSFORMS IMPORTS
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

//METRICS AND COUNTERS IMPORTS
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

//LOCAL IMPORTS
import com.example.common.ExampleUtils;

//LOGGING IMPORTS
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.joda.time.Instant;

import java.awt.*;

public class CloudStorageToBigQuery
{
    private static final Logger LOG = LoggerFactory.getLogger(CloudStorageToBigQuery.class);

    //Create a custom Pipeline options class
    public interface MyPipelineOptions extends PipelineOptions
    {
        @Description("This is the path to input file")
        @Validation.Required
        String getInput();
        void setInput(String value);

        @Description("This is the path to the output file")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Description("This is the size of streaming windows in Minutes")
        @Validation.Required
        int getWindowSize();
        void setWindowSize(int value);

        @Description("This is the number of shards")
        @Validation.Required
        int getNumberOfShards();
        void setNumberOfShards(int value);
    }

    public static class SplitWords extends DoFn<String, String> {

        private final Counter numberOfWords = Metrics.counter(SplitWords.class, "NumberOfWords");

        @ProcessElement
        public void processElement(ProcessContext c) {
            LOG.debug("Started processing input");
            // Split the line into words.
            String[] words = c.element().split(ExampleUtils.TOKENIZER_PATTERN);

            LOG.debug("Completed splitting lines. Starting to check the words.");
            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    numberOfWords.inc();
                    c.outputWithTimestamp(word, new Instant());
                }
            }
            LOG.debug("Completed sending valid words.");
        }
    }

    public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            LOG.debug("Starting step 1:");
            PCollection<String> words = lines.apply(
                    ParDo.of(new SplitWords())
            );

            LOG.debug("Starting step 2");
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.<String>perElement());

            LOG.debug("Completed");
            return wordCounts;
        }
    }

    public static class FormatForOutput extends SimpleFunction<KV<String, Long>, String>{
        @Override
        public String apply(KV<String, Long> input)
        {
            return input.getKey() + ": " + input.getValue() + ",";
        }

    }

    public static void main(String[] args)
    {
        //Start by using PipelineOptions extended class
        MyPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyPipelineOptions.class);

        //Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        //Read from input
        PCollection<String> input = pipeline
                .apply(TextIO.read().from(options.getInput()))
                .apply(ParDo.of(new SplitWords()));

        PCollection<String> windowedWords =
                input.apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

        PCollection<KV<String, Long>> wordCounts = windowedWords.apply(Count.<String>perElement());

        wordCounts.apply(MapElements.via(new FormatForOutput()))
                .apply(new WriteOneFilePerWindow(options.getOutput(), options.getNumberOfShards()));

        //Run the pipeline
        pipeline.run().waitUntilFinish();

    }

}
