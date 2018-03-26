package com.example;

//LOCAL IMPORTS
import com.example.common.ExampleUtils;

//PIPELINE IMPORTS
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

//INPUT IMPORTS
import org.apache.beam.sdk.io.TextIO;

//LOGGING IMPORTS
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TRANSFORMS IMPORTS
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.*;

//METRICS AND COUNTERS IMPORTS
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

public class MyWordCountBestPractice
{
    private static final Logger LOG = LoggerFactory.getLogger(MyWordCountBestPractice.class);

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
                    c.output(word);
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
        pipeline.apply(TextIO.read().from(options.getInput()))

        //Transform
        .apply(new CountWords())

         //For output
        .apply(MapElements.via(new FormatForOutput()))

        //Write to output
        .apply(TextIO.write().to(options.getOutput()));

        //Run the pipeline
        pipeline.run().waitUntilFinish();

    }

}
