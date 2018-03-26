package com.example;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;

import com.example.common.ExampleUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;


public class MyWordCountSimpliest
{

    public static void main(String[] args)
    {
        //Start by creating a Pipeline options object
        //This will take options from the command line
        PipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create();

        //Create teh pipeline
        Pipeline pipeline = Pipeline.create(options);


        //Read from input
        pipeline.apply(TextIO.read().from("gs://words-an/test.txt"))

                //Transform
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(ExampleUtils.TOKENIZER_PATTERN)){
                            if (!word.isEmpty())
                                c.output(word);
                        }
                    }
                }))

                .apply(Count.<String>perElement())

                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>(){
                            @Override
                            public String apply(KV<String, Long> input) {
                                return input.getKey() + ":" + input.getValue();
                            }
                        })
                )

                //Write to output
                .apply(TextIO.write().to("gs://words-an/words.txt"));

        //Run the pipeline
        pipeline.run().waitUntilFinish();

    }

}
