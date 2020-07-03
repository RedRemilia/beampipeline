package transform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class pipeline_transform_test_1 {
    private static PipelineResult run_transform_1(PipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);

        final List<KV<String, String>> origin_data = Arrays.asList(
                KV.of("amy", "amy@example.com"),
                KV.of("carl", "carl@example.com"),
                KV.of("julia", "julia@example.com"),
                KV.of("carl", "carl@email.com"));

        PCollection<KV<String, String>> pCollection = pipeline.apply("data into PCollection", Create.of(origin_data))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

        PCollection<KV<String, Iterable<String>>> result = pCollection.apply(GroupByKey.create());
        result.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
                c.output(c.element().getKey());
                //c.element().getValue().forEach(System.out::println);

            }
        }));


        return pipeline.run();
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        run_transform_1(options);
    }
}
