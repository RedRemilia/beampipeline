package pipeline_design;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.build.Plugin;

public class beam_test_4 {
    /*从本地读取文件，读入信息到内存中*/
    public static PipelineResult run_4(PipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);
        /*从文本文件中读取数据，并转化成KV键值对*/
        PCollection<String> input = pipeline.apply("read", TextIO.read().from("input/pipeline_input.txt"));
        input.apply("trans", ParDo.of(new DoFn<String, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println("原始数据："+c.element());
                String[] s = c.element().split(",");
                c.output(KV.of(s[0], s[1]));
            }
        })).apply("output key", ParDo.of(new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                c.output(c.element().getKey());
                System.out.println("key值"+c.element().getKey());
            }
        }));



        return pipeline.run();
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        run_4(options);
    }
}
