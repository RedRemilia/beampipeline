import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class beam_test_1 {
    public interface MyOptions extends PipelineOptions{
        @Description("Input for the pipeline")
        @Default.String("")
        String getInput();
        void setInput(String input);

        @Description("Output for the pipeline")
        @Default.String("")
        String getOutput();
        void setOutput(String output);
    }
    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyOptions.class);
        PipelineOptions options = PipelineOptionsFactory.fromArgs().withValidation().create();
//        options.setRunner(DirectRunner.class);
        Pipeline p = Pipeline.create(options);

        final List<String> LINES = Arrays.asList("ash","bash","ask","bread","argue","beef");
        PCollection<String> dbRowCollection = p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        PCollection<String> CollectionA = dbRowCollection.apply("ATrans", ParDo.of(new DoFn<>(){
            @ProcessElement
            public void processElement(ProcessContext c){
                if(c.element().startsWith("a")){
                    c.output(c.element());
                    System.out.append("以A开头的单词有:").append(c.element()).append("\n");
                }
            }
        }));

        PCollection<String> CollectionB = dbRowCollection.apply("BTrans", ParDo.of(new DoFn<>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                if(c.element().startsWith("b")){
                    c.output(c.element());
                    System.out.append("以B开头的单词有:").append(c.element()).append("\n");
                }
            }
        }));


        p.run().waitUntilFinish();


    }
}
