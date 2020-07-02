import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;


public class beam_test_2 {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs().withValidation().create();
        Pipeline pipeline = Pipeline.create(options);
    }
}
