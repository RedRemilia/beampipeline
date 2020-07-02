import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import java.util.*;

import static java.util.Arrays.asList;

public class beam_test_1 {
    //设定管道运行模式1，对同一数据集进行多次转换
    public static PipelineResult run_1(PipelineOptions options){
        //根据设定创建流水线管道
        Pipeline pipeline = Pipeline.create(options);
        //设定管道运行数据集
        final List<String> LINES = asList("ash","bash","ask","bread","argue","beef");
        PCollection<String> dbRowCollection = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        //创建名为ATrans的转换模式，输出以a开头的单词
        PCollection<String> CollectionA = dbRowCollection.apply("ATrans", ParDo.of(new DoFn<String, String>(){
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.append("ATrans流程运行中:  ");
                if(c.element().startsWith("a")){
                    c.output(c.element());
                    System.out.append("以A开头的单词有:").append(c.element());
                }
                System.out.append("\n");
            }
        }));
        //创建名为BTrans的转换模式，输出以b开头的单词
        PCollection<String> CollectionB = dbRowCollection.apply("BTrans", ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.append("BTrans流程运行中:  ");
                if(c.element().startsWith("b")){
                    c.output(c.element());
                    System.out.append("以B开头的单词有:").append(c.element());
                }
                System.out.append("\n");
            }
        }));
        return pipeline.run();
        /* 输出结果：
            ATrans流程运行中:
            ATrans流程运行中:  以A开头的单词有:ash
            BTrans流程运行中:
            BTrans流程运行中:  以B开头的单词有:bash
            BTrans流程运行中:
            ATrans流程运行中:  以A开头的单词有:ask
            ATrans流程运行中:
            ATrans流程运行中:
            BTrans流程运行中:  以B开头的单词有:bread
            ATrans流程运行中:  以A开头的单词有:argue
            BTrans流程运行中:
            BTrans流程运行中:  以B开头的单词有:beef
         */
    }


    public static void main(String[] args) {
        //设定运行参数
        PipelineOptions options = PipelineOptionsFactory.fromArgs().withValidation().create();
        //设定运行引擎，默认是DirectRunner，也就是本地运行模式
        options.setRunner(DirectRunner.class);

        run_1(options);
    }


}
