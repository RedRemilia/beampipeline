import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.*;
import java.util.logging.Logger;

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

    //创建一个处理模式，输出多个结果
    private static void run_2(PipelineOptions options) {
        //创建管道，定义初始数据集
        Pipeline pipeline = Pipeline.create(options);
        final List<String> LINES = asList("ash","bash","ask","bread","argue","beef", "cash", "crack", "change");
        PCollection<String> dbRowCollection = pipeline.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());

        //设置储存3分类数据的集合
        final TupleTag<String> startsWithATag = new TupleTag<>(){};
        final TupleTag<String> startsWithBTag = new TupleTag<>(){};
        final TupleTag<String> startsWithCTag = new TupleTag<>(){};

        /*将输入数据集按照首字母为a，b，c分别分成3类，并储存到3个集合中，输出时定义startWithATag为主输出集合，
          startsWithBTag和startWithCTag为其他数据集，用aslist
         */
        PCollectionTuple mixedCollection = dbRowCollection.apply("divide collection",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext d){
                if(d.element().startsWith("a")){
                    d.output(startsWithATag, d.element());
                    System.out.append("以a开头的单词: ").append(d.element()).append("\n");
                }else if(d.element().startsWith("b")){
                    d.output(startsWithBTag, d.element());
                    System.out.append("以b开头的单词: ").append(d.element()).append("\n");
                }else{
                    d.output(startsWithCTag, d.element());
                    System.out.append("以c开头的单词: ").append(d.element()).append("\n");
                }
            }
        }).withOutputTags(startsWithATag, TupleTagList.of(asList(startsWithBTag, startsWithCTag))));

        //单独输出b集合中的元素
        mixedCollection.get(startsWithATag).apply("output a",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext a){
                a.output(a.element());
                System.out.println(a.element());
            }
        }));
        /*
        //单独输出b集合中的元素
        mixedCollection.get(startsWithBTag).apply("output b",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext b){
                b.output(b.element());
                System.out.println(b.element());
            }
        }));

         */
        /*
        //单独输出c集合中的元素
        mixedCollection.get(startsWithCTag).apply("output c",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                c.output(c.element());
                System.out.println(c.element());
            }
        }));

         */

        pipeline.run();

    }

    public static void main(String[] args) {
        //1.创建管道流程
        //设定运行参数
        PipelineOptions options = PipelineOptionsFactory.fromArgs().withValidation().create();
        //设定运行引擎，默认是DirectRunner，也就是本地运行模式
        options.setRunner(DirectRunner.class);

//        run_1(options);
        run_2(options);
    }


}
