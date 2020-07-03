package pipeline_design;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.util.*;

import static java.util.Arrays.asList;

public class beam_test_3 {

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
//                    System.out.append("以a开头的单词: ").append(d.element()).append("\n");
                }else if(d.element().startsWith("b")){
                    d.output(startsWithBTag, d.element());
//                    System.out.append("以b开头的单词: ").append(d.element()).append("\n");
                }else{
                    d.output(startsWithCTag, d.element());
//                    System.out.append("以c开头的单词: ").append(d.element()).append("\n");
                }
            }
        }).withOutputTags(startsWithATag, TupleTagList.of(asList(startsWithBTag, startsWithCTag))));

        //单独输出b集合中的元素
        mixedCollection.get(startsWithATag).apply("output a",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext a){
                a.output(a.element());
//                System.out.println("集合A中的元素: "+a.element());
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

        /*将上一步中分离的A，B，C 3个集合中的元素再组合到同一个数据集中，进行处理。使用了Flatten方法，Flatten方法运用于
         * 同一种类型的集合，能够将多个pCollection的元素混合到同一个pCollection集合中*/
        PCollectionList<String> collectionList = PCollectionList.of(mixedCollection.get(startsWithATag))
                .and(mixedCollection.get(startsWithBTag)).and(mixedCollection.get(startsWithCTag));
        PCollection<String> mergedPCollection = collectionList.apply(Flatten.pCollections());
        mergedPCollection.apply("output all",ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext all){
                all.output(all.element());
                System.out.println("混合数据集合中的元素有: "+all.element());
            }
        }));

        pipeline.run();

    }

    public static void main(String[] args) {
        //1.创建管道流程
        //设定运行参数
        PipelineOptions options = PipelineOptionsFactory.fromArgs().withValidation().create();
        //设定运行引擎，默认是DirectRunner，也就是本地运行模式
        options.setRunner(DirectRunner.class);

        run_2(options);

    }


}
