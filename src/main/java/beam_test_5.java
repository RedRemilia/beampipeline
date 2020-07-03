import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.sql.ResultSet;

public class beam_test_5 {
    /*从数据库中读取数据并通过管道输出*/
    public static PipelineResult run_5(PipelineOptions options){
        Pipeline pipeline = Pipeline.create(options);
        /*配置jdbc数据源，因为使用的是mysql 8.0， 所以配置的jdbc driver版本需要相对应
        * 在pom文件中引入mysql-connector-java的相关包。
        * 配置的url为com.mysql.cj.jdbc.Driver，而不是5.0所用的com.mysql.jdbc.Driver,
        * 同时jdbc 8.0版本需要指定时区，这里设置为上海*/
        JdbcIO.DataSourceConfiguration configuration = JdbcIO.DataSourceConfiguration.create(
                "com.mysql.cj.jdbc.Driver",
//                "jdbc:mysql://localhost:3306/bookmanager?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai&useSSL=false"
                "jdbc:mysql://localhost:3306/bookmanager?serverTimezone=Asia/Shanghai"
        ).withUsername("root").withPassword("hu123456");

        PCollection<KV<Integer, String>> resource= pipeline.apply(JdbcIO.<KV<Integer, String>>read()
                .withDataSourceConfiguration(configuration)
                .withQuery("SELECT reader_id, name,passwd from reader_card")
                .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()))
                .withRowMapper(new JdbcIO.RowMapper<KV<Integer, String>>() {
                    @Override
                    public KV<Integer, String> mapRow(ResultSet resultSet) throws Exception {
                        return KV.of(resultSet.getInt(1), resultSet.getString(2));
                    }
                })
                );

        resource.apply("print", ParDo.of(new DoFn<KV<Integer, String>, String>() {
            @ProcessElement
            public void processElement(ProcessContext ce){
                ce.output(ce.element().getValue());
                System.out.println("reader_id: "+ce.element().getKey()+"    姓名:"+ce.element().getValue());
            }
        }));
        return pipeline.run();
    }
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        run_5(options);

    }
}
