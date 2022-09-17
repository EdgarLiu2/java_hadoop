package java_hadoop.atguigu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;


/**
 * <a href="https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html">WordCount</a>
 * mvn package -DskipTests
 * hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.atguigu.demo.WordCount2 WordCount /test/wordcount/input /test/wordcount/output
 * hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.atguigu.demo.WordCount2 WordCount -Dmapreduce.job.queuename=root.test /test/wordcount/input /test/wordcount/output
 * yarn jar java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.atguigu.demo.WordCount2 WordCount -Dmapreduce.job.queuename=root.test /test/wordcount/input /test/wordcount/output
 * Created by liuzhao on 2022/8/16
 */
public class WordCount2 implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Job job = Job.getInstance(getConf(), "WordCount");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(WordCount2.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(WordCount2Mapper.class);
        job.setReducerClass(WordCount2Reducer.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 指定Job的输入文件目录
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // 7. 指定Job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8. 提交Job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * Map阶段
     * 输入类型：Key=Object Value=Text
     * 输出类型：Key=Text Value=IntWritable
     */
    public static class WordCount2Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable outValueOne = new IntWritable(1);
        private final Text outKey = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 获取一行内容，并调用分词器
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(), " \t\n\r\f,.()/-");
            while (itr.hasMoreTokens()) {
                outKey.set(itr.nextToken());
                context.write(outKey, outValueOne);
            }
        }
    }

    /**
     * Reduce阶段
     * 输出类型：Key=Text Value=IntWritable
     * 输出类型：Key=Text Value=IntWritable
     */
    public static class WordCount2Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    private static Tool tool;

    public static void main(String[] args) throws Exception {
        // 创建配置
        Configuration conf = new Configuration();

        switch (args[0]) {
            case "WordCount":
                tool = new WordCount2();
                break;
            default:
                throw new IllegalArgumentException("Only support WordCount tool");
        }

        // 执行程序
        int result = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));
        System.exit(result);
    }
}
