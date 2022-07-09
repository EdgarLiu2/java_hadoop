package java_hadoop.atguigu.demo;

import java_hadoop.atguigu.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 * mvn package -DskipTests
 * hadoop jar ~/workspace/GitHub/java_leetcode/target/leetcode-1.0-SNAPSHOT.jar edgar.hadoop.WordCount /Users/liuzhao/Desktop/Bytedance/workspace/hadoop/hadoop_test/wordcount/input /Users/liuzhao/Desktop/Bytedance/workspace/hadoop/hadoop_test/wordcount/output
 * hadoop jar ~/workspace/GitHub/java_leetcode/target/leetcode-1.0-SNAPSHOT.jar edgar.hadoop.WordCount /test/wordcount/input /test/wordcount/output
 *
 * Created by liuzhao on 2022/6/19
 */
public class WordCount {

    /**
     * Map阶段
     * 输入类型：Key=Object Value=Text
     * 输出类型：Key=Text Value=IntWritable
     */
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // 获取一行内容，并调用分词器
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase(), " \t\n\r\f,.()/-");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * Reduce阶段
     * 输出类型：Key=Text Value=IntWritable
     * 输出类型：Key=Text Value=IntWritable
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

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

    public static class WordCountCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

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

    static void wordCount(String inputPath, String outputPath) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(WordCount.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 指定Job的输入文件目录
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // 7. 指定Job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 8. 提交Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static void wordCountWithCombineTextInputFormat(String inputPath, String outputPath) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(WordCount.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 如果不设置，默认是TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值：4MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);

        // 6. 指定Job的输入文件目录
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // 7. 指定Job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 8. 提交Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static void wordCountWithSetNumReduceTasks(String inputPath, String outputPath) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(WordCount.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 如果不设置，默认是TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值：4MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 设置自定义分区类
//        job.setPartitionerClass(CustomerPartitioner.class);
        // 设置ReduceTask的数目，默认为1
        job.setNumReduceTasks(2);

        // 6. 指定Job的输入文件目录
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // 7. 指定Job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 8. 提交Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static void wordCountWithCombiner(String inputPath, String outputPath) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(WordCount.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(IntSumReducer.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 指定Job的输入文件目录
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // 7. 指定Job的输出结果目录
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 8. 提交Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
//        wordCount(args[0], args[1]);
        wordCount(HDFSUtil.DATA_BASE + "/word_count/input", HDFSUtil.DATA_BASE + "/word_count/output");
//        wordCountWithCombiner(HDFSUtil.DATA_BASE + "/word_count/input", HDFSUtil.DATA_BASE + "/word_count/output");
    }
}
