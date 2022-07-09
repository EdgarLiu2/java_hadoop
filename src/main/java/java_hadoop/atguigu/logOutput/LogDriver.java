package java_hadoop.atguigu.logOutput;

import java_hadoop.atguigu.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by liuzhao on 2022/7/9
 */
public class LogDriver {

    public static void main(String[] args) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LogOutput");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(LogDriver.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // 自定义OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        // 6. 指定Job的输入文件目录
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(HDFSUtil.DATA_BASE + "/log_data/input"));

        // 7. 指定Job的输出结果目录
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(HDFSUtil.DATA_BASE + "/log_data/output"));

        // 8. 提交Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
