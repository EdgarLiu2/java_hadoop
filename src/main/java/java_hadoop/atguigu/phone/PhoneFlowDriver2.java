package java_hadoop.atguigu.phone;

import java_hadoop.atguigu.util.HDFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * mvn package -DskipTests
 * hadoop jar ~/workspace/GitHub/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.atguigu.phone.PhoneFlowDriver /Users/liuzhao/Desktop/Bytedance/workspace/GitHub/java_hadoop/src/input/phone_data/input2 /Users/liuzhao/Desktop/Bytedance/workspace/hadoop/hadoop_test/phone_data/output2
 *
 * Created by liuzhao on 2022/6/26
 */
public class PhoneFlowDriver2 {
    public static void main(String[] args) throws Exception {
        // 1. 获取配置信息，获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PhoneFlow");

        // 2. 指定本程序的jar包所再的本地路径
        job.setJarByClass(PhoneFlowDriver2.class);

        // 3. 关联Mapper/Reducer业务类
        job.setMapperClass(PhoneFlowMapper2.class);
        job.setReducerClass(PhoneFlowReducer2.class);

        // 4. 指定Mapper输出的<K, V>类型
        job.setMapOutputKeyClass(PhoneFlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 指定最终输出的<K, V>类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneFlowBean.class);

        // 设置自定义分区类
//        job.setPartitionerClass(PhonePartitioner.class);
        // 设置ReduceTask的数目，默认为1
//        job.setNumReduceTasks(5);

        // 6. 指定Job的输入文件目录
//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(HDFSUtil.INPUT_BASE + "/phone_data/input2"));

        // 7. 指定Job的输出结果目录
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(HDFSUtil.OUTPUT_BASE + "/phone_data/output2"));

        // 8. 提交Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
