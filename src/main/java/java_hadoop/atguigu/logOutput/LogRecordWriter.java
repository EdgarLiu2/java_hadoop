package java_hadoop.atguigu.logOutput;

import java_hadoop.atguigu.util.HDFSUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by liuzhao on 2022/7/9
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private FSDataOutputStream outputStream1;
    private FSDataOutputStream outputStream2;

    public LogRecordWriter(TaskAttemptContext job) {
        // 创建两个输出流
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());

            outputStream1 = fs.create(new Path(HDFSUtil.DATA_BASE + "/log_data/output/stream1.log"));
            outputStream2 = fs.create(new Path(HDFSUtil.DATA_BASE + "/log_data/output/stream2.log"));
        } catch (IOException e) {
            logger.error("Fail to create OutputStream", e);
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String domain = key.toString();

        if (domain.contains("atguigu")) {
            outputStream1.writeBytes(domain + "\n");
        } else {
            outputStream2.writeBytes(domain + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(outputStream1);
        IOUtils.closeStream(outputStream2);
    }
}
