package java_hadoop.atguigu.weblog_etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入：
 *  163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
 * 输出：
 *  Key:
 *  Value: NullWritable
 * Created by liuzhao on 2022/8/8
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

        // 1. 获取一行
        String line = value.toString();

        // 2. 切割 & ETL清洗
        boolean isValid = parseOneLog(line);
        if (!isValid) {
            return;
        }

        // 3. 写出
        context.write(value, NullWritable.get());
    }

    private boolean parseOneLog(String line) {
        String[] fields = line.split("\\s");

        return fields.length > 11;
    }
}
