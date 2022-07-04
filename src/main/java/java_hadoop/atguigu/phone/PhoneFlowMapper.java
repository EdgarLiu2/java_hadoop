package java_hadoop.atguigu.phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输出：
 *  Key：手机号
 *  Value：PhoneFlowBean
 *
 * Created by liuzhao on 2022/6/26
 */
public class PhoneFlowMapper extends Mapper<LongWritable, Text, Text, PhoneFlowBean> {
    private Text outKey = new Text();
    private PhoneFlowBean outValue = new PhoneFlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PhoneFlowBean>.Context context) throws IOException, InterruptedException {
        // 获取一行：
        // 1	13810001234	192.168.0.1	www.baidu.com	234 456 200
        String line = value.toString();

        // 切割
        String[] items = line.split("\t");
        int cols = items.length;
        String phone = items[1];
        long upFlow = Long.valueOf(items[cols - 3]);
        long downFlow = Long.valueOf(items[cols - 2]);

        // 封装
        outKey.set(phone);
        outValue.setUpFlowBytes(upFlow);
        outValue.setDownFlowBytes(downFlow);
        outValue.setTotalFlowBytes();

        // 写出
        context.write(outKey, outValue);
    }
}
