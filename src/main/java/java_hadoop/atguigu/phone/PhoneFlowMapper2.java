package java_hadoop.atguigu.phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输出：
 *  Key：PhoneFlowBean
 *  Value：手机号
 * Created by liuzhao on 2022/7/4
 */
public class PhoneFlowMapper2 extends Mapper<LongWritable, Text, PhoneFlowBean, Text> {

    private PhoneFlowBean outKey = new PhoneFlowBean();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PhoneFlowBean, Text>.Context context) throws IOException, InterruptedException {
        // 获取一行：
        // 1	13810001234	192.168.0.1	www.baidu.com	234 456 200
        String line = value.toString();

        // 切割
        String[] items = line.split("\t");
        String phone = items[0];
        long upFlow = Long.valueOf(items[1]);
        long downFlow = Long.valueOf(items[2]);

        // 封装
        outValue.set(phone);
        outKey.setUpFlowBytes(upFlow);
        outKey.setDownFlowBytes(downFlow);
        outKey.setTotalFlowBytes();

        // 写出
        context.write(outKey, outValue);
    }
}
