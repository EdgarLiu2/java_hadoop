package java_hadoop.atguigu.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输入：
 *  Key：PhoneFlowBean
 *  Value：手机号
 *
 * Created by liuzhao on 2022/7/4
 */
public class PhoneFlowReducer2 extends Reducer<PhoneFlowBean, Text, Text, PhoneFlowBean> {

    @Override
    protected void reduce(PhoneFlowBean key, Iterable<Text> values, Reducer<PhoneFlowBean, Text, Text, PhoneFlowBean>.Context context) throws IOException, InterruptedException {
        for (Text phone : values) {
            System.out.println(phone);
            System.out.println(key);
            // 写出
            context.write(phone, key);
        }
    }
}
