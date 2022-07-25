package java_hadoop.atguigu.reduceJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by liuzhao on 2022/7/23
 */
public class ProductOrderMapper2 extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> productMap = new HashMap<>();
    private Text outKey = new Text();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 获取缓存文件，并把文件内容封装到集合
        URI[] cacheFiles = context.getCacheFiles();

        // 获取文件系统
        FileSystem fs = FileSystem.get(context.getConfiguration());
        // 缓存文件输入流
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
        // 从输入流读数据
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        // 一次处理一行
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 切割
            String[] fields = line.split("\t");
            assert 2 == fields.length;

            productMap.put(fields[0], fields[1]);
        }

        // 关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        // 处理order.txt，获取一行
        String line = value.toString();
        String[] fields = line.split("\t");
        assert 3 == fields.length;

        ProductOrderBean bean = new ProductOrderBean();
        bean.setOrderId(fields[0]);
        bean.setProductName(productMap.get(fields[1]));
        bean.setAmount(Integer.valueOf(fields[2]));

        outKey.set(bean.toString());
        context.write(outKey, NullWritable.get());
    }
}
