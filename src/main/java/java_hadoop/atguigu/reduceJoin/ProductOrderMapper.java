package java_hadoop.atguigu.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by liuzhao on 2022/7/23
 */
public class ProductOrderMapper extends Mapper<LongWritable, Text, Text, ProductOrderBean> {


    private String tableName;
    private Text outKey = new Text();
    private ProductOrderBean outValue = new ProductOrderBean();


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, ProductOrderBean>.Context context) throws IOException, InterruptedException {
        // 初始化：根据输入文件，区别order和product表
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String fileName = inputSplit.getPath().getName();

        if (fileName.startsWith(ProductOrderBean.PRODUCT_TABLE)) {
            tableName = ProductOrderBean.PRODUCT_TABLE;
        } else if (fileName.startsWith(ProductOrderBean.ORDER_TABLE)) {
            tableName = ProductOrderBean.ORDER_TABLE;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ProductOrderBean>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        String[] cols = line.split("\t");

        // 针对不同的文件，进行解析
        switch (tableName) {
            case ProductOrderBean.PRODUCT_TABLE:
                // 商品表
                // 01	小米
                outKey.set(cols[0]);

                outValue.setProductId(cols[0]);
                outValue.setProductName(cols[1]);
                outValue.setFlag(ProductOrderBean.PRODUCT_TABLE);

                break;
            case ProductOrderBean.ORDER_TABLE:
                // 订单表
                // 1001	01	1
                outKey.set(cols[1]);

                outValue.setOrderId(cols[0]);
                outValue.setProductId(cols[1]);
                outValue.setAmount(Integer.valueOf(cols[2]));
                outValue.setFlag(ProductOrderBean.ORDER_TABLE);

                break;
        }

        context.write(outKey, outValue);
    }
}
