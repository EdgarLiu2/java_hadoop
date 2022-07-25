package java_hadoop.atguigu.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * Created by liuzhao on 2022/7/23
 */
public class ProductOrderReducer extends Reducer<Text, ProductOrderBean, ProductOrderBean, NullWritable> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void reduce(Text key, Iterable<ProductOrderBean> values, Reducer<Text, ProductOrderBean, ProductOrderBean, NullWritable>.Context context) throws IOException, InterruptedException {

        // 商品信息
        ProductOrderBean product = new ProductOrderBean();
        // 订单信息
        ArrayList<ProductOrderBean> orderList = new ArrayList<ProductOrderBean>();

        for (ProductOrderBean bean : values) {
            ProductOrderBean tmp = new ProductOrderBean();
            try {
                BeanUtils.copyProperties(tmp, bean);
            } catch (IllegalAccessException e) {
                logger.error("Fail to clone ProductOrderBean", e);
            } catch (InvocationTargetException e) {
                logger.error("Fail to clone ProductOrderBean", e);
            }

            switch (bean.getFlag()) {
                case ProductOrderBean.PRODUCT_TABLE:
                    product = tmp;
                    break;
                case ProductOrderBean.ORDER_TABLE:
                    orderList.add(tmp);
                    break;
            }
        }

        // 遍历orderList，赋值productName
        for (ProductOrderBean productOrderBean : orderList) {
            productOrderBean.setProductName(product.getProductName());

            // 输出
            context.write(productOrderBean, NullWritable.get());
        }
    }
}
