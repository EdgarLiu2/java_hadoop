package java_hadoop.atguigu.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by liuzhao on 2022/7/23
 */
public class ProductOrderBean implements Writable {

    public final static String PRODUCT_TABLE = "product";
    public final static String ORDER_TABLE = "order";

    // 订单ID
    private String orderId = "0";
    // 商品数量
    private int amount = 0;
    // 商品ID
    private String productId = "0";
    // 商品名称
    private String productName = "";
    // 标记数据来源表：order/product
    private String flag = "";

    public ProductOrderBean() {
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.productId);
        out.writeInt(this.amount);
        out.writeUTF(this.productName);
        out.writeUTF(this.flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.amount = in.readInt();
        this.productName = in.readUTF();
        this.flag = in.readUTF();
    }

    @Override
    public String toString() {
        return String.format("%s\t%s\t%d", orderId, productName, amount);
    }
}
