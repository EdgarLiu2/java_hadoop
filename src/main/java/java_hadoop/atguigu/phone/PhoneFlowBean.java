package java_hadoop.atguigu.phone;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by liuzhao on 2022/6/26
 */
public class PhoneFlowBean implements WritableComparable<PhoneFlowBean> {

    private long upFlowBytes;       // 上行流量
    private long downFlowBytes;     // 下行流量
    private long totalFlowBytes;    // 总流量

    public PhoneFlowBean() {
    }

    /**
     * 按照总流量的倒序进行排序，总流量相同时按照上行流量正序排序
     */
    @Override
    public int compareTo(PhoneFlowBean o) {

        // 先按总流量的倒序
        if (this.totalFlowBytes > o.getTotalFlowBytes()) {
            return -1;
        } else if (this.totalFlowBytes < o.getTotalFlowBytes()) {
            return 1;
        }

        // 当总流量相同时，按照上行流量正序排序
        if (this.upFlowBytes > o.getUpFlowBytes()) {
            return 1;
        } else if (this.upFlowBytes < o.getUpFlowBytes()) {
            return -1;
        }

        // 总流量和上行流量都相同
        return 0;
    }

    /**
     * 序列化方法
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upFlowBytes);
        out.writeLong(this.downFlowBytes);
        out.writeLong(this.totalFlowBytes);
    }

    /**
     * 反序列化方法
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlowBytes = in.readLong();
        this.downFlowBytes = in.readLong();
        this.totalFlowBytes = in.readLong();
    }

    public long getUpFlowBytes() {
        return upFlowBytes;
    }

    public void setUpFlowBytes(long upFlowBytes) {
        this.upFlowBytes = upFlowBytes;
    }

    public long getDownFlowBytes() {
        return downFlowBytes;
    }

    public void setDownFlowBytes(long downFlowBytes) {
        this.downFlowBytes = downFlowBytes;
    }

    public long getTotalFlowBytes() {
        return totalFlowBytes;
    }

    public void setTotalFlowBytes(long totalFlowBytes) {
        this.totalFlowBytes = totalFlowBytes;
    }

    public void setTotalFlowBytes() {
        this.totalFlowBytes = this.upFlowBytes + this.downFlowBytes;
    }

    @Override
    public String toString() {
        return upFlowBytes + "\t" + downFlowBytes + "\t" + totalFlowBytes;
    }
}
