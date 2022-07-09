package java_hadoop.atguigu.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 手机号136, 137, 138, 139开头的使用一个独立Partition，其它开头共用一个Partition
 *
 * Created by liuzhao on 2022/7/3
 */
public class PhonePartitioner2 extends Partitioner<PhoneFlowBean, Text> {
    @Override
    public int getPartition(PhoneFlowBean phoneFlowBean, Text text, int numPartitions) {
        String phone = text.toString();
        String phonePrefix = phone.substring(0, 3);

        int partitionId;
        switch (phonePrefix) {
            case "136":
                partitionId = 1;
                break;
            case "137":
                partitionId = 2;
                break;
            case "138":
                partitionId = 3;
                break;
            case "139":
                partitionId = 4;
                break;
            default:
                partitionId = 0;
        }

        return partitionId;
    }
}
