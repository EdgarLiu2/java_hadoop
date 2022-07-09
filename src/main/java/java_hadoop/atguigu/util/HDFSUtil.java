package java_hadoop.atguigu.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by liuzhao on 2022/6/11
 */
public class HDFSUtil {

    public final static String DATA_BASE = "/Users/liuzhao/Desktop/Bytedance/workspace/GitHub/java_hadoop/src/data";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String host;
    private int port;
    private String username;

    // 客户端对象
    private FileSystem fs;

    public HDFSUtil() throws URISyntaxException, IOException, InterruptedException {
        this("localhost", 8020, "liuzhao");
    }

    public HDFSUtil(String host, int port, String username) throws URISyntaxException, IOException, InterruptedException {
        this.host = host;
        this.port = port;
        this.username = username;

        init();
    }

    public String getHDFSRoot() {
        return String.format("hdfs://%s:%d", host, port);
    }

    private void init() throws URISyntaxException, IOException, InterruptedException {
        final String url = getHDFSRoot();

        // 集群NameNode地址
        URI uri = new URI(url);

        // 创建一个空配置
        Configuration conf = new Configuration();
//        conf.set("dfs.replication", "3");

        // 获取HDFS客户端对象
        fs = FileSystem.get(uri, conf, username);
    }

    public void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                logger.error("Can't close HDFS FileSystem", e);
            }
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public static String formatFileStatus(LocatedFileStatus fileStatus) {
        return String.format("fileName=%s path=%s owner=%s group=%s length=%d permission=%s modificationTime=%d",
                fileStatus.getPath().getName(),
                fileStatus.getPath(),
                fileStatus.getOwner(),
                fileStatus.getGroup(),
                fileStatus.getLen(),
                fileStatus.getPermission().toString(),
                fileStatus.getModificationTime());
    }
}
