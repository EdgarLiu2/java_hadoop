package java_hadoop.atguigu.demo;

import java_hadoop.atguigu.util.HDFSUtil;
import java_hadoop.atguigu.util.TimeUtil;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by liuzhao on 2022/6/11
 */
public class HDFSDemo {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private HDFSUtil hdfs;

    public HDFSDemo() {
        try {
            hdfs = new HDFSUtil();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            logger.error("Can't connect to HDFS", e);
        }
    }

    public void mkdirs() {
        try {
            hdfs.getFs().mkdirs(new Path("/xiyou/" + TimeUtil.getTimeStamp()));
        } catch (IOException e) {
            logger.error("HDFS fail to mkdirs ", e);
        }
    }

    public void copyFromLocalFile() {
        try {
            /**
             * The src files are on the local disk.  Add it to the filesystem at
             * the given dst name.
             * delSrc indicates if the source should be removed
             * @param delSrc whether to delete the src
             * @param overwrite whether to overwrite an existing file
             * @param srcs array of paths which are source
             * @param dst path
             * @throws IOException IO failure
             */
            hdfs.getFs().copyFromLocalFile(
                    false,
                    true,
                    new Path[]{new Path("pom.xml")},
                    new Path(hdfs.getHDFSRoot() + "/xiyou/pom.xml")
            );
        } catch (IOException e) {
            logger.error("HDFS fail to copyFromLocalFile ", e);
        }
    }

    public void copyToLocalFile() {
        /**
         * The src file is under this filesystem, and the dst is on the local disk.
         * Copy it from the remote filesystem to the local dst name.
         * delSrc indicates if the src will be removed
         * or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem
         * as the local file system or not. RawLocalFileSystem is non checksumming,
         * So, It will not create any crc files at local.
         *
         * @param delSrc
         *          whether to delete the src
         * @param src
         *          path
         * @param dst
         *          path
         * @param useRawLocalFileSystem
         *          whether to use RawLocalFileSystem as local file system or not.
         *
         * @throws IOException for any IO error
         */
        try {
            hdfs.getFs().copyToLocalFile(
                    false,
                    new Path(hdfs.getHDFSRoot() + "/xiyou"),
                    new Path("./"),
                    false); // 为true时不开启本地校验
        } catch (IOException e) {
            logger.error("HDFS fail to copyToLocalFile ", e);
        }

    }

    public void rename() {
        try {
            /**
             * Renames Path src to Path dst.
             * @param src path to be renamed
             * @param dst new path after rename
             * @throws IOException on failure
             * @return true if rename is successful
             */
            hdfs.getFs().rename(
                    new Path("/xiyou/pom.xml"),
                    new Path("/xiyou/pom.old.xml")
            );
        } catch (IOException e) {
            logger.error("HDFS fail to rename", e);
        }
    }

    public void listFiles() {
        try {
            RemoteIterator<LocatedFileStatus> iterator = hdfs.getFs().listFiles(
                    new Path("/"),
                    true
            );

            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();

                // 文件信息
                logger.info(HDFSUtil.formatFileStatus(fileStatus));
                
                // 块信息
                BlockLocation[] blocks = fileStatus.getBlockLocations();
                for (BlockLocation block : blocks) {
                    logger.info("block={}", block.toString());
                }
            }
        } catch (IOException e) {
            logger.error("HDFS fail to listFiles", e);
        }
    }

    public void listStatus() {
        try {
            FileStatus[] files = hdfs.getFs().listStatus(new Path("/"));
            for (FileStatus file : files) {
                logger.info("fileName={} isFile{}", file.getPath().getName(), file.isFile());
            }
        } catch (IOException e) {
            logger.error("HDFS fail to listStatus", e);
        }
    }

    public void delete() {

        try {
            /** Delete a file.
             *
             * @param f the path to delete.
             * @param recursive if path is a directory and set to
             * true, the directory is deleted else throws an exception. In
             * case of a file the recursive can be set to either true or false.
             * @return  true if delete is successful else false.
             * @throws IOException IO failure
             */
            hdfs.getFs().delete(
                    new Path("/xiyou/pom.old.xml"),
                    false);
            https://www.bilibili.com/video/BV1Qp4y1n7EN?p=53&spm_id_from=pageDriver&vd_source=8554742986ad8aaadca371526a97a40c
            // 递归删除
            hdfs.getFs().delete(
                    new Path("/xiyou"),
                    true);
        } catch (IOException e) {
            logger.error("HDFS fail to delete ", e);
        }
    }

    public static void main(String[] args) {
        HDFSDemo demo = new HDFSDemo();
        demo.mkdirs();
        demo.copyFromLocalFile();
        demo.copyToLocalFile();
        demo.rename();
        demo.listFiles();
        demo.listStatus();
        demo.delete();
    }
}
