package java_hadoop.chapter3;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {

	public static void main(String[] args) throws IOException {
		String localSrc = args[0];
		String dst = args[1];
		
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			public void progress() {
				System.out.print(".");
			}
		});
		
		IOUtils.copyBytes(in, out, 4096, true);

	}

}


// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter3.FileCopyWithProgress /Users/liuzhao/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/user/liuzhao/input/java_hadoop-0.0.1-SNAPSHOT.jar
