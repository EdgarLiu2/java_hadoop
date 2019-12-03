package java_hadoop.chapter5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {
	
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		
		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.err.println("No codec found for " + uri);
			System.exit(1);
		}
		
		String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}
	
}

// bin/hdfs dfs -put file.gz /user/liuzhao/input
// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.FileDecompressor /user/liuzhao/input/file.gz
