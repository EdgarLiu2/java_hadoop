package java_hadoop.chapter5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class StreamCompressor {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		String codecClassname = args[0];
		Class<?> codecClass = Class.forName(codecClassname);

		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
		
		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out, 4096, false);
		out.finish();
	}

}


// echo "Text" | bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip -