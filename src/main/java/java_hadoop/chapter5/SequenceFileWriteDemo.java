package java_hadoop.chapter5;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileWriteDemo {
	
	private static final String[] DATA = {
			"One, two, buckle my shoe",
			"Three, four, shut the door",
			"Five, size, pick up sticks",
			"Seven, eight, lay them straight",
			"Nine, ten, a big fat hen"
			
	};

	public static void main(String[] args) throws IOException {
		String uri = args[0];
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			
			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}

// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.SequenceFileWriteDemo numbers.seq
