package java_hadoop.chapter5;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

public class UnicodeIteration {

	public static void main(String[] args) {
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
		
		ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
		int cp;
		
		while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
			System.out.println(Integer.toHexString(cp));
		}
	}
}

// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.UnicodeIteration