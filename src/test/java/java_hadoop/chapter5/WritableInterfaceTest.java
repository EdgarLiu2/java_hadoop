package java_hadoop.chapter5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;

public class WritableInterfaceTest {
	
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		
		return out.toByteArray();
	}
	
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		
		return bytes;
	}

	@Test
	public void testSerialize() throws IOException {
		IntWritable writable = new IntWritable(163);
		byte[] bytes = serialize(writable);
		
		assertThat(bytes.length, is(4));
		assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));
	}
	
	@Test
	public void testDeserialize() throws IOException {
		IntWritable writable = new IntWritable(163);
		byte[] bytes = serialize(writable);
		
		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		
		assertThat(newWritable.get(), is(writable.get()));
	}
	
	@Test
	public void testTextIndexing() {
		Text t = new Text("hadoop");
	
		assertThat(t.getLength(), is(6));
		assertThat(t.getBytes().length, is(6));
		
		// charAt() return an int representing a Unicode code point
		assertThat(t.charAt(2), is((int)'d'));
		assertThat("Out of bounds", t.charAt(100), is(-1));
		
		assertThat("Find a substring", t.find("do"), is(2));
		assertThat("Finds first 'o'", t.find("o"), is(3));
		assertThat("Finds 'o' from position 4 or later", t.find("o", 4), is(4));
		assertThat("No match", t.find("pig"), is(-1));
	}
	
	@Test
	public void testBytesWritable() throws IOException {
		BytesWritable b = new BytesWritable(new byte[] {3, 5});
		byte[] bytes = serialize(b);
		
		assertThat(StringUtils.byteToHexString(bytes), is("000000020305"));
	}
}
