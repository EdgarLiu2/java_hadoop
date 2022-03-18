package java_hadoop.chapter3;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;


public class CoherencyModelTest {
	private MiniDFSCluster cluster;		// use an in-process HDFS cluster for testing
	private FileSystem fs;
	
	@Before
	public void setUp() throws IOException {
		Configuration conf = new Configuration();
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "/tmp");
		}
		
		cluster = new MiniDFSCluster.Builder(conf).build();
		fs = cluster.getFileSystem();
	}
	
	@After
	public void tearDown() throws IOException {
		if (fs != null) {
			fs.close();
		}
		
		if (cluster != null) {
		    cluster.shutdown();
		}
	}
	
	@Ignore
	@Test
	public void testFileExists() throws IOException {
		Path p = new Path("/test_fs_exists");
		fs.create(p);
		assertThat(fs.exists(p), is(true));
	}
	
	@Ignore
	@Test
	public void testFlush() throws IOException {
		Path p = new Path("/test_flush");
		OutputStream out = fs.create(p);
		out.write("content".getBytes("UTF-8"));
		// any content written to the file is not guaranteed to be visible, even if the stream is flushed.
		out.flush();
		// the file appears to have a length of zero
		assertThat(fs.getFileStatus(p).getLen(), is(0L));
	}
	
	@Ignore
	@Test
	public void testHFlush() throws IOException {
		Path p = new Path("/test_hflush");
		FSDataOutputStream out = fs.create(p);
		out.write("content".getBytes("UTF-8"));
		// force all buffers to be flushed to the datanodes
		out.hflush();
		// out.close(); performs an implicit hflush()
		
		// hsync() guarantee that the datanodes have written the data to disk
		// out.hsync();
		
		// data has reached all the datanodes in the write pipeline and is visible to all new readers
		assertThat(fs.getFileStatus(p).getLen(), is((long)"content".length()));
	}
	

}
