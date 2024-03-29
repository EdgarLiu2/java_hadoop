package java_hadoop.chapter3;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ShowFilesStatusTest {

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
		
		OutputStream out = fs.create(new Path("/dir/file"));
		out.write("content".getBytes("UTF-8"));
		out.close();
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
	@Test(expected = FileNotFoundException.class)
	public void throwsFileNotFoundForNonExistentFile() throws IOException {
		fs.getFileStatus(new Path("no-such-file"));
	}

	@Ignore
	@Test
	public void fileStatusForFile() throws IOException {
		Path file = new Path("/dir/file");
		FileStatus stat = fs.getFileStatus(file);
		
		assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
		assertThat(stat.isDirectory(), is(false));
		assertThat(stat.getLen(), is(7L));
		assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
		assertThat(stat.getReplication(), is((short)1));
		assertThat(stat.getBlockSize(), is(128 * 1024 * 1024L));
		assertThat(stat.getOwner(), is(System.getProperty("user.name")));
		assertThat(stat.getGroup(), is("supergroup"));
		assertThat(stat.getPermission().toString(), is("rw-r--r--"));
	}
	
	@Ignore
	@Test
	public void fileStatusForDirectory() throws IOException {
		Path file = new Path("/dir");
		FileStatus stat = fs.getFileStatus(file);
		
		assertThat(stat.getPath().toUri().getPath(), is("/dir"));
		assertThat(stat.isDirectory(), is(true));
		assertThat(stat.getLen(), is(0L));
		assertThat(stat.getModificationTime(), is(lessThanOrEqualTo(System.currentTimeMillis())));
		assertThat(stat.getReplication(), is((short)0));
		assertThat(stat.getBlockSize(), is(0L));
		assertThat(stat.getOwner(), is(System.getProperty("user.name")));
		assertThat(stat.getGroup(), is("supergroup"));
		assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
	}
}
