package java_hadoop.chapter8;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class NonSplittableTextInputFormat extends TextInputFormat {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.lib.input.TextInputFormat#isSplitable(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	
}
