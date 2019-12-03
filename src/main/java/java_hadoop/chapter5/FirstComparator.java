package java_hadoop.chapter5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class FirstComparator extends WritableComparator {
	
	private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

	public FirstComparator() {
		super(TextPair.class);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(byte[], int, int, byte[], int, int)
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
			int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
			
			return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		if (a instanceof TextPair && b instanceof TextPair) {
			return ((TextPair) a).getFirst().compareTo(((TextPair)b).getFirst());
		}
		return super.compare(a, b);
	}

	
}
