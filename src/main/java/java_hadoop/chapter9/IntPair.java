package java_hadoop.chapter9;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;


public class IntPair implements WritableComparable<IntPair> {
	
	private IntWritable first;
	private IntWritable second;
	
	public int getFirst() {
		return first.get();
	}

	public void setFirst(IntWritable first) {
		this.first = first;
	}

	public int getSecond() {
		return second.get();
	}

	public void setSecond(IntWritable second) {
		this.second = second;
	}

	public IntPair() {
		set(new IntWritable(), new IntWritable());
	}
	
	public IntPair(int first, int second) {
		set(new IntWritable(first), new IntWritable(second));
	}
	
	public IntPair(Integer first, Integer second) {
		set(new IntWritable(first), new IntWritable(second));
	}
	
	public void set(IntWritable first, IntWritable second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof IntPair) {
			IntPair tp = (IntPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		return first + "\t" + second;
	}

	@Override
	public int compareTo(IntPair tp) {
		int cmp = first.compareTo(tp.first);
		if (cmp != 0) {
			return cmp;
		}
			
		return second.compareTo(tp.second);
	}

}
