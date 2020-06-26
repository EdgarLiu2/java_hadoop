package java_hadoop.chapter9;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java_hadoop.chapter5.TextPair;

/**
 * Example 9-12. Application to join weather records with station names
 * 
 * @author liuzhao
 *
 */
public class JoinRecordWithStationName extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}


class NcdcStationMetadataParser {
	public boolean parse(Text value) {
		return true;
	}
	
	public String getStationId() {
		return "";
	}
	
	public String getStationName() {
		return "";
	}
}

/**
 * Example 9-9. Mapper for tagging station records for a reduce-side join
 * 
 * @author liuzhao
 *
 */
class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	
	private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TextPair, Text>.Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			context.write(new TextPair(parser.getStationId(),  "0"), new Text(parser.getStationName()));
		}
	}
	
}