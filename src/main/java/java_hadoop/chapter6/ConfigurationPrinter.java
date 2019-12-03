package java_hadoop.chapter6;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {
	
	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("yarn-default.xml");
		Configuration.addDefaultResource("yarn-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		for (Entry<String, String> entry : conf) {
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}

}


// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.ConfigurationPrinter -conf conf/hadoop-localhost.xml | grep yarn.resourcemanager.address
// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.ConfigurationPrinter -D color=yellow | grep color
// bin/hadoop jar ~/workspace/java_hadoop/target/java_hadoop-0.0.1-SNAPSHOT.jar java_hadoop.chapter5.ConfigurationPrinter -D mapreduce.job.reduces=n