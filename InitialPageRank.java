package PRFinal;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


//Modifying input file and assigning initial page ranks to all the nodes.

public class InitialPageRank extends Configured{

public class ModifyInputMapper
		extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		String input = value.toString();
		String startRank = "1";
		for (String line : input.split("\\r?\\n")) {
			if (line.contains("\\s+")) {
				String node = line.substring(0, line.indexOf("\\s+"));
				String Edges = line.replace(node, "");
				while (Edges.startsWith("\\s+")) {
					Edges = Edges.substring(1);
				}
				output.collect(new Text(node), new Text(Edges + "$"+ startRank));
			} 
			else {
				output.collect(new Text(line), new Text("$" + startRank));
			}
		}
	}
}

	public void modifyInput(String input, String output) 
			throws Exception {
		JobConf jobConf = new JobConf(GraphProperties.class);
		FileInputFormat.setInputPaths(jobConf, new Path(input));
		FileOutputFormat.setOutputPath(jobConf, new Path(output));

		jobConf.setMapperClass(ModifyInputMapper.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		JobClient.runJob(jobConf);
	}	
}
