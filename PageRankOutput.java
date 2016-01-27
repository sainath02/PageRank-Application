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

//Reads input file, finds nodes and page ranks and writes in output file

public class PageRankOutput extends Configured{

public class OutputMapper 
	extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String input = value.toString();
		for (String line : input.split("\\r?\\n")) {
			String[] inputLine = line.split("\\$");
			String nodes = inputLine[0];
			float pageRank = Float.parseFloat(inputLine[1]);
			String[] nodeArray = nodes.split("\\s+");
			String pageNode = nodeArray[0];
			output.collect(new Text(pageNode),new Text(Float.toString(pageRank)));
		}
	}
}

	public void OutputPageRank(String input, String output)
			throws Exception {
		JobConf jobConf = new JobConf(GraphProperties.class);
		FileInputFormat.setInputPaths(jobConf, new Path(input));
		FileOutputFormat.setOutputPath(jobConf, new Path(output));

		jobConf.setMapperClass(OutputMapper.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);
		JobClient.runJob(jobConf);
	}
}
