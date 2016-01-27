package PRFinal;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

// This calculates the PageRank of current input file.

public class RankComputer extends Configured{

public class RankComputerMapper 
		extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String input = value.toString();
		for (String line : input.split("\\r?\\n")) {
			String[] inputLine = line.split("\\$");
			String nodes = inputLine[0];
			float rank = Float.parseFloat(inputLine[1]), rankShare = (float) 0.0;
			String[] nodeArray = nodes.split("\\s+");
			String mainNode = nodeArray[0];
			String nodeEdges = nodes.replace(mainNode, "");
			int length = nodeArray.length;
			if (length > 1) {
				rankShare = rank / (length - 1);
				for (String node : nodeArray) {
					if (!node.equals(mainNode)) {
						output.collect(new Text(node), new Text(Float.toString(rankShare)));
					}
				}
			}
			output.collect(new Text(mainNode), new Text(nodeEdges + "$" + rank+ "~"));
		}
	}
}

private static float DAMPING_FACTOR;

public class RankComputerReducer 
	extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		float combinedRankShare = (float) 0.0, rankShare = (float) 0.0, pageRank = (float) 0.0;
		String nodes = "";
		while (values.hasNext()) {
			Text text = values.next();
			String inputLine = text.toString();
			if (inputLine.contains("~")) {
				String[] rankedNodes = inputLine.split("\\$");
				nodes = rankedNodes[0];
				pageRank = Float.parseFloat(rankedNodes[1].replace("~", ""));
			}
			else {
				rankShare = Float.parseFloat(inputLine);
				combinedRankShare = combinedRankShare + rankShare;
			}
		}
		float newRank = (float) ((1 - DAMPING_FACTOR) + (DAMPING_FACTOR * combinedRankShare));
		if (Math.abs(pageRank - newRank) > 0.001) {
			reporter.incrCounter(Counter.NON_CONVERGENT_NODES, 1);
		}
		output.collect(key, new Text(nodes + "$" + Float.toString(newRank)));
	}

	public void configure(JobConf jobConf) {
		DAMPING_FACTOR = Float.parseFloat(jobConf.get("DampingFactor"));
	}
}

	public long ComputeRank(String input, String output,String dampingFactor) 
			throws Exception {
		JobConf jobConf = new JobConf(new Configuration(), GraphProperties.class);
		jobConf.set("DampingFactor", dampingFactor);

		FileInputFormat.setInputPaths(jobConf, new Path(input));
		FileOutputFormat.setOutputPath(jobConf, new Path(output));

		jobConf.setMapperClass(RankComputerMapper.class);
		jobConf.setReducerClass(RankComputerReducer.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Text.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		RunningJob job = JobClient.runJob(jobConf);

		Counters counters = job.getCounters();
		long nonconvergentnodes = counters.getCounter(Counter.NON_CONVERGENT_NODES);
		return nonconvergentnodes;
	}
}
