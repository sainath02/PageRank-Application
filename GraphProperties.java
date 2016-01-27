package PRFinal;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;


// Compute Graph properties.

public class GraphProperties extends Configured{

	
//Mapper Class 
public class GraphPropertiesMapper 
		extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String input = value.toString();
		for (String line : input.split("\\r?\\n")) {
			int curRank = 0;
			if (line.contains(" ")) {
				String[] nodes = line.split("\\s+");
				curRank = nodes.length - 1;
				output.collect(new Text("OutDegree"),
						new IntWritable(curRank));
			} else {
				output.collect(new Text("OutDegree"), new IntWritable(0));
			}

		}
	}
}

//Reducer Class

public class GraphPropertiesReducer 
		extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	//It will calculate node count, edge count, max degree, min degree, avg degree etc.
	
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		int numOfNodes = 0;
		float minDeg = Integer.MAX_VALUE, maxDeg = Integer.MIN_VALUE, degSum = 0, degree = 0;
		while (values.hasNext()) {
			IntWritable intWritable = values.next();
			degree = intWritable.get();
			if (maxDeg < degree) {
				maxDeg = degree;
			}
			if (minDeg > degree) {
				minDeg = degree;
			}
			degSum = degSum + degree;
			numOfNodes++;
		}
		float avgDeg = degSum / numOfNodes;
		output.collect(new Text("Nodes Count"),
				new Text(Integer.toString(numOfNodes)));
		output.collect(new Text("Edges Count"),
				new Text(Float.toString(degSum)));
		output.collect(new Text("Max Degree"),
				new Text(Float.toString(maxDeg)));
		output.collect(new Text("Min Degree"),
				new Text(Float.toString(minDeg)));
		output.collect(new Text("Avg Degree"),
				new Text(Float.toString(avgDeg)));
	}
}

	public void getGraphProperties(String input, String output) 
			throws IOException{
		JobConf jobConf = new JobConf(GraphProperties.class);
		FileInputFormat.setInputPaths(jobConf, new Path(input));
		FileOutputFormat.setOutputPath(jobConf, new Path(output));

		jobConf.setMapperClass(GraphPropertiesMapper.class);
		jobConf.setReducerClass(GraphPropertiesReducer.class);

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(jobConf);
	}
}
