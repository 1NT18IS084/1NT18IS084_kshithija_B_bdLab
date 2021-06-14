package my.pack.fourteen;

//Total number of people who paid tax

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import my.pack.fourteen.Empamountproj.Map;
import my.pack.fourteen.Empamountproj.Reduce;


public class Ecountproj {

	//MAPPER CODE	
		   
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	
	// the variable one holds the value 1 in the form of Hadoop's IntWritable Object
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String myvalue = value.toString();
		String[] tokens = myvalue.split(",");
//		if (tokens[5] == "Yes")
		if(tokens[4].matches("Yes")) {
			output.collect(new Text(tokens[4]), one);
		}
		}
	}

	//REDUCER CODE	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
			int transcount = 0;
			while(values.hasNext()) {
				transcount += values.next().get();
			}
			output.collect(new Text("Tax Paid"), new IntWritable(transcount));
		}
	}

	//DRIVER CODE
	public static void main(String[] args) throws Exception {
	
		JobConf conf = new JobConf(Ecountproj.class);
		conf.setJobName("Various Operations");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class); 
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);   
	}
}
