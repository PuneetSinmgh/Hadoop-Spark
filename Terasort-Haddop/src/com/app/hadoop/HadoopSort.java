package com.app.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.htrace.shaded.fasterxml.jackson.databind.jsontype.SubtypeResolver;



public class HadoopSort {

	
	
	public static class TerasortMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {

		public TerasortMapper() {
			
		}
		
		
		@Override
		public void map(LongWritable k, Text v, OutputCollector<Text,Text > outputpair, Reporter reporter) throws IOException {
			
		//String s = v.toString();
		
		//String key = s.substring(0, 10);
		//String value =  s.substring(10, s.length());
		
		outputpair.collect(new Text(v.toString().substring(0, 10)), new Text(v.toString().substring(0, v.toString().length())));
			
		}	
	}
	
	
	public static class TerasortReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {

		
		public TerasortReducer() {
		
		}
		
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
			//ArrayList<Text> list = new ArrayList<>();
			
			while (values.hasNext()) {
				
			//list.add( values.next()); 
				output.collect(key, new Text(values.next()+"\r\n"));
				}

		}
	}
	
public static class ValueComparator extends WritableComparator{
	
	 public ValueComparator() {
		
		 super(Text.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text t1 = (Text)a;
		Text t2 = (Text)b;
		return super.compare(t1, t2);
	}
	
}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		JobConf conf = new JobConf(HadoopSort.class);
		JobClient client = new JobClient();
		
		//Job job = new Job(conf);
		
		//job.getJobConf();
		
		FileInputFormat.setInputPaths(conf, args[0]);
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		
		//Path partionOutpath = new Path(args[1]);
		conf.setJobName("TeraSot Hadoop");
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		//TotalOrderPartitioner.setPartitionFile(conf, partionOutpath);
			
		conf.setOutputValueGroupingComparator(ValueComparator.class);

		conf.setNumReduceTasks(8);
		
		conf.setMapperClass(TerasortMapper.class);
		
		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(CompressionCodec.class);
		
		conf.setCombinerKeyGroupingComparator(ValueComparator.class);
		
		conf.setCombinerClass(TerasortReducer.class);
		
		conf.setReducerClass(TerasortReducer.class);
		
		
		conf.setOutputKeyClass(Text.class);	
		conf.setOutputValueClass(Text.class);
		
		
		//InputSampler.Sampler<Text, Text> sample = new InputSampler.RandomSampler(.01, 10000);
		//InputSampler.writePartitionFile(conf, sample);
		
		//conf.setPartitionerClass( (Class<? extends Partitioner>) TotalOrderPartitioner.class);
		
		client.setConf(conf);
		
		JobClient.runJob(conf);
		
		}	
	
}
