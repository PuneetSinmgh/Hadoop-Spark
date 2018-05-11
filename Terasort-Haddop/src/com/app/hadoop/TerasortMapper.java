package com.app.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;


public class TerasortMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable k, Text v, OutputCollector<Text,Text > outputpair, Reporter reporter) throws IOException {
		
	String s = v.toString();
	
	String key = s.substring(0, 10);
	String value =  s.substring(10, s.length());
	
	outputpair.collect(new Text(key), new Text(value));
		
	}	
}
