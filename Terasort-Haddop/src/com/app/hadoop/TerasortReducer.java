package com.app.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TerasortReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {

	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
		ArrayList<Text> list = new ArrayList<>();
		
	while (values.hasNext()) {
			
		list.add( values.next()); 
		
		}
		
	Collections.sort(list);
	
	for(Text t : list) {
		
		output.collect(key, t);
	}
	list.clear();
	
	}

}
