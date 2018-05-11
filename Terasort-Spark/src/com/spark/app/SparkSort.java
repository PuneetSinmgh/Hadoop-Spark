package com.spark.app;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;


import scala.Tuple2;

public class SparkSort {
	
	
	public static void main(String args[]) {

		//JobConf conf = new JobConf();
		SortByTupleByValue sort = new SortByTupleByValue();
		
		JavaSparkContext sc =new JavaSparkContext();
			
		JavaRDD<String> lines = sc.textFile(args[0]);
		
		JavaPairRDD<String, String> pairs = lines.mapToPair(s-> new Tuple2<String, String>(s.substring(0, 10),s.substring(0, s.length()))).repartitionAndSortWithinPartitions(new Partitioner() {
			
			@Override
			public int numPartitions() {
				// TODO Auto-generated method stub
				return 0;
			}
			
			@Override
			public int getPartition(Object arg0) {
				// TODO Auto-generated method stub
				return 0;
			}
		}, new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				s1.compareTo(s2);
				return 0;
			}
		});
		
		//JavaPairRDD<String, String> orderedpairs = pairs.repartition(4).sortByKey();
		
		//int partitions = orderedpairs.partitions().size();
		
		JavaPairRDD<Tuple2<String, String>, Integer> secondarypairs = pairs.mapToPair(t -> new Tuple2<>(t, new Integer(1))).sortByKey(sort);
		
		//JavaPairRDD<Tuple2<String, String>, Integer> sortedsecondarypairs = secondarypairs.sortByKey(sort);
		
		JavaPairRDD<String, String> originalpairs = secondarypairs.mapToPair(s-> new Tuple2(s._1._1(), s._1._2()));
		
		//originalpairs.saveAsHadoopFile(args[1],Text.class, Text.class, TextOutputFormat.class);
		originalpairs.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, String> arg0) throws Exception {
			
				List<String> returnValues = new ArrayList<String>();
				returnValues.add(arg0._1()+arg0._2() + "\r");
				
				return returnValues.iterator();
			}
		}).saveAsTextFile(args[1]);
			
			
	}
		
}
