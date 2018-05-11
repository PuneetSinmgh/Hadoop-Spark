package com.spark.app;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;


public class SortByTupleByValue implements Comparator<Tuple2<String,String>>,Serializable{

	public class Cmp implements Comparator<String>, Serializable{

		@Override
		public int compare(String o1, String o2) {
		
			return o1.compareTo(o2);
		}	
	}
	
	private Comparator<String> copmstring = new Cmp();

	/*	
		@Override
		public int compare(String arg0, String arg1) {
			// TODO Auto-generated method stub
			return arg0.compareTo(arg1);
		}
	};
	
	*/
		
	@Override
	public int compare(Tuple2<String, String> T1, Tuple2<String, String> T2) {
		
		int res = T1._1().compareTo(T2._1());
		if (res==0) {
			return copmstring.compare(T1._2(), T2._2());
		}
		else
		return res;
	}
	
}

