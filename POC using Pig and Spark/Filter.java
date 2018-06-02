package com.spark.Assignment3;

/**
 *  Name: Arun Kumar N
	Student ID : 2017CBDE030
	Spark Assignment
 *
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Filter {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
long starttime = System.currentTimeMillis();
		
		SparkConf conf = new SparkConf().setAppName("GroupBy").setMaster("local[*]");
		
		 // Create a Java version of the Spark Context from the configuration
       
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> Datafile = sc.textFile("C:/Users/Admin/eclipse-workspace/SparkAssignment/in/trip_yellow_taxi.data");
		
		//Extracting the InputData without Header
		JavaRDD<String> DatafileWithoutHeader = Datafile.zipWithUniqueId().filter(x -> x._2 != 0).map(x -> x._1);
				
			
		
		//Splitting the Data by delimeter "," and loading the column x[9] that is 
		JavaRDD<String> PayRDD = DatafileWithoutHeader.map(x->x.split(",")).map(x->x[9]);
		
		JavaPairRDD<String,Integer> pairRDD = PayRDD.mapToPair(
				x -> new Tuple2<String,Integer>(x,1)
				);
		
		//pairRDD.foreach(x->System.out.println(x._1+":"+x._2));
		
		//Getting the count of Payment Type
		
		JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey(
				(x,y) -> x+y
				);
		
		//To sort in ascending order, change the key to value and value to key
		JavaPairRDD<Integer,String> sortRDD =  countRDD.mapToPair( x -> new Tuple2<Integer,String>(x._2,x._1));
		
		//Sorting in Ascending Order 
		sortRDD.sortByKey().foreach(x->System.out.println(x._2+":"+x._1));
		
		
//		countRDD.foreach(x->System.out.println(x._1+":"+x._2));
		
		//Running time which denotes end time
		long endtime = System.currentTimeMillis();
		
		//Close of Spark Context
		sc.close();
		System.out.println(starttime +"-------------" + endtime );

	}

}
