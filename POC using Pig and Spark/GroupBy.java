package com.spark.Assignment2;

/**
 *  Name: Arun Kumar N
	Student ID : 2017CBDE030
	Spark Assignment
 *
 */
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class GroupBy {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Running time which denotes the start time
		long starttime = System.currentTimeMillis();
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setAppName("Filter Operation").setMaster("local[*]");
		
		 // Create a Java version of the Spark Context from the configuration
	    
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> Datafile = sc.textFile("C:/Users/Admin/eclipse-workspace/SparkAssignment/in/trip_yellow_taxi.data");
		
		//splitting the column using delimeter "," and get x[5] column which is ratecodeID and check whether it contains 4 and printing the values
		Datafile.map(x->x.split(",")).filter(x -> x[5].contains("4")).map(x -> Arrays.toString(x)).foreach(x->System.out.println(x));
		
		//Close of Spark Context
		sc.close();
		
		//Running time denotes the end time
		long endtime = System.currentTimeMillis();
		
		System.out.println(starttime + "-------------" + endtime);

	}

}
