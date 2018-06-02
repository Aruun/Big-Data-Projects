package com.spark.Assignment1;

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

public class SingleRowLookup {

	public static void main(String[] args) {
			long starttime = System.currentTimeMillis();
		
		SparkConf conf = new SparkConf().setAppName("SingleRowLookup").setMaster("local[*]");
		
		 // Create a Java version of the Spark Context from the configuration
      
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Load the input data, which is a text file read from the command line
		JavaRDD<String> Datafile = sc.textFile("C:/Users/Admin/eclipse-workspace/SparkAssignment/in/trip_yellow_taxi.data");
		
		//splitting the column using delimeter "," and get VendorID, Pickuptime, Drop time, Passenger_count and Trip Distance and displaying the value
		Datafile.map(x->x.split(",")).filter(x -> x[0].contains("2") && x[1].contains("2017-10-01 00:15:30") && x[2].contains("2017-10-01 00:25:11") && x[3].contains("1") && x[4].contains("2.17")).map(x -> Arrays.toString(x)).foreach(x->System.out.println(x));
	
		//Datefile.saveAsTextFile("C:/Temp");
		//Close of Spark Context
		sc.close();
		long endtime = System.currentTimeMillis();
		
		//Running Time which denotes EndTime
		System.out.println(starttime + "-------------" + endtime);

	}

}
