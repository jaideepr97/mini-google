package com.virtualpairprogrammers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("FirstApp").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
//		JavaRDD<String> inputdocs = sc.textFile("hdfs://localhost:9000/user/Project1_data/*");
//		JavaPairRDD<String, Integer> words = inputdocs.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(word -> new Tuple2<String, Integer>(word, 1))
//        .reduceByKey((x, y) ->  x +  y)
//        .sortByKey();
//		words.foreach(data -> {
//	        System.out.println(data._1()+"-"+data._2());
//	    });
		JavaPairRDD<String, String> inputdocs = sc.wholeTextFiles("hdfs://localhost:9000/user/Project1_data/*");

		JavaPairRDD<List<String>, String> sentences = inputdocs.mapToPair(data -> new Tuple2< List<String>, String>(Arrays.asList(data._2().split(" ")), data._1()));
//		JavaPairRDD<String, String> words = sentences.mapToPair(data ->  data._1().forEach(item -> {new Tuple2<String, String> (item, data._2()); } )); 
		List<Tuple2<String, String>> temp = new ArrayList<>();
		sentences.foreach(sentence -> {
			List<String> sent = sentence._1();
			
			sent.forEach(word -> {
				
				temp.add(new Tuple2<String, String> (word.toLowerCase(), sentence._2()));
			});
		});
		JavaRDD<Tuple2<String, String>> rdd = sc.parallelize(temp);
		JavaPairRDD<String, String> words = JavaPairRDD.fromJavaRDD(rdd);
//		JavaPairRDD<String, String> words = sentences.
		words.foreach(data -> {
	        System.out.println(data._1()+"-"+data._2());
	    });
	}

}
