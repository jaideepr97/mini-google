package com.virtualpairprogrammers;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.*;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("FirstApp").setMaster("local[*]");
		String path = "hdfs://localhost:8020/docfiles/";
		JavaSparkContext sc = new JavaSparkContext(conf);
		//Get all the input files as (docPath, content)
		JavaPairRDD<String, String> inputdocs = sc.wholeTextFiles(path+"*");
		//Convert it to an rdd as (docPath, List of words)
		JavaPairRDD<String, List<String>> docToWords = inputdocs.mapToPair(data -> new Tuple2<>(data._1.substring(path.length()), Arrays.asList(data._2().split(" "))));
		//Reverse it as (List of words, docPath)
		JavaPairRDD<List<String>, String> wordsToDoc = docToWords.mapToPair(data -> new Tuple2<>(data._2(), data._1()));
		//Flatten it to (word, docPath)
		JavaPairRDD<String, String> flatWordsToDoc = wordsToDoc.flatMapToPair(data ->
		{
			List<String> _words = data._1;
			String _doc = data._2;
			List<Tuple2<String, String>> _result = new ArrayList();
			for(String _w : _words)
			{
				_result.add(new Tuple2<>(_w, _doc));
			}
			return _result.iterator();
		});
		//Optional - Remove special characters from flatWordsToDoc
		flatWordsToDoc = flatWordsToDoc.mapToPair(data -> new Tuple2<>(data._1.replaceAll("[ ](?=[ ])|[^-_A-Za-z0-9 ]+", ""),data._2()));
		//Creating a new rdd as ((word, doc), 1) - starting the process of calculating wordcount
		JavaPairRDD<Tuple2<String, String>, Integer> wordDocToOne = flatWordsToDoc.mapToPair(data -> new Tuple2<>(new Tuple2<>(data._1, data._2),1));
		//Grouping all the (word, doc) pairs and summing the word count
		JavaPairRDD<Tuple2<String, String>, Integer> wordDocToCount = wordDocToOne.reduceByKey((x,y) -> x+y);
		//Converting the tuple ((word,docPath), count) to (word, (docPath,count))
		JavaPairRDD<String, Tuple2<String, Integer>> wordToDocCount = wordDocToCount.mapToPair(data -> new Tuple2<>(data._1._1, new Tuple2<>(data._1._2, data._2)));
		// Grouping by words
		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> wordToDocCountGrouped = wordToDocCount.groupByKey();
		List<Tuple2<String, Iterable<Tuple2<String, Integer>>>>result = wordToDocCountGrouped.take(10);
		for(Tuple2<String, Iterable<Tuple2<String, Integer>>> r : result)
		{
			System.out.println("Word:" + r._1);
			System.out.println("Docs | Counts");
			for(Tuple2<String, Integer> d : r._2)
				System.out.println(d._1 + " " + d._2);
			System.out.println(".........................................");
		}
		//JavaPairRDD<List<String>, String> sentences = inputdocs.mapToPair(data -> new Tuple2< List<String>, String>(Arrays.asList(data._2().split(" ")), data._1()));
		/*
		List<Tuple2<String, String>> result = inputdocs.collect();
		for (Tuple2<String, String> res : result) {
			// Note that the paths from `wholeTextFiles` are in URI format on Windows,
			// for example, file:/C:/a/b/c.
			System.out.println(res._1 + "-" + res._2);
		}
		*/
	}

}
