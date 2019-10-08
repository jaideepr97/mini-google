package com.sparkinvertedindex;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class SparkInvertedIndex {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf = new SparkConf().setAppName("FirstApp").setMaster("local[*]");
		String contentPath = "hdfs://localhost:8020/docfiles/";
		String id_url_path = "hdfs://localhost:8020/id_url_pairs/id_URL_pairs.txt";
		JavaSparkContext sc = new JavaSparkContext(conf);
		//Get id-url pairs
		JavaPairRDD<String, String> idUrlPairs = sc.textFile(id_url_path).mapToPair(data ->
		{
			String[] temp = data.split(",");
			return new Tuple2<>(temp[0]+".txt", temp[1]);
		});
		List<Tuple2<String, String>> idUrlPairsTest = idUrlPairs.take(10);
		//Get all the input files as (docPath, content)
		JavaPairRDD<String, String> inputdocs = sc.wholeTextFiles(contentPath+"*");
		inputdocs = inputdocs.mapToPair(data -> new Tuple2<>(data._1.substring(contentPath.length()), data._2));
		//List<Tuple2<String, String>> inputDocsTest = inputdocs.take(10);
		//Join idUrlPairs with inputdocs
		JavaPairRDD<String, Tuple2<String, String>> docIDToURLContent = inputdocs.join(idUrlPairs);
		//List<Tuple2<String, Tuple2<String, String>>> test = docIDToURLContent.take(2);
		/*
		for(Tuple2<String, Tuple2<String, String>> r : test)
		{
			System.out.println("DocID:" + r._1);
			System.out.println("Content: " + r._2._1);
			System.out.println("URL: " + r._2._2);

		}

		 */
		JavaPairRDD<String, String> urlToContent = docIDToURLContent.mapToPair(data -> new Tuple2<String, String>(data._2._2, data._2._1));
		//List<Tuple2<String, String>> testUrlToContent = urlToContent.take(10);
		//Convert it to an rdd as (url, List of words)
		JavaPairRDD<String, List<String>> docToWords = urlToContent.mapToPair(data -> new Tuple2<>(data._1, Arrays.asList(data._2().split(" "))));
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
		RocksDB.loadLibrary();
		String RocksdbPath = "/Users/aayushgupta/IdeaProjects/data/";

		List<Tuple2<String, Iterable<Tuple2<String, Integer>>>> invertedIndex = wordToDocCountGrouped.collect();
		ArrayList<byte[]> word= new ArrayList<>();
		ArrayList<byte[]> URL= new ArrayList<>();
		for(Tuple2<String, Iterable<Tuple2<String, Integer>>> r : invertedIndex)
		{
			word.add(r._1.getBytes());
			for(Tuple2<String, Integer> d : r._2) {
				URL.add(d._1().getBytes());
			}

		}

 		try (final Options options = new Options().setCreateIfMissing(true)) {

		    // a factory method that returns a RocksDB instance
		    try (final RocksDB db = RocksDB.open(options, RocksdbPath)) {
		    	for(int i=0; i<word.size(); i++) {

		    		db.put(word.get(i), URL.get(i));
		    	}
		        // do something
		    }
		  } catch (RocksDBException e) {
 			System.out.println(e.toString());
		    // do some error handling

		  }


	}

}
