package springbootServer;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.charset.StandardCharsets;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


@RestController
public class SearchController {

	@RequestMapping("/search")
	@ResponseBody
	public List<String> getURLs(@RequestParam("query") String query) throws Exception {

		RocksDB.loadLibrary();
		System.out.println(query);
		List<String> preprocessedWords = Arrays.asList(query.split(" "));
		List<String> wordList = new ArrayList<>();
		for(String x: preprocessedWords) {
			String z = x.replaceAll("[ ](?=[ ])|[^-_A-Za-z0-9 ]+", "")
			wordList.add(z);		
		}
		HashSet<String> finalQuery = new HashSet<>();
		for(String x: wordList) {
			finalQuery.add(x.toLowerCase());
		}
		List<List<String>> urlList = new ArrayList<>();
		// Add absolute path to where the RocksDB files are stored
		String RocksdbPath = "/Users/aayushgupta/IdeaProjects/data/";
		Options rockopts = new Options();
		try {
	        final RocksDB db = RocksDB.open(rockopts, RocksdbPath);
	        RocksIterator iter = db.newIterator();
	        iter.seekToFirst();
	        while (iter.isValid()) {
	            String key = new String(iter.key(), StandardCharsets.UTF_8);
	            if(finalQuery.contains(key) || finalQuery.contains(key.toLowerCase()) || finalQuery.contains(key.toUpperCase()))
				{
					String temp = new String(iter.value(), StandardCharsets.UTF_8);
					String[] tempArray = temp.split(" ");
					List<String> tempList = new ArrayList<>(Arrays.asList(tempArray));
					urlList.add(tempList);
				}
	            iter.next();
	        }
			iter.close();
	        db.close();
	    } catch (RocksDBException rdbe) {
	        rdbe.printStackTrace(System.err);
	    }

			HashSet<String> finalURLset = new HashSet<>();
			for(List<String> s: urlList) {
				for(String x: s) {
					finalURLset.add(x);
				}
			}
			List<String> finalURLlist = new ArrayList<String>(finalURLset);
	    return finalURLlist;
	}

}
