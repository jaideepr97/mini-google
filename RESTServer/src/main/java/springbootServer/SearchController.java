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
	public List<List<String>> getURLs(@RequestParam("query") String query) throws Exception {
//		HashMap<String, String> idURLpairs = new HashMap<>();
//		final Path path = new Path("/user/id_URL_pairs.txt");
//		try(final DistributedFileSystem dFS = new DistributedFileSystem() {
//			{
//				initialize(new URI("hdfs://localhost:9000"), new Configuration());
//			}
//		};
//
//		final FSDataInputStream streamReader = dFS.open(path);
//		final Scanner scanner = new Scanner(streamReader);) {
//
//			while(scanner.hasNextLine()) {
//				idURLpairs.put(scanner.nextLine().split(",")[0], scanner.nextLine().split(",")[1]);
//			}
//
//		}

		RocksDB.loadLibrary();
		System.out.println(query);
		List<String> wordList = Arrays.asList(query.split(" "));
		HashSet<String> finalQuery = new HashSet<>();
		for(String x: wordList) {
			finalQuery.add(x.toLowerCase());
		}
		List<List<String>> URLList = new ArrayList<>();
		String RocksdbPath = "/Users/aayushgupta/IdeaProjects/data/";
		Options rockopts = new Options();
		RocksDB db = null;
		try {
	        db = RocksDB.open(rockopts, RocksdbPath);
	        RocksIterator iter = db.newIterator();
	        iter.seekToFirst();
	        while (iter.isValid()) {
	            String key = new String(iter.key(), StandardCharsets.UTF_8);
	            if(finalQuery.contains(key))
				{
					List<String> val = new ArrayList<>();
					String temp = new String(iter.value(), StandardCharsets.UTF_8);
					URLList.add(new ArrayList<>(val));
				}
	            iter.next();
	        }

	        db.close();
	    } catch (RocksDBException rdbe) {
	        rdbe.printStackTrace(System.err);
	    }

	    return URLList;
	}

}
