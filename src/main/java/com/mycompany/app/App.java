package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.TreeMap;

import com.mongodb.MongoClient;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.Block;
import org.bson.Document;
import org.bson.types.ObjectId;

public class App 
{
	// future args
	private static final String URI = "mongodb://localhost:27017";
//	private static final String FILE = "D:\\enwiki-latest-pages-articles-multistream.xml";
//	private static final String FILE = "test.txt";
	private static final String FILE = "";
	
    public static void main( String[] args )
    {
    	try {
	    	DBConnection db = new DBConnection(URI);
	    	List<Thread> processThreads = new ArrayList<>();
	    	for(int i = 0; i < ProcessRunnable.NUM_OF_PROCESS_THREADS; i++) {
	    		processThreads.add(new Thread(new ProcessRunnable()));
	    	}
	    	
	    	System.out.println("Running process threads");
	    	for(Thread t : processThreads) {
	    		t.start();
	    	}
	    	
	    	// upload file and see results
	    	if(FILE != null && !"".equals(FILE.trim())) {
	    		File file = new File(FILE);
		    	
		    	String checksum = db.uploadFile(file);
		    	
		    	processFile(checksum, db);
		    	
		    	// kill all process threads
		    	for(Thread t : processThreads) {
		    		t.interrupt();
		    	}
		    	
	    	} else {
	    		// run this as just a worker node until...forever
	    		//TODO make get this to stop on some user interrupt
	    		while(true) {
	    			Thread download = new Thread(new ChunkDownloadRunnable(new DBConnection(URI)));
	    			download.start(); // starts downloading chunk files
		    		
	    			// wait till next chunk file is processed
	    			ChunkFileMeta next = getNextProcessedMeta();
		    		while(next == null) {
		    			Thread.sleep(1000);
		    			next = getNextProcessedMeta();
		    		}
		    		
		    		// reduce it (inc upload)
		    		reducePartialResults(next, db);
	    			
		    		// final reduce it
		    		Document results = finalReduce(next.getChecksum(), db);
	    		}
	    	}
    	} catch (FileNotFoundException e) {
    		System.out.println("File not found, please check the path given");
//    		e.printStackTrace();
    	} catch (MongoTimeoutException e) {
    		//TODO this is being thrown from daemon and prints stack trace elsewhere, ideally disable that somehow
    		System.out.println("Connection to the database failed, please restart");
    	} catch (IOException e) {
//    		e.printStackTrace();
    		System.out.println("An unexpected error with processing the file occured");
    	} catch (InterruptedException e) {
			// this should never be thrown
			e.printStackTrace();
		}
    }
    
    /**
     * Splits the given file content and feeds it into ProcessRunnable.linesToProcess
     * @param file
     * @return Number of lines in the given file
     */
    public static int splitLines(ChunkFile file) {
    	String[] lines = file.getContent().split(System.lineSeparator());
    	for(String line : lines) {
    		ProcessRunnable.linesToProcess.add(new Pair<ChunkFileMeta, String>(file.getFileMeta(), line));
    	}
    	return lines.length;
    }
    
    /**
     * Fully processes a file identified by a checksum stored in a given database.  
     * @param checksum
     * @param db
     */
    public static void processFile(String checksum, DBConnection db) {
		try {
			// get and process all the chunk files
			ChunkFile fileToProcess;
			while((fileToProcess = db.getNextChunkFile(checksum)) != null) {
				ObjectId fileId = fileToProcess.getFileMeta().getId();
				
				// register the file with the map
				ProcessRunnable.reduceMap.put(fileId, new ConcurrentHashMap<String, Integer>());
				
				fileToProcess.getFileMeta().setNumOfLines((splitLines(fileToProcess)));
    	    	
    	    	ProcessRunnable.linesCounter.put(fileId, 0);
    	    	
    	    	while(!checkIfProcessingFinished(fileToProcess.getFileMeta())) {
    	    		Thread.sleep(1000);
    	    	}
    	    	// reduce the partial results
    	    	reducePartialResults(fileToProcess.getFileMeta(), db);
			}
			// finalise the results
			Document results = finalReduce(checksum, db);
			// display
			displayResults(results);
		} catch (InterruptedException e) {
			// won't be thrown
			System.out.println("Interrupted");
			e.printStackTrace();
		}
    }
    
    /**
     * Reduces local results for a given chunk file id and database connection
     * @param fileId
     * @param db
     */
    private static void reducePartialResults(ChunkFileMeta file, DBConnection db) {
    	System.out.println("Writing partial results to db");
    	ObjectId fileId = file.getId();
    	
    	Document results = db.getPartialResults(fileId);
    	if(results == null) {
    		// no results yet, lets save them
			// build the doc
			results = new Document("_fileId", fileId);
			for(String key : ProcessRunnable.reduceMap.get(fileId).keySet()) {
				results.append(key, ProcessRunnable.reduceMap.get(fileId).get(key));
			}
			db.writePartialResultsToDB(fileId, results);
    	} else {
    		// this won't happen naturally, only after manipulating the db manually, but prevents bugs
    		// if results are there, then surely the file was processed
    		if(file.getProcessed() == 0) {
    			db.setProcessed(file.getId(), 1);
    		}
    		//TODO what to do when results are already in db? check whether they're same and if not...?
    	}
		// clear the maps
		ProcessRunnable.reduceMap.get(fileId).clear();
		ProcessRunnable.linesCounter.remove(fileId);
    }
    
    /**
     * Ensures the reduction of the partial results into final one and uploads to the db. In case the final results
     * record already exists, downloads that instead.
     * @param checksum
     * @param db
     * @return Document with the results
     */
    private static Document finalReduce(String checksum, DBConnection db) {
    	// finalise the results
    	Document results = db.getFinalResults(checksum);
		if(results == null) {
			System.out.println("Processing done, final reducing the results");
			// download partial results
			List<Document> partialResults = db.getAllResults(checksum);
			// reduce
			results = reduceResults(partialResults);
			results.append("_fileChecksum", checksum);
			// upload the final results
			db.uploadFinalResult(results, checksum);
		} else {
			// this should not naturally occur, but fixes unwanted behaviour should 'finalised' be edited manually
			db.setFileFinalised(checksum, 1);
		}
		return results;
    }
    
    /**
     * Method to display the results.
     * @param results
     */
    public static void displayResults(Document results) {
    	Map<String, Object> map = new TreeMap<>();
    	map.putAll((Map<String, Object>)results);
    	
    	// remove metafields
    	map.remove("_id");
    	map.remove("_fileChecksum");
    	
    	// put results into a list (for easier results access)
    	List<Map.Entry<String, Object>> list = new ArrayList<>();
		list.addAll(map.entrySet());
		
		
		System.out.println((list.size() > 10) ? list.size()-1 : 10);
		List<Map.Entry<String, Object>> topCommon = list.subList(0, (list.size() < 10) ? list.size() : 10);
		List<Map.Entry<String, Object>> leastCommon = list.subList((list.size() < 10) ? 0 : list.size()-10, list.size());
		
		System.out.println("Top 10 common words: ");
		System.out.println(topCommon);
		
		System.out.println("Top 10 least common words: ");
		System.out.println(leastCommon);
    }
    
    /**
     * Checks whether the local processing of a given file has finished.
     * @param meta ChunkFileMeta file identifying the file.
     * @return true if processing finished, false otherwise
     */
	public static boolean checkIfProcessingFinished(ChunkFileMeta meta) {
    	return meta.getNumOfLines() <= ProcessRunnable.linesCounter.get(meta.getId());
    }
	
	/**
	 * Returns the ChunkFileMeta of the next processed chunk file or null if no such exists
	 * @return ChunkFileMeta
	 */
	public static ChunkFileMeta getNextProcessedMeta() {
		for(ChunkFileMeta meta : ChunkDownloadRunnable.processedChunks) {
			System.out.println(Arrays.toString(ProcessRunnable.linesCounter.keySet().toArray()));
			if(meta.getNumOfLines() <= ProcessRunnable.linesCounter.get(meta.getId())) {
				ChunkDownloadRunnable.processedChunks.remove(meta);
				return meta;
			}
		}
    	return null;
    }
	
	/**
	 * Reduces results into a single Document. All Documents in the list provided should be of String => Integer mapping.
	 * @param results List of partial results
	 * @return Document containing final results
	 */
	private static Document reduceResults(List<Document> results) {
        // final reduce into map
        Map<String, Integer> finalReduceMap = new HashMap<>();
        for(Document doc : results) {
        	for(String word : doc.keySet()) {
        		if(!("_id".equals(word) || "_fileId".equals(word))) {
        			finalReduceMap.merge(word, doc.getInteger(word), (oldValue, newValue) -> oldValue + newValue);
        		}
        	}
        }
        
        // into document
        Document finalResults = new Document();
        for(String key : finalReduceMap.keySet()) {
        	finalResults.append(key, finalReduceMap.get(key));
        }
        return finalResults;

	}
}
