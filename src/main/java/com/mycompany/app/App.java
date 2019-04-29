package com.mycompany.app;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoTimeoutException;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class App 
{
	public static String URI;
	public static String FILE = "";
	public static int NUM_OF_PROCESS_THREADS;
	public static int CHUNK_SIZE;
	public static int VIEW;
	
    public static void main( String[] args )
    {
    	// argument parsing
		 ArgumentParser parser = ArgumentParsers.newFor("Wordcount").build()
	                .defaultHelp(true)
	                .description("Count words in the file");
		 	parser.addArgument("-m", "--mongo")
		 			.dest("mongo")
		     		.type(String.class)
		     		.setDefault("mongodb://localhost:27017")
		            .help("MongoDB URI (e.g. mongodb://localhost:27017)");
		 	parser.addArgument("-f", "--file")
		 			.dest("file")
	        		.type(String.class)
	        		.setDefault("")
	                .help("Input file");
	        parser.addArgument("-w", "--workers")
	        		.dest("workers")
	        		.type(Integer.class)
	        		.setDefault(4)
	                .help("Number of processing threads, default: 4");
	        parser.addArgument("--chunk-size")
	        		.dest("chunkSize")
	        		.type(Integer.class)
	        		.setDefault(16)
	                .help("Chunk file size (MB), default: 16");
	        parser.addArgument("-v")
		    		.dest("view")
		    		.type(Integer.class)
		    		.setDefault(0)
		            .help("View the top and least X results in the console, 0 for viewing all results (desc), -1 for none, default: 0");
    	try {
	        Namespace res = parser.parseArgs(args);
            URI = res.getString("mongo");
            FILE = res.getString("file");
            NUM_OF_PROCESS_THREADS = res.getInt("workers");
            CHUNK_SIZE = res.getInt("chunkSize");
            VIEW = res.getInt("view");
            
            // setup 
	    	DBConnection db = new DBConnection(URI);
	    	List<Thread> processThreads = new ArrayList<>();
	    	for(int i = 0; i < NUM_OF_PROCESS_THREADS; i++) {
	    		processThreads.add(new Thread(new ProcessRunnable()));
	    	}
	    	
	    	// run process threads
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
	    		Thread download = new Thread(new ChunkDownloadRunnable(new DBConnection(URI)));
    			download.start(); // starts downloading chunk files
    			System.out.println("Running...");
    			// run this as just a worker node until...forever
	    		//TODO make get this to stop on some user interrupt
	    		while(true) {
	    			// wait till next chunk file is processed
	    			ChunkFileMeta next = getNextProcessedMeta();
		    		while(next == null) {
		    			Thread.sleep(1000);
		    			next = getNextProcessedMeta();
		    		}
		    		
		    		System.out.println("Writing partial results to db");
	    			reduceAndSavePartialResults(next, db);
	    			
		    		// final reduce it
	    			if(db.isToBeFinalised(next.getChecksum())) {
		    			System.out.println("Processing done, reducing to get the final results");
			    		Document results = finalReduce(next.getChecksum(), db);
			    		
			    		// save
			    		db.uploadFinalResult(results, next.getChecksum());
	    			}
	    		}
	    	}
    	} catch (FileNotFoundException e) {
    		System.out.println("File not found, please check the path given");
//    		e.printStackTrace();
    	} catch (MongoTimeoutException e) {
    		System.out.println("Connection to the database failed, please try again");
    	} catch (IOException e) {
//    		e.printStackTrace();
    		System.out.println("An unexpected error with processing the file occured");
    	} catch (InterruptedException e) {
    		Thread.currentThread().interrupt();
			e.printStackTrace();
	    } catch (ArgumentParserException e) {
	        parser.handleError(e);
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
				
				splitLines(fileToProcess);
				
    	    	ProcessRunnable.linesCounter.put(fileId, 0);
    	    	
    	    	while(!checkIfProcessingFinished(fileToProcess.getFileMeta())) {
    	    		Thread.sleep(1000);
    	    	}
    	    	// reduce the partial results
    	    	System.out.println("Writing partial results to db");
    	    	reduceAndSavePartialResults(fileToProcess.getFileMeta(), db);
			}
			// finalise the results
			System.out.println("Processing done, reducing to get the final results");
			Document results = finalReduce(checksum, db);
			// display
			if(VIEW > 0) {
				displayResults(results, VIEW);
			}
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
    private static void reduceAndSavePartialResults(ChunkFileMeta file, DBConnection db) {
    	ObjectId fileId = file.getId();
    	
    	Document results = db.getPartialResults(fileId);
    	if(results == null) {
    		// no results yet, lets save them
			// build the doc
			List<Document> wordResults = new ArrayList<>();
			for(String key : ProcessRunnable.reduceMap.get(fileId).keySet()) {
				wordResults.add(new Document("word", key).append("value", ProcessRunnable.reduceMap.get(fileId).get(key)));
			}
			results = new Document("_fileId", fileId).append("checksum", file.getChecksum()).append("results", wordResults);
			// save
			if(!wordResults.isEmpty())
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
		ProcessRunnable.reduceMap.remove(fileId);
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
			// download partial results
			List<Document> partialResults = db.getAllPartialResults(checksum);
			// reduce
			results = reduceResults(partialResults);
			results.append("checksum", checksum);
			// upload the final results
			db.uploadFinalResult(results, checksum);
		} else {
			// this should not naturally occur, but fixes unwanted behaviour should 'finalised' be edited manually
			db.setFileFinalised(checksum, 1);
		}
		return results;
    }
    
    /**
     * Method to display the sorted results.
     * @param results Final results document
     * @param topX Number of most and least counted words
     */
	public static void displayResults(Document results, int topX) {
    	@SuppressWarnings("unchecked")
		List<Document> wordResults = ((List<Document>) results.get("results"));
    	wordResults.sort(new Comparator<Document> () {
			@Override
			public int compare(Document d1, Document d2) {
				return d1.getInteger("value", 0) - d2.getInteger("value", 0);
			}
    	});
    	
		if(topX == 0) {
			// display all
			System.out.format("%s%30s%n", "word", "count");
			for(Document d : wordResults) {
				System.out.format("%s%30d%n", d.getString("word"), d.getInteger("value"));
			}
		} else {
			List<Document> top = wordResults.subList(0, (wordResults.size() < topX) ? wordResults.size() : topX);
	    	List<Document> bot = wordResults.subList((wordResults.size() < topX) ? 0 : wordResults.size()-topX, wordResults.size());
	    	
			System.out.format("Top %d common words:%n", topX);
			System.out.format("%s%30s%n", "word", "count");
			for(Document d : top) {
				System.out.format("%s%30d%n", d.getString("word"), d.getInteger("value"));
			}
			System.out.format("Top %d least common words:%n", topX);
			System.out.format("%s%30s%n", "word", "count");
			for(Document d : bot) {
				System.out.format("%s%30d%n", d.getString("word"), d.getInteger("value"));
			}	
		}
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
		// flatten
		List<Document> flatten = new ArrayList<>();
		for(Document doc : results) {
			flatten.addAll((List<Document>) doc.get("results"));
		}
		
        // final reduce into sorted map
        Map<String, Integer> finalReduceMap = new HashMap<>();
        for(Document doc : flatten) {
			finalReduceMap.merge(doc.getString("word"), doc.getInteger("value"), (oldValue, newValue) -> oldValue + newValue);
        }
        
        // into document
        List<Document> resultsByWords = new ArrayList<>();
        for(String key : finalReduceMap.keySet()) {
        	resultsByWords.add(new Document("word", key).append("value", finalReduceMap.get(key)));
        }
        Document finalResults = new Document("results", resultsByWords);
        
        return finalResults;

	}
}
