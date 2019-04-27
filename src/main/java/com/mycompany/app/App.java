package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
	private static final String FILE = "kody_k_hram.txt";
	
	
    public static void main( String[] args )
    {
    	try {
	    	DBConnection db = new DBConnection(URI);
	    	List<Thread> processThreads = new ArrayList<>();
	    	for(int i = 0; i < ProcessThread.NUM_OF_PROCESS_THREADS; i++) {
	    		processThreads.add(new Thread(new ProcessThread()));
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
		    		String checksum = db.getNextTargetFile();
		    		while(checksum == null) {
		    			Thread.sleep(5000); //TODO move to config
		    			checksum = db.getNextTargetFile();
		    		}
		    		processFile(checksum, db);
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
    
    // splits the file into lines and feeds them into the queue
    public static int splitLines(ChunkFile file) {
    	String[] lines = file.getContent().split(System.lineSeparator());
    	for(String line : lines) {
    		ProcessThread.linesToProcess.add(new Pair<ChunkFileMeta, String>(file.getFileMeta(), line));
    	}
    	return lines.length;
    }
    
    public static void processFile(String checksum, DBConnection db) {
    	while(true) {
    		try {
    			ChunkFile fileToProcess = db.getNextChunkFile(checksum);
    	    	fileToProcess.getFileMeta().setNumOfLines((splitLines(fileToProcess)));
    	    	
    	    	ProcessThread.linesCounter.put(fileToProcess.getFileMeta(), 0);
    	    	
//    	    	System.out.println(fileToProcess.getFileMeta());
    	    	while(!checkIfProcessingFinished(fileToProcess.getFileMeta())) {
    	    		Thread.sleep(500);
    	    	}
    	    	
    	    	System.out.println("Writing results to db");
    	    	db.writeReducedResultsToDB(fileToProcess.getFileMeta());
//    	    	System.out.println("Processed ChunkFile");
    	    	
    		} catch (VydraNoChunkFilesFoundException e) {
    			Document results = db.getFinalResults(checksum);
    			if(results == null) {
    				System.out.println("Processing done, final reducing the results");
    				results = db.finalReduce(checksum);
    			}
//    			displayResults(results);
    			break;
    		} catch (InterruptedException e) {
    			// won't be thrown
				System.out.println("Interrupted");
				e.printStackTrace();
			}
    	}
    }
    
	public static boolean checkIfProcessingFinished(ChunkFileMeta meta) {
    	return meta.getNumOfLines() <= ProcessThread.linesCounter.get(meta);
    }
}
