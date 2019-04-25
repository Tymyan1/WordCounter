package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.Block;
import org.bson.Document;

public class App 
{
	// future args
	private static final String URI = "mongodb://localhost:27017";
	private static final String FILE = "D:/kody_k_hram.txt";
	
	
    public static void main( String[] args )
    {
    	
    	DBConnection db = new DBConnection(URI);
    	List<Thread> processThreads = new ArrayList<>();
    	for(int i = 0; i < ProcessThread.NUM_OF_PROCESS_THREADS; i++) {
    		processThreads.add(new Thread(new ProcessThread()));
    	}
    	
    	System.out.println("Running process threads");
    	for(Thread t : processThreads) {
    		t.start();
    	}
    	
    	System.out.println("Connecting to db");
    	db.connect();
    	System.out.println("Uploading the file");
    	db.uploadFile(FILE);
    	
    	System.out.println("File uploaded");
    	while(true) {
    		try {
    			System.out.println("Getting next ChunkFile");
    			ChunkFile fileToProcess = db.getNextChunkFile("kody_k_hram.txt");
    	    	fileToProcess.setNumOfLines(splitLines(fileToProcess));
    	    	
    	    	ProcessThread.linesCounter.put(fileToProcess.getFileMeta(), 0);
    	    	System.out.println(fileToProcess.getFileMeta());
    	    	while(!checkIfProcessingFinished(fileToProcess.getFileMeta())) {
    	    		Thread.sleep(1000);
    	    	}
    	    	
    	    	System.out.println("Writing results to db");
    	    	db.writeReducedResultsToDB(fileToProcess.getFileMeta());
    	    	System.out.println("Processed ChunkFile");
    	    	
    		} catch (VydraNoChunkFilesFoundException e) {
    			System.out.println("Finished");
    		} catch (InterruptedException e) {
				System.out.println("Interrupted");
				e.printStackTrace();
			}
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
    
    
	public static boolean checkIfProcessingFinished(ChunkFileMeta meta) {
		if (meta == null) System.out.println("a");
		if (ProcessThread.linesCounter == null) System.out.println("b");
		System.out.println(ProcessThread.linesCounter.get(meta));
    	return meta.getNumOfLines() <= ProcessThread.linesCounter.get(meta);
    }
}
