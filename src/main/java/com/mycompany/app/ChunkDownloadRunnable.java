package com.mycompany.app;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.types.ObjectId;

/**
 * Runnable responsible for downloading chunk files once the processing queue has under DOWNLOAD_THRESHOLD
 * tokens to consume.
 * @author Vydra
 *
 */
public class ChunkDownloadRunnable implements Runnable {
	
	/**
	 * Number of tokens in the process queue to (attempt to) start downloading the next chunk file
	 */
	public static final int DOWNLOAD_THRESHOLD = 1000;
	
	/**
	 * Chunk files that are currently being processed by this process
	 */
	public static final Set<ChunkFileMeta> processedChunks = new HashSet<>(); 
	
	private final DBConnection db;
	
	private String curChecksum;
	
	public ChunkDownloadRunnable(DBConnection db) {
		this.db = db;
	}
	
	@Override
	public void run() {
		try {
			while(!(Thread.currentThread().isInterrupted())) {
				if(ProcessRunnable.linesToProcess.size() < DOWNLOAD_THRESHOLD) {
					ChunkFile file = this.db.getNextChunkFile(this.getCurChecksum());
					if(file == null) {
						// get next target
						String next = db.getNextTargetFile();
						while(next == null) {
							Thread.sleep(5000);
							next = db.getNextTargetFile();
						}
						this.setChecksum(next);
						System.out.println("Processing new file: " + next);
						// get the file
						file = this.db.getNextChunkFile(this.getCurChecksum());
						if(file == null) {
							// all chunk files processed but final results not finalised
							// pass through a dummy 
							ChunkFileMeta dummy = new ChunkFileMeta(new ObjectId("000000000000000000000000"), 0);
							dummy.setChecksum(next);
							dummy.setNumOfLines(0);
							file = new ChunkFile(dummy, "");
						} else {
							System.out.println("Got a new chunk file");
						}
					}
			    	// register
					ProcessRunnable.reduceMap.put(file.getFileMeta().getId(), new ConcurrentHashMap<String, Integer>());
			    	ProcessRunnable.linesCounter.put(file.getFileMeta().getId(), 0);
			    	processedChunks.add(file.getFileMeta());
			    	
			    	// split and push
					String[] lines = file.getContent().split(System.lineSeparator());
			    	for(String line : lines) {
			    		ProcessRunnable.linesToProcess.add(new Pair<ChunkFileMeta, String>(file.getFileMeta(), line));
			    	}
				}
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void setChecksum(String checksum) {
		this.curChecksum = checksum;
	}
	
	private String getCurChecksum() {
		return this.curChecksum;
	}
}
