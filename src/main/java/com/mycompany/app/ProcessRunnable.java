package com.mycompany.app;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.types.ObjectId;

/**
 * A Runnable processing parts of the chunk files.
 * 
 * @author Vydra
 *
 */
public class ProcessRunnable implements Runnable {
	
	/**
	 * Stores parts of the file yet to be processed by the ProcessRunnable.
	 */
	public static final BlockingQueue<Pair<ChunkFileMeta, String>> linesToProcess = new LinkedBlockingQueue<>();
	
	/**
	 * Stores results of the given chunk file.
	 */
	public static final Map<ObjectId, Map<String, Integer>> reduceMap = new ConcurrentHashMap<>();
	
	/**
	 * Counter used to determine whether processing of a given chunk file has been completed.
	 */
	public static final Map<ObjectId, Integer> linesCounter = new ConcurrentHashMap<>();

	@Override
	public void run() {
		while(!(Thread.currentThread().isInterrupted())) {
			// count words in the line
			Pair<ChunkFileMeta, String> pair;
			try {
				pair = linesToProcess.take();
				if(pair != null) {
					String line = pair.getSecond().trim();
					// otherwise split(" ") doesn't work 
					if(!("".equals(line.trim()))) {
						for(String word : line.split(" ")) {
							word = word.trim(); // in case of multi spaces ('  ')
							reduceMap.get(pair.getFirst().getId()).merge(word, 1, (oldValue, one) -> oldValue + one);
						}	
					}
					// increase the 'lines counted per file' counter
					linesCounter.merge(pair.getFirst().getId(), 1, (oldValue, one) -> oldValue + one);
				}
			} catch (InterruptedException e) {
                Thread.currentThread().interrupt();
			}
		}
	}
}
