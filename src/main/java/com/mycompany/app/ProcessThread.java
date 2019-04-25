package com.mycompany.app;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.bson.types.ObjectId;

public class ProcessThread implements Runnable {

	public static final int NUM_OF_PROCESS_THREADS = 2;
	
	public static final BlockingQueue<Pair<ChunkFileMeta, String>> linesToProcess = new LinkedBlockingQueue<>();
	public static final Map<String, Integer> reduceMap = new ConcurrentHashMap<>();
	public static final Map<ChunkFileMeta, Integer> linesCounter = new ConcurrentHashMap<>();
	
	@Override
	public void run() {
		while(true) {
			// count words in the line
			Pair<ChunkFileMeta, String> pair;
			try {
				pair = linesToProcess.take();
				if(pair != null) {
					String line = pair.getSecond().trim();
					// otherwise split(" ") doesn't work 
					if(!("".equals(line))) {
						for(String word : line.split(" ")) {
							reduceMap.merge(word, 1, (oldValue, one) -> oldValue + one);
						}	
					}
					// increase the 'lines counted per file' counter
					linesCounter.merge(pair.getFirst(), 1, (oldValue, one) -> oldValue + one);
				}
			} catch (InterruptedException e) {
				// shouldn't happen
				e.printStackTrace();
			}
		}
	}

}
