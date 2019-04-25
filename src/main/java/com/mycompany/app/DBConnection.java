package com.mycompany.app;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.bson.Document;
import org.bson.conversions.Bson;
//import com.twmacinta.util.MD5;
import org.bson.types.ObjectId;

public class DBConnection {

	private final String URI;
	private MongoClient client;
	private MongoDatabase db;
	
	private static final String BUCKET_NAME = "wordfiles";
	private static final String RESULTS_COLLECTION = "results";
	private static final String META_FILES = "metafiles";
	
	public DBConnection(String uri) {
		this.URI = uri;
	}

	//TODO kill connection on destroy?
	public void connect() {
		try {
			this.client = new MongoClient(new MongoClientURI(this.URI));
			this.db= this.client.getDatabase("db");
		} catch (Exception e) { //TODO better exceptions 
			e.printStackTrace();
		}
    	
    	
//        MongoCollection<Document> col = database.getCollection("myTestCollection");
	}
	
	public void uploadFile(String path) {
		//TODO checksums
//		String hash = MD5.asHex(MD5.getHash(new File(filename)));
		File file = new File(path);
		
		final int linesPerFile = 2;
		final int bufferSize = 8 * 1024;
		
		// start reading the file
		int i = 0;
		int j = 0; // chunk file it
    	try(BufferedReader bufferedReader = new BufferedReader(new FileReader(file), bufferSize)) {
    		String line = bufferedReader.readLine();
    		int numOfLines = 0;
    		StringBuilder lines = new StringBuilder();
    	    while(line != null) {
    	    	lines.append(line);
    	    	lines.append(System.lineSeparator()); // \n gets dropped
    	    	numOfLines++;
    	    	if(i++ >= linesPerFile) {
    	    		// upload file
    	    		uploadChunkFile(lines.toString(), numOfLines, file.getName(), j++);
    	    		i = 0; // reset counter
    	    		lines = new StringBuilder();
    	    		numOfLines = 0;
    	    	}
    	        line = bufferedReader.readLine();
    	    }
    	} catch (FileNotFoundException e) {
    		System.out.println("File not found");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public ChunkFile getNextChunkFile(String fileName) {
		
		MongoCollection<Document> metaCol = this.db.getCollection(META_FILES);
		
		// find unused file
		Document fileMeta = metaCol.find(new Document("downloaded", 0)).first();
		if(fileMeta == null) {
			// find a file already being processed
			fileMeta = metaCol.find(new Document("processed", 0)).first();
			if(fileMeta == null) {
				// there's nothing more to do
				//TODO actually maybe stats?
				throw new VydraNoChunkFilesFoundException("No not-yet-downloaded and processed chunk files remaining");		
			}
		}
		ObjectId fileId = (ObjectId)fileMeta.get("id");
		
		// update the info
		metaCol.updateOne(fileMeta, new Document("$inc", new Document("downloaded", 1)));
		
		GridFSBucket gridFSBucket = GridFSBuckets.create(this.db, fileName);		
		// get the chunk file
		GridFSFindIterable found = gridFSBucket.find(new BasicDBObject("_id", fileId));
		GridFSFile file = found.first();
		ObjectId id = file.getObjectId();
		
		// download it
		try (GridFSDownloadStream  downloadStream = gridFSBucket.openDownloadStream(id)) {
			//TODO read in batches (but don't split words)
			int fileLength = (int) downloadStream.getGridFSFile().getLength();
			byte[] bytesToWriteTo = new byte[fileLength];
			downloadStream.read(bytesToWriteTo);
			return new ChunkFile(id, new String(bytesToWriteTo, StandardCharsets.UTF_8));
		}
	}
	
	public void writeReducedResultsToDB(ChunkFileMeta file) {
		MongoCollection<Document> col = this.db.getCollection(RESULTS_COLLECTION);
		
		// check whether the results already exist (some1 already done the job?)
		Document existingResult = col.find(new Document("fileId", file.getId())).first();
		if(existingResult == null) {
			// no results yet, lets save them
			// build the doc
			Document results = new Document("fileId", file.getId());
			for(String key : ProcessThread.reduceMap.keySet()) {
				results.append(key, ProcessThread.reduceMap.get(key));
			}
			// save it
			col.insertOne(results);
		} else {
			//TODO what to do if results already computed? save a duplicate in case they differ and let the 'merge-results' method decide?
			System.out.println("Results document already exists ( id: " + existingResult.get("_id") + ")");
		}
		// updates the processed meta
		this.db.getCollection(META_FILES).findOneAndUpdate(file.toDocument(), new Document("$inc", new Document("processed", 1)));
		
		// clear the map
		//TODO change this once adding support for multiple chunk files
		ProcessThread.reduceMap.clear();
	}

	
	private void uploadChunkFile(String lines, int numOfLines, String fileName, int it) {
		try {
			GridFSBucket gridFSBucket = GridFSBuckets.create(this.db, fileName);
			InputStream stream = new ByteArrayInputStream(lines.getBytes("UTF-8") );
		    // Create some custom options
		    GridFSUploadOptions options = new GridFSUploadOptions()
		                                        .metadata(new Document("origFileName", fileName)
		                                        			.append("numOfLines", numOfLines));

		    ObjectId fileId = gridFSBucket.uploadFromStream(fileName + it, stream, options);
		    
		    // saves the record of processing
		    this.db.getCollection(META_FILES).insertOne(new Document("id", fileId)
		    												.append("numOfLines", numOfLines)
		    												.append("origFileName", fileName)
															.append("downloaded", 0)
															.append("processed", 0));
		} catch (UnsupportedEncodingException e) {
			// this should not be ever thrown
			e.printStackTrace();
		}
	}
}
