package com.mycompany.app;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.MongoCollection;
import com.mongodb.Block;
import static com.mongodb.client.model.Filters.eq;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.bson.Document;
//import com.twmacinta.util.MD5;
import org.bson.types.ObjectId;

public class DBConnection {

	private final String URI;
	private MongoClient client;
	private MongoDatabase db;
	
	private static final String BUCKET_NAME = "wordfiles";
	
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
    		StringBuilder lines = new StringBuilder();
    	    while(line != null) {
    	    	lines.append(line);
    	    	lines.append(System.lineSeparator()); // \n gets dropped 
    	    	if(i++ >= linesPerFile) {
    	    		// upload file
    	    		uploadChunkFile(lines.toString(), file.getName(), j++);
    	    		i = 0; // reset counter
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
	
	public void getNextChunkFile(String fileName) {
		GridFSBucket gridFSBucket = GridFSBuckets.create(this.db, fileName);
		
		ObjectId id = gridFSBucket.find(eq("metadata.used", 0)).first().getObjectId();
		
		try(FileOutputStream stream = new FileOutputStream(fileName)) {
		    gridFSBucket.downloadToStream(id, stream);
		} catch (IOException e) {
		    // handle exception
			e.printStackTrace();
		}
		
	
	}
	
	private void uploadChunkFile(String lines, String fileName, int it) {

		try {
			GridFSBucket gridFSBucket = GridFSBuckets.create(this.db, fileName);
			InputStream stream = new ByteArrayInputStream(lines.getBytes("UTF-8") );
		    // Create some custom options
		    GridFSUploadOptions options = new GridFSUploadOptions()
		                                        .metadata(new Document("origFileName", fileName)
		                                        						.append("used", 0));

		    ObjectId fileId = gridFSBucket.uploadFromStream(fileName + it, stream, options);
		} catch (UnsupportedEncodingException e) {
			// this should not be ever thrown
			e.printStackTrace();
		}
	}
}
