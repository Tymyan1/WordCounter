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
import com.mongodb.client.MongoCursor;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.DatatypeConverter;

import org.bson.Document;
import org.bson.conversions.Bson;
//import com.twmacinta.util.MD5;
import org.bson.types.ObjectId;

/**
 * Class representing a MongoDB client. Contains methods for database access.
 * 
 * @author Vydra
 *
 */
public class DBConnection {

	public final String URI;
	private MongoClient client;
	private MongoDatabase db;
	private GridFSBucket fileBucket;
	private MongoCollection<Document> colResults;
	private MongoCollection<Document> colFinalResults;
	private MongoCollection<Document> colMetaFiles;
	private MongoCollection<Document> colFileRegister;
	
	public DBConnection(String uri) {
		this.URI = uri;
		this.client = new MongoClient(new MongoClientURI(this.URI));
		this.db = this.client.getDatabase("db");
		this.fileBucket = GridFSBuckets.create(this.db, "wordfiles");
		this.colResults = this.db.getCollection("results");
		this.colFinalResults = this.db.getCollection("final_results");
		this.colMetaFiles = this.db.getCollection("metafiles");
		this.colFileRegister = this.db.getCollection("file_register");
	}
	
	/**
	 * Checks the upload stage of a file given the checksum.
	 * @param checksum
	 * @return
	 */
	public FileUploadStage fileUploaded(String checksum) {
		Document doc = this.colFileRegister.find(new Document("checksum", checksum)).first();
		if(doc == null) return FileUploadStage.NO_UPLOAD;
		int uploads = doc.getInteger("fullyUploaded");
		if(uploads > 0) return FileUploadStage.FULL_UPLOAD;
		else return FileUploadStage.PARTIAL_UPLOAD;
	}
	
	/**
	 * Uploads a given file by calculating the checksum, clearing out any partial uploads, splitting the file into chunk files
	 * and uploading them. Does not clear/upload anything if the file has already been uploaded (based on checksum).
	 * @param file File to be uploaded
	 * @return Checksum of the file
	 * @throws IOException 
	 */
	public String uploadFile(File file) throws IOException {
		
		System.out.println("Calculating the checksum");
		String checksum = Util.getChecksum(file.getAbsolutePath());
  
		switch(fileUploaded(checksum)) {
		case FULL_UPLOAD:
			System.out.println("File already seems to be uploaded!");
			return checksum;
		case PARTIAL_UPLOAD:
			//TODO check numOfLines per chunkFile and maybe not discard the files?
			System.out.println("Found partial upload, clearing up...");
			clearChunkFiles(checksum);
			break;
		case NO_UPLOAD:
			break;
		};
		
		System.out.println("Uploading file");
		
		// register the file
		this.colFileRegister.insertOne(new Document("checksum", checksum).append("fullyUploaded", 0).append("finalised", 0));
		
		final int linesPerFile = 2000; //TODO move into config or similar
		final int bufferSize = 8 * 1024;
		
		// start reading the file
		int i = 0;
		int j = 0; // chunk file iterator
    	try(BufferedReader bufferedReader = new BufferedReader(new FileReader(file), bufferSize)) {
    		String line = bufferedReader.readLine();
    		int numOfLines = 0;
    		StringBuilder lines = new StringBuilder();
    	    while(line != null) {
    	    	// don't bother with empty lines
    	    	line = line.trim();
    	    	if("".equals(line)) {
    	    		line = bufferedReader.readLine();
    	    		continue;
    	    	}
    	    	
    	    	line = line.toLowerCase();
    	    	lines.append(line);
    	    	lines.append(System.lineSeparator()); // \n gets dropped
    	    	numOfLines++;
    	    	if(numOfLines >= linesPerFile) {
    	    		// upload file
    	    		uploadChunkFile(lines.toString(), numOfLines, checksum, j++);
    	    		// reset counters
    	    		i = 0; 
    	    		lines = new StringBuilder();
    	    		numOfLines = 0;
    	    	}
    	        line = bufferedReader.readLine();
    	    }
    	    // ensure everything is uploaded
    		if(lines.length() > 0) {
    			uploadChunkFile(lines.toString(), numOfLines, checksum, j++);
    		}
    	    // increase the fullyUploaded counter
    	    this.colFileRegister.updateOne(new Document("checksum", checksum), new Document("$inc", new Document("fullyUploaded", 1)));
    	    return checksum;
    	} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
	}
	
	/**
	 * Deletes all chunk files associated to a given checksum.
	 * @param checksum
	 */
	public void clearChunkFiles(String checksum) {
		// delete meta files
		this.colMetaFiles.deleteMany(new Document("checksum", checksum));
		
		// delete the actual files
		this.fileBucket.find(new Document("checksum", checksum)).forEach(new Block<GridFSFile>(){
			@Override
			public void apply(GridFSFile file) {
				fileBucket.delete(file.getId());
			}
		});
		
		// delete from the register as well (will be reentered on upload)
		this.colFileRegister.deleteOne(new Document("checksum", checksum));
	}	
	
	/**
	 * Downloads the next chunk file to process associated to a given checksum. Prioritises chunk files not yet downloaded
	 * by other processes over those already downloaded but not yet processed.
	 * @param checksum of the file
	 * @return ChunkFile 
	 */
	public ChunkFile getNextChunkFile(String checksum) {
		
		// find unused file
		Document fileMeta = this.colMetaFiles.find(new Document("downloaded", 0)).first();
		if(fileMeta == null) {
			// find a file already being processed
			fileMeta = this.colMetaFiles.find(new Document("processed", 0)).first();
			if(fileMeta == null) {
				// there's nothing more to do
				//TODO actually maybe stats?
				return null;
			}
		}
		ObjectId fileId = fileMeta.getObjectId("id");
		
		// update the info
		this.colMetaFiles.updateOne(fileMeta, new Document("$inc", new Document("downloaded", 1)));
		
		// get the chunk file
		GridFSFindIterable found = this.fileBucket.find(new Document("_id", fileId));
		GridFSFile file = found.first();
		ObjectId id = file.getObjectId();
		
		// download it
		try (GridFSDownloadStream  downloadStream = this.fileBucket.openDownloadStream(id)) {
			//TODO read in batches (but don't split words)
			int fileLength = (int) downloadStream.getGridFSFile().getLength();
			byte[] bytesToWriteTo = new byte[fileLength];
			downloadStream.read(bytesToWriteTo);
			
			ChunkFile chunkFile = new ChunkFile(id, new String(bytesToWriteTo, StandardCharsets.UTF_8));
			chunkFile.getFileMeta().setChecksum(checksum);
			
			// register the file with the map
			ProcessRunnable.reduceMap.put(id, new ConcurrentHashMap<String, Integer>());
			return chunkFile;
		}
	}
	
	/**
	 * Given a chunk file meta, collects the partial results from ProcessRunnable.reduceMap and uploads them to the database
	 * @param file
	 */
	//TODO refactor
	public void writeReducedResultsToDB(ChunkFileMeta file) {
		// check whether the results already exist (some1 already done the job?)
		Document existingResult = this.colResults.find(new Document("fileId", file.getId())).first();
		if(existingResult == null) {
			// no results yet, lets save them
			// build the doc
			Document results = new Document("fileId", file.getId());
			for(String key : ProcessRunnable.reduceMap.get(file.getId()).keySet()) {
				results.append(key, ProcessRunnable.reduceMap.get(file.getId()).get(key));
			}
			// save it
			this.colResults.insertOne(results);
		} else {
			//TODO what to do if results already computed? save a duplicate in case they differ and let the 'merge-results' method decide?
			System.out.println("Results document already exists ( id: " + existingResult.get("_id") + ")");
		}
		// updates the processed meta
		this.colMetaFiles.findOneAndUpdate(file.toIdDocument(), new Document("$inc", new Document("processed", 1)));
		
		// clear the map
		ProcessRunnable.reduceMap.get(file.getId()).clear();
	}

	/**
	 * Collects all partial results for a given checksum
	 * @param checksum
	 * @return results in List<Document>
	 */
	public List<Document> getAllResults(String checksum) {
		// get all the chunkfile meta documents
		FindIterable<Document> chunkFileMetas = this.colMetaFiles.find(new Document("checksum", checksum));
   
        //TODO maybe get the ids directly from gridfs?
        // get all the chunkfile ids
        List<ObjectId> ids = new ArrayList<>();
        chunkFileMetas.forEach(new Block<Document>(){
			@Override
			public void apply(Document doc) {
				ids.add(doc.getObjectId("id"));
			}
        });
        
        // download all the relevant results
        FindIterable<Document> resultsIt = this.colResults.find(new Document("fileId", new Document("$in", ids)));
        // into list
        List<Document> results = new ArrayList<>();
        resultsIt.into(results);
        
        return results;
	}
	
	/**
	 * Uploads the final result for a given checksum
	 * @param finalResults
	 * @param checksum
	 */
	public void uploadFinalResult(Document finalResults, String checksum) {
		// upload results
      this.colFinalResults.insertOne(finalResults);
      // acknowledge in file register
      this.colFileRegister.updateOne(new Document("checksum", checksum), new Document("$inc", new Document("finalised", 1)));
      
	}
	
	/**
	 * Collects final results from database for a given checksum
	 * @param checksum
	 * @return Final results Document or null if final results not present for given checksum
	 */
	public Document getFinalResults(String checksum) {
		Document doc = this.colFinalResults.find(new Document("_fileChecksum", checksum)).first();
		return doc;
	}
	
	/**
	 * Finds the next file to process
	 * @return Checksum of the file
	 */
	public String getNextTargetFile() {
		// get the next chunk file not being processed
		Document fileRegister = this.colFileRegister.find(new Document("finalised", 0).append("fullyUploaded", new Document("$gt", 0))).first();
		if(fileRegister == null) return null;
		return fileRegister.getString("checksum");
	}
	
	/**
	 * This method is used to correct the 'finalised' value in FileRegister when the final_results record exists.
	 * The program itself should never end up in that state, but manually editing the value could lead to unexpected behaviour. 
	 * @param checksum 
	 * @param val
	 */
	public void setFileFinalised(String checksum, int val) {
		this.colFileRegister.updateOne(new Document("checksum", checksum), new Document("$set", new Document("finalised", val)));
	}
	
	/**
	 * Uploads a single chunk file
	 * @param lines Content of the chunk file
	 * @param numOfLines Number of lines in the chunk file
	 * @param checksum Checksum of the file
	 * @param it iterator to append to the chunk file's name
	 */
	private void uploadChunkFile(String lines, int numOfLines, String checksum, int it) {
		try {
			InputStream stream = new ByteArrayInputStream(lines.getBytes("UTF-8") );
		    // Create some custom options
		    GridFSUploadOptions options = new GridFSUploadOptions()
		                                        .metadata(new Document("checksum", checksum)
		                                        			.append("numOfLines", numOfLines));

		    ObjectId fileId = this.fileBucket.uploadFromStream(checksum + "_" + it, stream, options);
		    
		    // saves the metas
		    this.colMetaFiles.insertOne(new Document("id", fileId)
    												.append("numOfLines", numOfLines)
    												.append("checksum", checksum)
//    												.append("origFileName", fileName)
													.append("downloaded", 0)
													.append("processed", 0));
		} catch (UnsupportedEncodingException e) {
			// this should not be ever thrown
			e.printStackTrace();
		}
	}
}
