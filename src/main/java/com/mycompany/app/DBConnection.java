package com.mycompany.app;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.bson.Document;
//import com.twmacinta.util.MD5;
import org.bson.types.ObjectId;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

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
		this.db = this.client.getDatabase("wordcount");
		this.fileBucket = GridFSBuckets.create(this.db, "wordfiles");
		this.colResults = this.db.getCollection("partial_results");
		this.colFinalResults = this.db.getCollection("results");
		this.colMetaFiles = this.db.getCollection("metafiles");
		this.colFileRegister = this.db.getCollection("file_register");
		
		// indexes
		IndexOptions indexOptions = new IndexOptions().unique(true);
		this.colFinalResults.createIndex(Indexes.ascending("checksum"), indexOptions);
		this.colMetaFiles.createIndex(Indexes.ascending("id"), indexOptions);
		this.colResults.createIndex(Indexes.ascending("_fileId"), indexOptions);
		this.colFileRegister.createIndex(Indexes.ascending("checksum"), indexOptions);
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
			System.out.println("Found partial upload, clearing up...");
			clearChunkFiles(checksum);
			break;
		case NO_UPLOAD:
			break;
		};
		
		System.out.println("Uploading file");
		
		// register the file
		this.colFileRegister.insertOne(new Document("checksum", checksum).append("fullyUploaded", 0).append("finalised", 0));
		
		final int charFileSize = App.CHUNK_SIZE * 1024*1024 / Character.SIZE; // MB
		final int bufferSize = 8 * 1024;
		
		// start reading the file
		int length = 0;
		int j = 0; // chunk file iterator
		int numOfLines = 0;
    	try(BufferedReader bufferedReader = new BufferedReader(new FileReader(file), bufferSize)) {
    		String line = bufferedReader.readLine();
    		StringBuilder sb = new StringBuilder();
    	    while(line != null) {
//    	    	// don't bother with empty lines
//    	    	line = line.trim();
//    	    	if("".equals(line)) {
//    	    		line = bufferedReader.readLine();
//    	    		continue;
//    	    	}
    	    	numOfLines++;
    	    	
    	    	line = line.toLowerCase();
    	    	sb.append(line);
    	    	sb.append(System.lineSeparator());
    	    	length += line.length();
    	    	if(length >= charFileSize) {
    	    		// find the last word
    	    		String s = sb.toString();
    	    		int index = s.lastIndexOf(' ');
    	    		String content = s.substring(0, index);
    	    		String remainder = s.substring(index); // starts with ' ' 
    	    		
    	    		// upload file
    	    		uploadChunkFile(content, numOfLines, checksum, j++);
    	    		// reset counters
    	    		sb = new StringBuilder(remainder);
    	    		numOfLines = ("".equals(remainder) ? 0 : 1); // in case we've just hit the end with ' '
    	    		length = sb.length();
    	    	}
    	        line = bufferedReader.readLine();
    	    }
    	    // ensure everything is uploaded
    		if(sb.length() > 0) {
    			uploadChunkFile(sb.toString(), numOfLines, checksum, j++);
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
		Set<ObjectId> locallyProcessedChunks = ChunkDownloadRunnable.processedChunks.stream()
	              .map(ChunkFileMeta::getId)
	              .collect(Collectors.toSet());
		
		Document fileMeta = this.colMetaFiles.find(new Document("downloaded", 0).append("id", new Document("$nin", locallyProcessedChunks))).first();
		if(fileMeta == null) {
			// find a file already being processed
			fileMeta = this.colMetaFiles.find(new Document("processed", 0).append("id", new Document("$nin", locallyProcessedChunks))).first();
			if(fileMeta == null) {
				// there's nothing more to do
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
			int fileLength = (int) downloadStream.getGridFSFile().getLength();
			byte[] bytesToWriteTo = new byte[fileLength];
			downloadStream.read(bytesToWriteTo);
			
			ChunkFile chunkFile = new ChunkFile(new ChunkFileMeta(fileMeta), new String(bytesToWriteTo, StandardCharsets.UTF_8));
			
			return chunkFile;
		}
	}
	
	/**
	 * Uploads results for a chunk file with given id
	 * @param id Chunk file id
	 * @param results Document results
	 */
	public void writePartialResultsToDB(ObjectId id, Document results) {
		this.colResults.insertOne(results);
		// updates the processed meta
		this.colMetaFiles.findOneAndUpdate(new Document("id", id), new Document("$inc", new Document("processed", 1)));
	}
	
	/**
	 * Returns results for a chunk file with given id or null if none exist in the database
	 * @param id ObjectId id
	 * @return results Document or null if none found 
	 */
	public Document getPartialResults(ObjectId id) {
		return this.colResults.find(new Document("_fileId", id)).first();
	}

	/**
	 * Collects all partial results for a given checksum
	 * @param checksum
	 * @return results in List<Document>
	 */
	public List<Document> getAllResults(String checksum) {
		// get all the chunkfile meta documents
		FindIterable<Document> chunkFileMetas = this.colMetaFiles.find(new Document("checksum", checksum));
   
        // get all the chunkfile ids
        List<ObjectId> ids = new ArrayList<>();
        chunkFileMetas.forEach(new Block<Document>(){
			@Override
			public void apply(Document doc) {
				ids.add(doc.getObjectId("id"));
			}
        });
        
        // download all the relevant results
        FindIterable<Document> resultsIt = this.colResults.find(new Document("_fileId", new Document("$in", ids)));
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
	 * This method is used to correct the 'processed' value in MetaFiles collection after manual editing.
	 * This is to prevent unexpected behaviour.
	 * @param id
	 * @param val
	 */
	public void setProcessed(ObjectId id, int val) {
		this.colMetaFiles.updateOne(new Document("id", id), new Document("$set", new Document("processed", val)));
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
