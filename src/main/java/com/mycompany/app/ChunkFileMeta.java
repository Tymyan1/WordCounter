package com.mycompany.app;

import org.bson.Document;
import org.bson.types.ObjectId;

/***
 * Light-weight version of ChunkFile with only the metadata included (no content)
 * 
 * @author Vydra
 */
public class ChunkFileMeta {

	private ObjectId id;
	private int numOfLines;
//	private String originalFileName;
	private String checksum;
	private int downloaded;
	private int processed;
	
	public ChunkFileMeta(ObjectId id) {
		super();
		this.id = id;
	}
	
	public ChunkFileMeta(ObjectId id, int numOfLines) {
		super();
		this.id = id;
		this.numOfLines = numOfLines;
	}

	public ChunkFileMeta(Document doc) {
		super();
		this.id = doc.getObjectId("id");
		this.numOfLines = doc.getInteger("numOfLines", 0);
		this.checksum = doc.getString("checksum");
		this.downloaded = doc.getInteger("downloaded", 0);
		this.processed = doc.getInteger("processed", 0);
	}
	
	public Document toDocument() {
		return new Document("id", this.id).append("numOfLines", numOfLines).append("checksum", checksum);
	}
	
	public Document toIdDocument() {
		return new Document("id", this.id);
	}
	
	public ObjectId getId() {
		return id;
	}
	public void setId(ObjectId id) {
		this.id = id;
	}
	public int getNumOfLines() {
		return numOfLines;
	}
	public void setNumOfLines(int numOfLines) {
		this.numOfLines = numOfLines;
	}
	
//	public String getOriginalFileName() {
//		return originalFileName;
//	}
//
//	public void setOriginalFileName(String originalFileName) {
//		this.originalFileName = originalFileName;
//	}
	
	public String getChecksum() {
		return checksum;
	}

	public void setChecksum(String checksum) {
		this.checksum = checksum;
	}
	
		
	public int getDownloaded() {
		return downloaded;
	}

	public void setDownloaded(int downloaded) {
		this.downloaded = downloaded;
	}

	public int getProcessed() {
		return processed;
	}

	public void setProcessed(int processed) {
		this.processed = processed;
	}

	@Override
	public String toString() {
		return "ChunkFileMeta{id: " + id + ", checksum: " + checksum + ", numOfLines: " + numOfLines + "}";
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == this) return true;
		if (o instanceof ChunkFileMeta) {
			return ((ChunkFileMeta)o).id == this.id;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return 17 * 31 * this.id.hashCode();
	}
}
