package com.mycompany.app;

import org.bson.Document;
import org.bson.types.ObjectId;

public class ChunkFileMeta {
	/***
	 * Light-weight version of ChunkFile with only the metadata included (no content)
	 */

	private ObjectId id;
	private int numOfLines;
	private String originalFileName;
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
	
	public Document toDocument() {
		return new Document("id", this.id).append("numOfLines", numOfLines).append("originalFileName", originalFileName);
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
	
	public String getOriginalFileName() {
		return originalFileName;
	}

	public void setOriginalFileName(String originalFileName) {
		this.originalFileName = originalFileName;
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
		return "ChunkFileMeta{id: " + id + ", originalFileName: " + originalFileName + ", numOfLines: " + numOfLines + "}";
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
