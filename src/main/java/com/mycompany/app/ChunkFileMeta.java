package com.mycompany.app;

import org.bson.Document;
import org.bson.types.ObjectId;

public class ChunkFileMeta {
	/***
	 * Light-weight version of ChunkFile with only the metadata included (no content)
	 */

	private ObjectId id;
	private int numOfLines;
	
	public ChunkFileMeta(ObjectId id, int numOfLines) {
		super();
		this.id = id;
		this.numOfLines = numOfLines;
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
	
	public Document toDocument() {
		return new Document("id", this.id);
	}

	@Override
	public String toString() {
		return "ChunkFileMeta{id: " + id + ", numOfLines: " + numOfLines + "}";
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
