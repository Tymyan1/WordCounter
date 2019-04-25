package com.mycompany.app;

import org.bson.types.ObjectId;

public class ChunkFile {

	private ObjectId id;
	private String content;
	private int numOfLines;
	
	public ChunkFile(ObjectId id, String content) {
		this.id = id;
		this.content = content;
	}

	public ChunkFileMeta getFileMeta() {
		return new ChunkFileMeta(this.id, this.numOfLines);
	}
	
	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public int getNumOfLines() {
		return numOfLines;
	}

	public void setNumOfLines(int numOfLines) {
		this.numOfLines = numOfLines;
	}
	
	@Override
	public String toString() {
		return "ChunkFile{id: " + id + ", numOfLines: " + numOfLines + ", content: " + content + "}";
	}
	
}
