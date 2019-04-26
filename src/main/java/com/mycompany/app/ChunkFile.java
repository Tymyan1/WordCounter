package com.mycompany.app;

import org.bson.types.ObjectId;

public class ChunkFile {

	private String content;
	private ChunkFileMeta fileMeta;
	
	public ChunkFile(ObjectId id, String content) {
		this.fileMeta = new ChunkFileMeta(id);
		this.content = content;
	}
	
	public ChunkFileMeta getFileMeta() {
		return this.fileMeta;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "ChunkFile{fileMeta: " + fileMeta + ", content: " + content + "}";
	}
	
}
