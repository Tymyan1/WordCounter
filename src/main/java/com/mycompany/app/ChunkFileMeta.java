package com.mycompany.app;

import org.bson.Document;
import org.bson.types.ObjectId;

public class ChunkFileMeta {
	/***
	 * Light-weight version of ChunkFile with only the metadata included (no content)
	 */

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
		this.id = fieldTransform(Integer.class, "id", doc);
		this.numOfLines = fieldTransform(Integer.class, "numOfLines", doc);
		this.checksum = fieldTransform(String.class, "checksum", doc);
		this.downloaded = fieldTransform(Integer.class, "downloaded", doc);
		this.processed = fieldTransform(Integer.class, "processed", doc);
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

	private static <T> T fieldTransform(Class<?> T, String fieldName, Document doc) {
		Object field = doc.get(fieldName);
		if(field != null && field instanceof Class<?>) {
			return (T)field;
		} else {
			// no switch for non-enum/string/int vars :(
			if(T.equals(Integer.class)) return (T) new Integer(0);
			else if(T.equals(String.class)) return (T) new String("");
			else if(T.equals(Boolean.class)) return (T) new Boolean(false);
			else if(T.equals(Long.class)) return (T) new Long(0);
			else if(T.equals(Double.class)) return (T) new Double(0);
			else if(T.equals(Float.class)) return (T) new Float(0);
			else if(T.equals(Object.class)) return (T) null;
			else if(T.equals(Byte.class)) return (T) new Byte((byte) 0);
			else if(T.equals(Short.class)) return (T) new Short((short) 0);
			else if(T.equals(Character.class)) return (T) new Character('\u0000');
			return null;
		}
	}
}
