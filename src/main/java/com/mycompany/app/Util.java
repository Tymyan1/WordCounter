package com.mycompany.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

/**
 * Class providing static utility methods 
 * 
 * @author Vydra
 *
 */
public abstract class Util {
	
	/**
	 * Computes a checksum for a given file.
	 * @param filename Name of the file (path)
	 * @return Checksum
	 * @throws IOException 
	 */
	public static String getChecksum(String filename) throws IOException {
		
		try(InputStream is = new FileInputStream(filename)) {
		    MessageDigest md = MessageDigest.getInstance("MD5");
		    byte[] buffer = new byte[8*1024*1024]; // or any other size
		    int len;
		    while ((len = is.read(buffer)) != -1)
		    {
		        md.update(buffer, 0, len); // only update with the just read bytes
		    }
		    
		    byte[] digest = md.digest();
		    String checksum = DatatypeConverter.printHexBinary(digest).toUpperCase();
		    return checksum;
		} catch (NoSuchAlgorithmException e) {
			// Shouldn't ever be thrown
			e.printStackTrace();
		} catch (IOException e) {
			throw e;
		}
		return null;
    }
}
