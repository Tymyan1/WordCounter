package com.mycompany.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.xml.bind.DatatypeConverter;

public class Util {
	public static String getChecksum(String filename) {
		
		try(InputStream is = new FileInputStream(filename)) {
		    MessageDigest md = MessageDigest.getInstance("MD5");
		    byte[] buffer = new byte[8*1024]; // or any other size
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
			//TODO handle this
			e.printStackTrace();
		}
		return null;
    }
}
