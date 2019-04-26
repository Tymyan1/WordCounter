package com.mycompany.app;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.xml.bind.DatatypeConverter;

public class Util {
	public static String getChecksum(String filename) {
		
		try {
			
			MessageDigest md = MessageDigest.getInstance("MD5");
		    md.update(Files.readAllBytes(Paths.get(filename)));
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
