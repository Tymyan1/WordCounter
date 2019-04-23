package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.Block;
import org.bson.Document;

public class App 
{
	// future args
	private static final String URI = "mongodb://localhost:27017";
	private static final String FILE = "D:/kody_k_hram.txt";
	
	
    public static void main( String[] args )
    {
    	
    	DBConnection db = new DBConnection(URI);
    	db.connect();
    	db.uploadFile(FILE);
    	db.getNextChunkFile("kody_k_hram.txt");

    	
    }
}
