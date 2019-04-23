package com.mycompany.app;

import java.util.Arrays;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.Block;
import org.bson.Document;

public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        try (MongoClient mongoClient = new MongoClient()) {
        	System.out.println("ok");
        	MongoDatabase database = mongoClient.getDatabase("test");
	        MongoCollection<Document> col = database.getCollection("myTestCollection");
	        
	        
	        Block<Document> printBlock = new Block<Document>() {
	            @Override
	            public void apply(final Document document) {
	                System.out.println(document.toJson());
	            }
	        };
	     
	        col.find(new Document("name", "Café Con Leche")).forEach(printBlock);
	        System.out.println("ok");
        } catch (Exception e) {
        	e.printStackTrace();
        }
    }
}
