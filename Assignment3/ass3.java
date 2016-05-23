package Assignment3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32;

public class ass3 {
	
	static String filePath = "C:\\Users\\eleon\\Documents\\Programvaruteknik\\Databasteori\\Workspace\\Databasteori\\RedditInfo\\RC_2007-10";
	static List<JSONObject> jsonObjects = new ArrayList<JSONObject>();
	
	public static void main(String[] args) {
	
		FileReader fileReader;
		try {
			System.out.println(filePath);
			fileReader = new FileReader(filePath);
	        BufferedReader reader = new BufferedReader(fileReader);
	        String data = "";
	        while((data = reader.readLine()) != null){
	        	createJson(data);
	        }
	        System.out.print("Number of json objects: " + jsonObjects.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		createDatabase();	
	}
	

	public static void createJson(String json){
		try {
			jsonObjects.add((JSONObject)new JSONParser().parse(json));
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static String decode(Object object){
	    return Long.valueOf(object.toString(), 36).toString();
	}
	public static Object parseValue(Object object){
		return (Object)object.toString().substring(3);
	}
	
	public static void createDatabase(){
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient();
			DB db = mongoClient.getDB( "redditDB" );
			DBCollection coll = db.getCollection("redditCollection");
			BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
			builder = coll.initializeUnorderedBulkOperation();
			
			for(JSONObject json : jsonObjects){
				
				String id = decode(json.get("id").toString());
				String parent_id = decode(parseValue(json.get("parent_id").toString()));
				String link_id = decode(parseValue(json.get("link_id").toString()));
				String name = decode(parseValue(json.get("name").toString()));
				String author = json.get("author").toString();
				String body = json.get("body").toString();
				String subreddit_id = decode(parseValue(json.get("subreddit_id").toString()));
				String subreddit = json.get("subreddit").toString();
				String score = json.get("score").toString();
				long unixSeconds = Long.parseLong(json.get("created_utc").toString());
				Date date = new Date(unixSeconds*1000L); // *1000 is to convert seconds to milliseconds
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // the format of your date
				sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // give a timezone reference for formating (see comment at the bottom
				String created_utc = sdf.format(date);
				
				StringBuffer sb = new StringBuffer();
				char cArray[] = body.toCharArray();
				for(int i = 0; i < body.length(); i++)
				{
				   if(cArray[i] == '\'') // find single quote in String
				   {
				sb.append('\''); //append the escape character
				   }
				   sb.append(cArray[i]); //append the regular character
				}
				body =  sb.toString();

				
				builder.insert(new BasicDBObject("id", id)
						.append("parent_id", parent_id)
						.append("link_id", link_id)
						.append("name", name)
						.append("author", author)
						.append("body", body)
						.append("subreddit_id", subreddit_id)
						.append("subreddit", subreddit)
						.append("score", score)
						.append("created_utc", created_utc));
				
			}

			BulkWriteResult result = builder.execute();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}