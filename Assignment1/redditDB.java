package Assignment1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class redditDB {

	static String filePath = "C:\\Users\\eleon\\Documents\\Programvaruteknik\\Databasteori\\Workspace\\Databasteori\\RedditInfo\\RC_2007-10";
	static List<JSONObject> jsonObjects = new ArrayList<JSONObject>();
	
	public static void main(String[] args) throws org.json.simple.parser.ParseException {
		
		FileReader fileReader;
		try {
			System.out.println(filePath);
			fileReader = new FileReader(filePath);
			BufferedReader reader = new BufferedReader(fileReader);
			String data = "";
			while((data = reader.readLine()) != null) {
				createJson(data);
			}
			System.out.print("Number of json objects: " + jsonObjects.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
		createDatabase();
		System.out.println("Done!");
	}

	public static void createJson(String json) throws org.json.simple.parser.ParseException{
		jsonObjects.add((JSONObject)new JSONParser().parse(json));
	}
	public static String decode(Object object){
	    return Long.valueOf(object.toString(), 36).toString();
	}
	public static Object parseValue(Object object){
		return (Object)object.toString().substring(3);
	}
	
	public static void createDatabase(){
		try (
	         Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/redditdb", "root", "");
	         Statement stmt = conn.createStatement();
	      ) {

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
				
				String user = "INSERT IGNORE INTO `redditdb`.`user` (`name`) VALUES ('"+ author +"');";
				stmt.addBatch(user);
				
				String post = "INSERT INTO `redditdb`.`post` (`post_id`, `post_body`, `post_parent`, `post_link`, `post_created`, `post_author`, `post_subreddit_id`, `post_score`) VALUES "
							+ "('"+ id +"', '"+ body +"', '"+ parent_id +"', '"+ link_id +"', '"+ created_utc +"', '"+ author +"', '"+ subreddit_id +"', '"+ score +"');";
				stmt.addBatch(post);
				
				String sub = "INSERT IGNORE INTO `redditdb`.`subreddit` (`subreddit_id`, `subreddit_name`) VALUES ('"+ subreddit_id +"','"+ subreddit +"');";
				stmt.addBatch(sub);
			}
			
	        stmt.executeBatch();
	        stmt.close();
	        conn.close();

	      } catch(SQLException ex) {
	         ex.printStackTrace();
	      }
	}
	
}
