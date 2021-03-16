

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class HttpConnectorHelper {
	
	public static void main(String[] args) throws IOException, JSONException {

		JSONObject input = new JSONObject();
		input.put("meterId", 6);
		input.put("timestamp", "2020-06-07 21:15:00");
		// sendGET();
		System.out.println("GET DONE");
		 new HttpConnectorHelper().sendPostWithToken("http://14.139.98.213:4012/agent/fetchTransactionData",input);
		System.out.println("POST DONE");
	}

	
public ArrayList<JSONObject> sendPostWithToken(String url, JSONObject params) throws IOException, JSONException {
		
		ArrayList<JSONObject> listOfObjects = new ArrayList<>();
		JSONObject jsonObject1 = null;
		JSONObject js2= new JSONObject();
		try {
		HashMap<String,String> map = new HashMap<>();
		JSONArray jsonObject = null;
		System.out.println("Start POST Request " );
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		OutputStream os = con.getOutputStream();
		System.out.println(params.toString());
		os.write(params.toString().getBytes());	
		os.flush();
		os.close();
		int responseCode = con.getResponseCode();
		System.out.println("POST Response Code :: " + responseCode);

		if (responseCode == 200) { //success
			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				jsonObject1 = new JSONObject(response.toString());
			if (responseCode != 200) {
				js2.put("error", true);
			} else {
				js2.put("error", false);
			}
				in.close();
				con.disconnect();
				listOfObjects.add(jsonObject1);
				listOfObjects.add(js2);
				// print result
				System.out.println("Response" + response.toString());
			} 
			
		else {
			js2.put("error", true);
			listOfObjects.add(js2);
				System.out.println("POST request not worked");
			}
			
		
		
	}
		catch(Exception e) {
			System.out.println(e.getMessage());
			js2.put("error", true);
			listOfObjects.add(js2);
			return listOfObjects;
			
		}
		System.out.println("Response List"+ listOfObjects);
		return listOfObjects;
	}
	public int sendPostWithToken(String url,String token, JSONObject params) throws IOException, JSONException {
		
		JSONObject jsonObject1 = null;
		try {
		HashMap<String,String> map = new HashMap<>();
		JSONArray jsonObject = null;
		
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("Authorization", "Bearer "+token);
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		OutputStream os = con.getOutputStream();
		System.out.println(params.toString());
		os.write(params.toString().getBytes());	
		os.flush();
		os.close();
		int responseCode = con.getResponseCode();
		System.out.println(new Timestamp(System.currentTimeMillis())+ " POST Response Code :: " + responseCode +"   "+params.toString());

		if (responseCode == HttpURLConnection.HTTP_OK) { //success
			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				jsonObject1 = new JSONObject(response.toString());
				
				 in.close();
				con.disconnect();
				System.out.println(new Timestamp(System.currentTimeMillis())+ " Response :" + (int)jsonObject1.get("status") + responseCode +"  "+params.toString());
				 if ((int)jsonObject1.get("status") == 1 && responseCode == 200) {
					 return 1;
				 }  else {
					 return 0;
				 }
			
			} 
			
		else {
			
				System.out.println("POST request not worked");
				 return 0;
			}
		
	}
		catch(Exception e) {
			return 0;
			
		}
	}
	
	
public ArrayList<JSONObject> sendPostWithTokenForKiot(String url, JSONObject params, String token) throws IOException, JSONException {
		
		ArrayList<JSONObject> listOfObjects = new ArrayList<>();
		JSONObject jsonObject1 = null;
		JSONObject js2= new JSONObject();
		try {
		HashMap<String,String> map = new HashMap<>();
		JSONArray jsonObject = null;
		System.out.println("Start POST Request " );
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		con.setRequestMethod("POST");
		con.setRequestProperty("Authorization", "Bearer "+token);
		con.setRequestProperty("Content-Type", "application/json");
		con.setDoOutput(true);
		OutputStream os = con.getOutputStream();
		System.out.println(params.toString());
		os.write(params.toString().getBytes());	
		os.flush();
		os.close();
		int responseCode = con.getResponseCode();
		System.out.println("POST Response Code :: " + responseCode);

		if (responseCode == HttpURLConnection.HTTP_OK) { //success
			BufferedReader in = new BufferedReader(new InputStreamReader(
					con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				JSONArray arr = new JSONArray(response.toString());
				jsonObject1 = new JSONObject();
				jsonObject1.put("data",arr);
			if (responseCode != 200) {
				js2.put("error", true);
			} else {
				js2.put("error", false);
			}
				in.close();
				con.disconnect();
				listOfObjects.add(jsonObject1);
				listOfObjects.add(js2);
				// print result
			//	System.out.println(response.toString());
			} 
			
		else {
			js2.put("error", true);
			listOfObjects.add(js2);
				System.out.println("POST request not worked");
			}
			
		
		
	}
		catch(Exception e) {
			System.out.println(e.getMessage());
			js2.put("error", true);
			listOfObjects.add(js2);
			return listOfObjects;
			
		}
		return listOfObjects;
	}
	
}