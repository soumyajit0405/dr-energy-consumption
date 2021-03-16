
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class WorkerThread implements Runnable {
	public int userId;
	public String kiotDeviceId;
	public String bearerToken;
	public int userDeviceId;

	public WorkerThread(int userId, String kiotDeviceId, String bearerToken, int userDeviceId) {
		this.userId = userId;
		this.kiotDeviceId = kiotDeviceId;
		this.bearerToken = bearerToken;
		this.userDeviceId = userDeviceId;
	}
	static Connection con;
	public void run() {
		try {
			getStartTxDataFromKiot();

		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		finally {

		}
		System.out.println(Thread.currentThread().getName() + " (End)");// prints thread name
	}

	private void processmessage() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void getStartTxDataFromKiot() throws ClassNotFoundException, SQLException, JSONException, IOException, ParseException {
		final long HOUR = 3600 * 1000; // in milli-seconds.
		final long HALFHOUR = 1800 * 1000;
		final long ONE_MINUTE_IN_MILLIS=60000;//millisecs
		
		  Date dnew=new Date(new Date().getTime() +5*HOUR+HALFHOUR- 30*ONE_MINUTE_IN_MILLIS);
		  int day = dnew.getDate();
			int year = dnew.getYear() + 1900;
			int month = dnew.getMonth() + 1;
	        SimpleDateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd");
	       //SimpleDateFormat dt1 = new SimpleDateFormat("2018-03-06");
	        System.out.println(dt1.format(dnew));
	        SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
	        String datenew = dt1.format(dnew);
	        String timenew = dt2.format(dnew);
	        String startTime = datenew +" "+timenew;
		String[] values = startTime.split("-");
		String[] dateTime =values[2].split(" "); 
		String[] time = dateTime[1].split(":");
		ArrayList<String> deviceIds = new ArrayList<String>();
		deviceIds.add(kiotDeviceId);
		double meterReading = 0;
		DBHelper dbhelper = new DBHelper();
		HttpConnectorHelper httpconnectorhelper = new HttpConnectorHelper();
		JSONObject input = new JSONObject();
		JSONObject payload = new JSONObject();
		JSONObject filterSlots = new JSONObject();
		filterSlots.put("hour", Integer.parseInt(time[0]));
		filterSlots.put("minute", Integer.parseInt(time[1]));
		payload.put("duration", "day");
		payload.put("group_by_minutes", "15");
		payload.put("filter_slot", filterSlots);
		payload.put("year", year);
		payload.put("month", month);
		payload.put("startDay", day);
		payload.put("device_ids", deviceIds);
		input.put("payload", payload);
		input.put("intent", "action.entities.ENERGY_DATA");
		//System.out.println("Request ----- >" + input);
		ArrayList<JSONObject> responseFromDevice = httpconnectorhelper.sendPostWithTokenForKiot(
				"https://api.kiot.io/integrations/ctp/go", input,
				bearerToken);
		parseAndStoreResponse(responseFromDevice,dnew);
		}

	public static void main(String args[]) throws ClassNotFoundException, SQLException, JSONException, IOException, ParseException {
	//	WorkerThread wk = new WorkerThread(16, "1", "1",9);
	//	wk.getStartTxDataFromKiot();
		
	}
	
	public void parseAndStoreResponse(ArrayList<JSONObject> request, Date d1) throws JSONException, SQLException, ParseException {
		SimpleDateFormat dt1 = new SimpleDateFormat("yyyy-MM-dd");
		String newDate = dt1.format(d1);
		if (request.size() > 1 && !(boolean)request.get(1).get("error")) {
			  Statement stmt = null;
			try{
				if (ScheduleDAO.con == null ) {
					if ( con == null) {
					con = JDBCConnection.getOracleConnection();
					}
			 } else {
				 con = ScheduleDAO.con;
			 }
			con.setAutoCommit(false);
			stmt = con.createStatement();
			JSONArray jsArr = (JSONArray)request.get(0).get("data");
			for (int i =0;i<jsArr.length();i++) {
				JSONObject jso= (JSONObject) jsArr.get(i);
				JSONObject js1 = (JSONObject)jso.get("_id");
				int energy =(int)(jso.get("avg_energy"));
				String hour = Integer.toString((int)js1.get("hour"));
				String mins = Integer.toString((int)js1.get("minute"));
				if (mins.equalsIgnoreCase("0")) {
					mins = mins.concat("0");
				} if (hour.length() <= 1)
				{
					hour = "0"+hour; 
				}
					
				String value = TimeSlots.valueOf("_"+hour+mins).getTimeSlot();
				stmt.addBatch("insert into customer_power_consumption(customer_id,device_id,timeslot_id,power_consumed,date) values "
						+ "("+userId+","+userDeviceId+","+Integer.parseInt(value)+","+energy+",'"+newDate+"')");
			}
		     int[] result = stmt.executeBatch();
		      System.out.println("The number of rows inserted: "+ result.length);
		      con.commit();
		}
			catch(Exception e) {
				System.out.println("Issue in Accumulating");
				e.printStackTrace();
				con.rollback();
				if (ScheduleDAO.con == null) {
					con.close();
				}
			}
			finally {
				if (ScheduleDAO.con == null) {
					con.close();
				}
			}
		}
	}
  

}