

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.json.JSONException;
import org.json.JSONObject;


public class ScheduleDAO {

	static Connection con;
	Statement stmt = null;
	PreparedStatement pStmt = null;
	static String startTime="", endTime="";
	 public ArrayList<HashMap<String,Object>> getUserDevices() throws SQLException, ClassNotFoundException, JSONException
	 {
		 PreparedStatement pstmt = null;
		//JDBCConnection connref =new JDBCConnection();
		 
		 if (con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
		//	System.out.println("select aso.sell_order_id,ubc.private_key,ubc.public_key,abc.order_id from all_sell_orders aso,all_blockchain_orders abc, user_blockchain_keys ubc where aso.transfer_start_ts ='"+date+" "+time+"' and abc.general_order_id=aso.sell_order_id and abc.order_type='SELL_ORDER' and ubc.user_id  = aso.seller_id and aso.order_status_id=1");
		 // String query="select aso.sell_order_id,ubc.private_key,ubc.public_key,abc.order_id,abc.all_blockchain_orders_id from all_sell_orders aso,all_blockchain_orders abc, user_blockchain_keys ubc where aso.transfer_start_ts ='"+date+" "+time+"' and abc.general_order_id=aso.sell_order_id and abc.order_type='SELL_ORDER' and ubc.user_id  = aso.seller_id and aso.order_status_id=3";
//		 	String query="SELECT all_users.user_id,all_kiot_switches.custom_data,kiot_user_mappings.bearer_token,user_dr_devices.user_dr_device_id FROM all_users, user_dr_devices, all_kiot_switches,kiot_user_mappings\r\n" + 
//		 			"where all_users.user_id = user_dr_devices.user_id\r\n" + 
//		 			"and all_kiot_switches.id = user_dr_devices.port_number\r\n" + 
//		 			"and kiot_user_mappings.contract_number = all_users.dr_contract_number\r\n" + 
//		 			"and all_users.dr_contract_number is not null";
		 
		 String query ="SELECT all_users.user_id,all_kiot_switches.custom_data,kiot_user_mappings.bearer_token,user_dr_devices.user_dr_device_id FROM all_users,\r\n"
		 		+ " user_dr_devices, all_kiot_switches,kiot_user_mappings"
		 		+ "		 			where all_users.user_id = user_dr_devices.user_id\r\n"
		 		+ "		 			and all_kiot_switches.id = user_dr_devices.port_number\r\n"
		 		+ "		 			and kiot_user_mappings.contract_number = all_users.dr_contract_number\r\n"
		 		+ "		 			and all_users.dr_contract_number is not null";
			//String query="select a.event_id from all_events a where  a.event_status_id= 3 and a.event_end_time ='2020-07-29 13:15:00'";
			pstmt=con.prepareStatement(query);
		// pstmt.setString(1,controllerId);
		 ResultSet rs= pstmt.executeQuery();
		 ArrayList<HashMap<String,Object>> al=new ArrayList<>();
		 while(rs.next())
		 {
			 JSONObject js = new JSONObject(rs.getString("custom_data"));
			 HashMap<String,Object> data=new HashMap<>();
			 data.put("userId",(rs.getInt("user_id")));
			 data.put("bearerToken",rs.getString("bearer_token"));
			 data.put("kiotDeviceId",(String)js.get("ud_id"));
			 data.put("userDeviceId",rs.getInt("user_dr_device_id"));
			 //data.put("endTime","2020-07-29 13:15:00");
			 al.add(data);
			// initiateActions(rs.getString("user_id"),rs.getString("status"),rs.getString("controller_id"),rs.getInt("device_id"),"Timer");
			//topic=rs.getString(1);
		 }
		 // updateEventsManually(date, time);
		return  al;
	 }
	 
	 public void updateEventStatus(int eventId) throws SQLException, ClassNotFoundException
	 {
		 PreparedStatement pstmt = null, pstmst1= null;
		 double committedPower =0, actualPower =0;
		 int eventSetId=0;
		//JDBCConnection connref =new JDBCConnection();
		 if (con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
		 String query = "select commited_power,actual_power,event_set_id from all_events where event_id=?";
			pstmt = con.prepareStatement(query);
			 pstmt.setInt(1,eventId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				committedPower = rs.getDouble(1);
				actualPower = rs.getDouble(2);
				eventSetId = rs.getInt(3);
		}
			double resultantPower= committedPower- actualPower;
		 // query="update all_events set event_status_id=3,short_fall="+resultantPower+" where event_id =?";
		 query="update all_events set event_status_id=9 where event_id =?";
		 pstmt=con.prepareStatement(query);
		pstmt.setInt(1,eventId);
		 pstmt.executeUpdate();
		 
		 query="update all_event_sets set actual_power=actual_power+? where event_set_id =?";
		 pstmt=con.prepareStatement(query);
		 pstmt.setDouble(1,actualPower);
		pstmt.setInt(2,eventSetId);
		 pstmt.executeUpdate();
			 
	 }
	 
		public String getBlockChainSettings() throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			String val = "";
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query = "select value from general_config where name='dr_blockchain_enabled'";
			pstmt = con.prepareStatement(query);
			// pstmt.setString(1,controllerId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				val = rs.getString(1);
			}
			if (val.equalsIgnoreCase("N")) {
			//autoUpdateTrades();
			}
			return val;

		}
		
		
		public ArrayList<Integer> getEventCustomer(int eventId) throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			ArrayList<Integer> customerList = new ArrayList<>();
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query = "select customer_id from event_customer_mapping where event_id="+eventId +" and event_customer_status_id=8";
			pstmt = con.prepareStatement(query);
			// pstmt.setString(1,controllerId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				customerList.add(rs.getInt(1));
			}
			
			return customerList;

		}

		
		public ArrayList<HashMap<String,Object>> getEventCustomerData(int customerId, int eventId) throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			ArrayList<HashMap<String,Object>> customerData = new ArrayList<>();
			int count =0;
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query1 = "";
			String query = "select distinct kiot_user_mappings.kiot_user_mapping_id,kiot_user_mappings.kiot_user_id,kiot_user_mappings.bearer_token,all_kiot_switches.kiot_device_id \n" + 
					",all_kiot_switches.custom_data from " + 
					"	all_kiot_switches , kiot_user_mappings  , all_users  " + 
					"where all_users.user_id = "+customerId+" and all_users.dr_contract_number=kiot_user_mappings.contract_number " + 
					" and kiot_user_mappings.kiot_user_mapping_id is not null and kiot_user_mappings.kiot_user_mapping_id = all_kiot_switches.kiot_user_mapping_id";
			pstmt = con.prepareStatement(query);
			// pstmt.setString(1,controllerId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				HashMap<String,Object> data = new HashMap<String, Object>();
				data.put("kiotUserMappingId",rs.getInt(1));
				data.put("kiotUserId",rs.getString(2));
				data.put("bearerToken",rs.getString(3));
				data.put("kiotDeviceId",rs.getString(4));
				data.put("customData",rs.getString(5));
				customerData.add(data);
				count++;
			}
			if (count == 0) {
				 query="update event_customer_mapping set event_customer_status_id=12 where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,customerId); 
				  pstmt.execute();
			}
			return customerData;

		}


		public ArrayList<String> getDeviceIds(int customerId) throws ClassNotFoundException, SQLException, JSONException {
			PreparedStatement pstmt = null;
			ArrayList<String> deviceData = new ArrayList<>();
			int count =0;
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query1 = "";
			String query = "SELECT custom_data FROM energytrade.all_users a, energytrade.kiot_user_mappings b , energytrade.all_kiot_switches c " + 
					"where a.dr_contract_number = b.contract_number and c.kiot_user_mapping_id= b.kiot_user_mapping_id " + 
					"and a.user_id= "+customerId;
			pstmt = con.prepareStatement(query);
			// pstmt.setString(1,controllerId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				JSONObject js = new JSONObject(rs.getString(1));
				deviceData.add((String)js.get("ud_id"));
			}
			
			return deviceData;

		}

		public String getConnectionString(int customerId, int eventId) throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			int count=0;
			ArrayList<HashMap<String,Object>> customerData = new ArrayList<>();
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String connectionString = "";
			String query = "select dr_device_repository.connection_string from 	dr_device_repository ,all_users \n" + 
					"where all_users.user_id = "+customerId +" and all_users.dr_contract_number=dr_device_repository.contract_number";
			pstmt = con.prepareStatement(query);
			// pstmt.setString(1,controllerId);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				count++;
				connectionString=rs.getString(1);
			}
			if (count ==0) {
				query="update event_customer_mapping set event_customer_status_id=11 where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,customerId); 
				  pstmt.execute();
			}
			
			return connectionString;

		}

		public int getEventCustomerStatus(int customerId, int eventId) throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			int customerStatus=0;
			ArrayList<HashMap<String,Object>> customerData = new ArrayList<>();
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query="select event_customer_status_id from event_customer_mapping where event_id=? and customer_id=?";
			  pstmt=ScheduleDAO.con.prepareStatement(query);
			  pstmt.setInt(1,eventId); 
			  pstmt.setInt(2,customerId); 
			  ResultSet rs = pstmt.executeQuery();
			  while(rs.next()) {
				  customerStatus = rs.getInt("event_customer_status_id");
			 }
			
			return customerStatus;

		}


		public int getContractType(int userId) throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			int contractType=0;
			ArrayList<HashMap<String,Object>> customerData = new ArrayList<>();
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query="select contract_type_id from dr_contracts,all_users where dr_contracts.contract_number=all_users.dr_contract_number and all_users.user_id =? ";
			  pstmt=ScheduleDAO.con.prepareStatement(query);
			  pstmt.setInt(1,userId); 
			  ResultSet rs = pstmt.executeQuery();
			  while(rs.next()) {
				  contractType = rs.getInt("contract_type_id");
			 }
			
			return contractType;

		}
		

		public String getBearerToken(int userId) throws ClassNotFoundException, SQLException {
			PreparedStatement pstmt = null;
			String bearerToken="";
			try {
			ArrayList<HashMap<String,Object>> customerData = new ArrayList<>();
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query="select b.bearer_token from all_users a, kiot_user_mappings b where a.dr_contract_number = b.contract_number " + 
					"and a.user_id= "+userId;
			  pstmt=ScheduleDAO.con.prepareStatement(query);
			//  pstmt.setInt(1,userId); 
			  ResultSet rs = pstmt.executeQuery();
			  while(rs.next()) {
				  bearerToken = rs.getString("bearer_token");
			 }
			}
			catch(Exception e) {
				e.printStackTrace();
			}
			return bearerToken;

		}

		public double getAveragePower(String startTime) throws ClassNotFoundException, SQLException, ParseException {
			double averagePower=0;
			try {
			System.out.println("Get Average Power --- startTime" + startTime);
			PreparedStatement pstmt = null;
			
			//startTime = "2020-07-15 14:45:00";
			String pattern = "yyyy-MM-dd HH:mm:ss";
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
			Date date = simpleDateFormat.parse(startTime);
			int day =date.getDay()+1;
			System.out.println("Get Average Power --- day "+day);
			String startTimeArr[] = startTime.split(" ");
			String timeArr[] = startTimeArr[1].split(":");
			String timeToBeCompared ="";
			int min= Integer.parseInt(timeArr[1]);
			if (min == 15 || min == 0) {
				timeToBeCompared = timeArr[0]+":00";
			} else if (min == 45 || min ==30) {
				timeToBeCompared = timeArr[0]+":30";
			}
			System.out.println("timeToBeCompared"+timeToBeCompared);
			
			ArrayList<HashMap<String,Object>> customerData = new ArrayList<>();
			if (con == null) {
				con = JDBCConnection.getOracleConnection();
			}
			String query = "";
			if (day != 1 && day !=7) {
			query="select b.average_power from all_timeslots_power b where b.time_slot_name =  '" +timeToBeCompared+ 
					"' and b.day= 5";
			} else {
				query="select b.average_power from all_timeslots_power b where b.time_slot_name =  '" +timeToBeCompared+ 
						"' and b.day= '"+Integer.toString(day)+"'";
			}
			   pstmt=ScheduleDAO.con.prepareStatement(query);
			//  pstmt.setInt(1,userId); 
			  ResultSet rs = pstmt.executeQuery();
			  while(rs.next()) {
				  averagePower = rs.getDouble("average_power");
			 }
					}
			catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("Get Average Power --- " +averagePower);
			return averagePower;

		}
		
		public static void main(String args[]) throws ClassNotFoundException, SQLException, ParseException {
			ScheduleDAO scd = new ScheduleDAO();
			scd.getAveragePower("2020-07-27 21:00");
		}
}
