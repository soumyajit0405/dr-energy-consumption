import java.beans.Customizer;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



public class DBHelper {

	static Connection con;
	 
	 public void updateEndTradeEventCustomer(double meterReading, int eventId, int userId, String status, String endTime) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
		 System.out.println("Inside After--- ");
			BigDecimal zero =new BigDecimal(0);
		 PreparedStatement pstmt = null;
			BigDecimal actualPower= zero;
			double meterStartReading =0;

			if(ScheduleDAO.con!=null || con!=null)
			{
				System.out.println("Inside --- ");
				String query="select customer_net_meter_reading_s from event_customer_mapping where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs=pstmt.executeQuery();
				  while(rs.next()) {
					  meterStartReading=rs.getFloat(1);
				  } 
				  System.out.println("meterStartReading--- "+meterStartReading);
				  BigDecimal transferredPower = BigDecimal.valueOf((meterReading - meterStartReading )*4);
				  if (transferredPower.compareTo(zero) < 0) {
					  transferredPower =zero;
				  }
				  System.out.println("transferredPower--- "+transferredPower);
				  ScheduleDAO scd = new ScheduleDAO();
				  BigDecimal averagePower = BigDecimal.valueOf(scd.getAveragePower(endTime));
				  System.out.println("averagePower ---1 " +averagePower);
				  actualPower = averagePower.subtract(transferredPower);
				  System.out.println("actualPower--- "+actualPower);
				  if (actualPower.compareTo(zero) < 0) {
					  actualPower =zero;
				  }
				  System.out.println("Before update of end meter reading "+ status + "  "+meterReading);
				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
					 query="update event_customer_mapping set customer_net_meter_reading_e=? ,actual_power=?,event_customer_status_id=15 where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,meterReading);
					  pstmt.setDouble(2,actualPower.doubleValue());
					  pstmt.setInt(3,eventId); 
					  pstmt.setInt(4,userId); 
					  pstmt.execute();
					  
					  System.out.println("After update of end meter reading");  
					  query="select actual_power from event_customer_mapping where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					   rs=pstmt.executeQuery();
					  while(rs.next()) {
						  actualPower=BigDecimal.valueOf(rs.getDouble(1));
					  }
					  query="update all_events set actual_power=actual_power+? where event_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,actualPower.doubleValue());
					  pstmt.setInt(2,eventId); 
					  pstmt.execute();
					  validate(eventId,userId); 
			
				} else {
						 query="update event_customer_mapping set event_customer_status_id=11 where event_id=? and customer_id=?";
						  pstmt=ScheduleDAO.con.prepareStatement(query);
						  pstmt.setInt(1,eventId); 
						  pstmt.setInt(2,userId); 
						  pstmt.execute();
						  
						
				}
				
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}

	 
	 public void updateKiotEndTradeEventCustomer(double meterReading, int eventId, int userId, String status) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			BigDecimal zero =new BigDecimal(0);
			BigDecimal actualPower=zero;
			BigDecimal meterStartReading =zero;
			
			BigDecimal four =new BigDecimal(4);
			BigDecimal oneTwenty =new BigDecimal(120);
			if(ScheduleDAO.con!=null)
			{ 
				String query="select customer_net_meter_reading_s from event_customer_mapping where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs=pstmt.executeQuery();
				  while(rs.next()) {
					  meterStartReading=BigDecimal.valueOf(rs.getFloat(1));
				  } 
				  BigDecimal transferredPower = (BigDecimal.valueOf(meterReading).subtract((meterStartReading) )).multiply(four);
				  if (transferredPower.compareTo(zero) <0) {
					  transferredPower =zero;
				  }
				  actualPower = oneTwenty.subtract(transferredPower);
				  if (actualPower.compareTo(zero ) < 0) {
					  actualPower =zero;
				  }
				  
				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
					 query="update event_customer_mapping set customer_net_meter_reading_e=? ,actual_power=?,event_customer_status_id=15 where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,meterReading);
					  pstmt.setDouble(2,actualPower.doubleValue());
					  pstmt.setInt(3,eventId); 
					  pstmt.setInt(4,userId); 
					  pstmt.execute();
					  
					  
					  query="select actual_power from event_customer_mapping where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					   rs=pstmt.executeQuery();
					  while(rs.next()) {
						  actualPower=BigDecimal.valueOf(rs.getDouble(1));
					  }
					  query="update all_events set actual_power=actual_power+? where event_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,actualPower.doubleValue());
					  pstmt.setInt(2,eventId); 
					  pstmt.execute();
					  validate(eventId,userId); 
			
				} else {
						 query="update event_customer_mapping set event_customer_status_id=12 where event_id=? and customer_id=?";
						  pstmt=ScheduleDAO.con.prepareStatement(query);
						  pstmt.setInt(1,eventId); 
						  pstmt.setInt(2,userId); 
						  pstmt.execute();
						  
						
				}
				
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}

	 public void updateKiotEnergy(double meterReading, int eventId, int userId, String status) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			double actualPower=0;
			double committedPower =0;
			if(ScheduleDAO.con!=null)
			{ 
				String query="select  commited_power from event_customer_mapping where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs=pstmt.executeQuery();
				  while(rs.next()) {
					  committedPower=rs.getFloat(1);
				  } 
				  double transferredPower = (meterReading/(3600*1000))*4;
				  
				  actualPower = committedPower - transferredPower;
	
				  
				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
					 query="update event_customer_mapping set actual_power=?,event_customer_status_id=15 where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  //pstmt.setDouble(1,meterReading);
					  pstmt.setDouble(1,actualPower);
					  pstmt.setInt(2,eventId); 
					  pstmt.setInt(3,userId); 
					  pstmt.execute();
					  
					  
					  query="select actual_power from event_customer_mapping where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					   rs=pstmt.executeQuery();
					  while(rs.next()) {
						  actualPower=rs.getDouble(1);
					  }
					  query="update all_events set actual_power=actual_power+? where event_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,actualPower);
					  pstmt.setInt(2,eventId); 
					  pstmt.execute();
					  validate(eventId,userId); 
			
				} else {
						 query="update event_customer_mapping set event_customer_status_id=12 where event_id=? and customer_id=?";
						  pstmt=ScheduleDAO.con.prepareStatement(query);
						  pstmt.setInt(1,eventId); 
						  pstmt.setInt(2,userId); 
						  pstmt.execute();
						  
						
				}
				
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}

	 public void validate( int eventId, int userId) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			BigDecimal zero = new BigDecimal(0);
			BigDecimal four = new BigDecimal(4);
			BigDecimal onePointTwo = new BigDecimal(1.2);
			BigDecimal actualPower=zero, committedPower=zero, bidPrice=zero;
			if(ScheduleDAO.con!=null)
			{
				
				String query="select actual_power,commited_power,bid_price from event_customer_mapping  where event_id =? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs=pstmt.executeQuery();
				  while(rs.next()) {
					  actualPower = BigDecimal.valueOf(rs.getDouble("actual_power"));
					  committedPower = BigDecimal.valueOf(rs.getDouble("commited_power"));
					  bidPrice = BigDecimal.valueOf(rs.getDouble("bid_price"));
				  }
				  BigDecimal fine = ((committedPower.subtract(actualPower)).divide(four)).multiply((bidPrice).multiply(onePointTwo));
				  BigDecimal commitedNew = committedPower.multiply(new BigDecimal(0.03));
				  
				  if((actualPower.subtract(committedPower)).compareTo(zero) >= 0 && actualPower.compareTo(zero) > 0 ) {
					  BigDecimal earnings = (committedPower.divide(four)).multiply(bidPrice);
					  query="update event_customer_mapping set customer_fine=0, is_fine_applicable='N',earnings="+earnings+"  where event_id =? and customer_id=?";
						//	  }
							  pstmt=ScheduleDAO.con.prepareStatement(query);
							  pstmt.setInt(1,eventId); 
							  pstmt.setInt(2,userId); 
							  pstmt.execute();
				  } else {
				  if((committedPower.subtract(actualPower)).compareTo(commitedNew) >= 0 && actualPower.compareTo(zero) > 0 ) {
					  BigDecimal earnings = (actualPower.divide(four)).multiply(bidPrice);
				//	  fine = ((committedPower.subtract(actualPower)).divide(four)).multiply((bidPrice).multiply(onePointTwo));
				//	  if (fine.compareTo(zero) == 0) {
				//		   query="update event_customer_mapping set customer_fine="+fine+", is_fine_applicable='N',earnings="+earnings+"  where event_id =? and customer_id=?";
				//	  } else {
					  query="update event_customer_mapping set customer_fine=0, is_fine_applicable='Y',earnings="+earnings+"  where event_id =? and customer_id=?";
				//	  }
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
					  
				  } else if((committedPower.subtract(actualPower)).compareTo(commitedNew) < 0 && actualPower.compareTo(zero) > 0 ) {
					  BigDecimal earnings = (committedPower.divide(four)).multiply(bidPrice);
					  query="update event_customer_mapping set customer_fine=0, is_fine_applicable='N',earnings="+earnings+"  where event_id =? and customer_id=?";
					  //query="update event_customer_mapping set customer_fine=0, is_fine_applicable='N',earnings=0  where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				  } else {
					  BigDecimal earnings = (committedPower.divide(four)).multiply(bidPrice);
					  //query="update event_customer_mapping set customer_fine=0, is_fine_applicable='N',earnings="+earnings+"  where event_id =? and customer_id=?";
					  query="update event_customer_mapping set customer_fine=0, is_fine_applicable='Y',earnings=0  where event_id =? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				  }
				  }
						  
					}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}
	 
	 public void updateEventCustomer(double meterReading, int eventId, int userId, String status) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			String deviceName="";
			if(ScheduleDAO.con!=null)
			{
				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
					String query="update event_customer_mapping set customer_net_meter_reading_s=?,event_customer_status_id=15 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,meterReading);
					  pstmt.setInt(2,eventId); 
					  pstmt.setInt(3,userId); 
					  pstmt.execute();
				} else {
					String query="update event_customer_mapping set event_customer_status_id=11 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				}
					 
					 
					
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }
		}
	 
	 public void updateKiotEventCustomer(double meterReading, int eventId, int userId, String status) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			String deviceName="";
			if(ScheduleDAO.con!=null)
			{
				if (status.equalsIgnoreCase("ne") && meterReading !=0) {
					String query="update event_customer_mapping set customer_net_meter_reading_s=?,event_customer_status_id=15 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setDouble(1,meterReading);
					  pstmt.setInt(2,eventId); 
					  pstmt.setInt(3,userId); 
					  pstmt.execute();
				} else {
					String query="update event_customer_mapping set event_customer_status_id=12 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				}
					 
					 
					
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }
		}
		
		public void updateEventCustomer( int eventId, int userId) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			int customerStatus=0;
			if(ScheduleDAO.con!=null)
			{
				String query="select event_customer_status_id from event_customer_mapping where event_id=? and customer_id=?";
				  pstmt=ScheduleDAO.con.prepareStatement(query);
				  pstmt.setInt(1,eventId); 
				  pstmt.setInt(2,userId); 
				  ResultSet rs = pstmt.executeQuery();
				  while(rs.next()) {
					  customerStatus = rs.getInt("event_customer_status_id");
				  }
				  if (customerStatus != 12 ) {
					 query="update event_customer_mapping set event_customer_status_id=12 where event_id=? and customer_id=?";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setInt(1,eventId); 
					  pstmt.setInt(2,userId); 
					  pstmt.execute();
				  }	 
					 
					
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}

		
		public static void main(String args[]) throws ClassNotFoundException, SQLException {
			BigDecimal committedPower = new BigDecimal(20);
			BigDecimal actualPower = new BigDecimal(19.9997);
			BigDecimal bidPrice = new BigDecimal(60);
			BigDecimal onePointTwo = new BigDecimal(1.2);
				BigDecimal fine = ((committedPower.subtract(actualPower)).divide(new BigDecimal(4))).multiply((bidPrice).multiply(onePointTwo));
			  BigDecimal commitedNew = committedPower.multiply(new BigDecimal(0.03));
			  System.out.println((committedPower.subtract(actualPower)).compareTo(commitedNew)); 
			
		}
}
