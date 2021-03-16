

import java.awt.List;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;



public class JDBCConnection {
	private static Connection connection = null;
	static String  url="jdbc:mysql://103.120.179.22:3306/autoiinno_energytrade?noAccessToProcedureBodies=true";
	static String  userName="autoiinno_admin";
	static String  password="Autoi@12345";
	public static Connection getOracleConnection()
			throws ClassNotFoundException, SQLException {

		Class.forName("com.mysql.jdbc.Driver");  
		  
		/*Connection con=DriverManager.getConnection(  
				url,userName,password);    */
//		Connection con=DriverManager.getConnection(  
//				"jdbc:mysql://139.59.30.90:3306/energytrade_brpl_bckup?noAccessToProcedureBodies=true","root","Admin@12345");
		
		Connection con=DriverManager.getConnection(  
				"jdbc:mysql://134.209.154.124:3306/energytrade?noAccessToProcedureBodies=true","root","SIBRPLDEMO@123");
   

		return con;

	}
	
	
	/*public static void main(String args[]) throws ClassNotFoundException, SQLException, NamingException
	{
		
		Connection con=getOracleConnection();
		System.out.println(con);
		
	}*/
	
	public static Connection getConnection() throws NamingException, SQLException{
		/*Context ctx = InitialContext();
		Object obj = ctx.lookup("skfalkjfds");
		Connection conn = (Connection)obj;*/
		Connection conn=null;
		try
		{
		Context ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("java:/comp/env/jdbc/MyLocalDB");
		 conn= ds.getConnection();
		return conn;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return conn;
	}
	
	

	
}
