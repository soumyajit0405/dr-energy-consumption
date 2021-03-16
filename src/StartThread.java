

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;




class StartThread implements Runnable{  
public void run(){  
	//System.out.println("thread is running...")
	try {
		
		
		ActionThreadPoolExecutor atc=new ActionThreadPoolExecutor();
		
		atc.getStarted();
		/*int minsLeft=getTimeDifference();
		 System.out.println("minsLeft"+minsLeft);
		try
		{
			Thread.sleep(minsLeft*1000*1000);
		}
		catch (Exception e) {
			e.printStackTrace();
		}*/
		
	} catch (ClassNotFoundException | SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (JSONException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}  
  
public static void main(String args[]){  
	/*ScheduleDevice m1=new ScheduleDevice();  
Thread t1 =new Thread(m1);  
t1.start(); */

ExecutorService executor = Executors.newFixedThreadPool(1);//creating a pool of 2 threads  
for (int i = 0; i < 1; i++) {  
    Runnable startThread = new StartThread();  
    executor.execute(startThread);//calling execute method of ExecutorService  
  }  
executor.shutdown();  
while (!executor.isTerminated()) {  
//	System.out.println("Thread not closed");
}  

System.out.println("Finished all threads");  
 }  


public int getTimeDifference()
{
	Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    int minsLeft =  calendar.get(Calendar.MINUTE) % 5;
   
    if(minsLeft==0)
    {
    	return 5;
    }
    return minsLeft;
    
}
}  