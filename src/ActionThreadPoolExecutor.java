
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONException;

public class ActionThreadPoolExecutor {

	public void getStarted() throws ClassNotFoundException, SQLException, JSONException {
	ScheduleDAO scdao = new ScheduleDAO();
		System.out.println("Before start of all threads");
		ScheduleDAO sdo= new ScheduleDAO();
		if (sdo.getBlockChainSettings().equalsIgnoreCase("N") ) {
			
			ArrayList<HashMap<String, Object>> userDevices = scdao.getUserDevices();
			System.out.println("List of Pending Trades");
			if (userDevices.size() > 0) {

				ExecutorService executor = Executors.newFixedThreadPool(userDevices.size());// creating a pool of 1000
																							// threads
				for (int i = 0; i < userDevices.size(); i++) {
					Runnable worker = new WorkerThread((int) userDevices.get(i).get("userId"),(String) userDevices.get(i).get("kiotDeviceId"), (String) userDevices.get(i).get("bearerToken"),(int)userDevices.get(i).get("userDeviceId"));
					System.out.println("List of run workers");
					executor.execute(worker);// calling execute method of ExecutorService
				}
				executor.shutdown();
				while (!executor.isTerminated()) {
				}

			}
			System.out.println("Finished all threads");

		} else {
			
		}
			}
}
