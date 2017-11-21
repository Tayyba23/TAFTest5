package com.td.tafd.parsers.logfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;



import java.util.List;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.vo.ModuleTimeInfo;
import com.td.tafd.vo.PieChartInfo;

/**
 * @author kt186036
 *
 */

public class LogFileParser 
{
	public ArrayList<ModuleTimeInfo> getModulesAndExecutionTimes(String fileName)
	{
		String currentModule = "Running Module";
		String name = "";
		long start = 0;
		long end = 0;
		
		ArrayList<ModuleTimeInfo> modulesAndExecutionTimes = new ArrayList<ModuleTimeInfo>();
		
		try {
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));

		String line = "";
		
		while ((line = reader.readLine()) != null)
		{	
			if(!"".equals(line.trim()) && !line.equals("\n"))
			{
				if(line.contains(currentModule))
				{
					name = (line.substring(line.indexOf(currentModule)+currentModule.length(), line.length()-1)).trim();
					start = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(line.substring(0,line.indexOf("INFO")).trim()).getTime();
				}
				
				else if(line.contains("completed successfully") && start != 0)
				{
					end = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(line.substring(0,line.indexOf("INFO")).trim()).getTime();
					
					modulesAndExecutionTimes.add(new ModuleTimeInfo(name, end-start));
					
					name = "";
					end = 0;
					start = 0;
				}
			}
		}

		reader.close();
		} catch(FileNotFoundException e) {
		} catch (IOException e) {
		} catch (ParseException e) {
		}
		
		return modulesAndExecutionTimes;
	}
	
	public String getLatestFilefromDir(String dirPath){
	    File dir = new File(dirPath);
	    File[] files = dir.listFiles();
	    if (files == null || files.length == 0) {
	        return null;
	    }

	    File lastModifiedFile = files[0];
	    for (int i = 1; i < files.length; i++) {
	       if (lastModifiedFile.lastModified() < files[i].lastModified() && (!files[i].getName().startsWith("Application"))) {
	           lastModifiedFile = files[i];
	       }
	    }
	    return lastModifiedFile.getAbsolutePath();
	}
	
	public void tableWiseExecutionTimes(ArrayList<ModuleTimeInfo> table1ExecutionTimes, ArrayList<ModuleTimeInfo> table2ExecutionTimes, ArrayList<ModuleTimeInfo> table3ExecutionTimes)
	{
		ArrayList<ModuleTimeInfo> modulesAndExecutionTimes = getModulesAndExecutionTimes(getLatestFilefromDir(System.getProperty("user.dir") + "/log files"));
		
		int switchToList = 0;
		
		for(ModuleTimeInfo module : modulesAndExecutionTimes)
		{
			if(module.getModuleName().trim().contains("VERIFICATION"))
				continue;
			if(module.getModuleName().trim().equalsIgnoreCase("'row count'"))
				switchToList += 1;
			
			if(switchToList == 1)
				table1ExecutionTimes.add(module);
			if(switchToList == 2)
				table2ExecutionTimes.add(module);
			if(switchToList == 3)
				table3ExecutionTimes.add(module);	
		}	
	}
	
	public List<PieChartInfo> getPassFailCounts()
	{
		List<PieChartInfo> pieChartInfoList = new ArrayList<PieChartInfo>();
		
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			String connurl = "jdbc:teradata://153.65.144.102";//	+ ConfigurationManager.getInstance().getUserConfig().getHostname();

			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(connurl, "dbc", "dbc");
			
			StringBuffer buffer = new StringBuffer();
			buffer.append("select test_status, count(test_status) Count_1 from tafd_db.sum_avg_recon_rslt group by test_status Union all select  test_status, count(test_status) Count_1 from tafd_db.min_max_recon_rslt group by test_status Union all select  test_status, count(test_status) Count_1 from tafd_db.hh_rslt group by test_status;");
			//buffer.append("select distinct count(test_status) as CountOf from " + ApplicationDatabaseStructure.getInstance().getDbName() + ".sum_avg_recon_rslt group by test_status");
			
			// repeat the above query for all result tables (find a group by solution)
			
			ps = conn.prepareStatement(buffer.toString());
			
			rs = ps.executeQuery();
			
			double [] passCnt = new double[3];
			double [] failCnt = new double[3];
			
			boolean isPassCnt = false;
			int i = 0;
			int passIndex = 0;
			int failIndex = 0;
			
			while(rs.next())
			{
				if(isPassCnt)
				{
					passCnt[passIndex] = rs.getInt(2);
					++passIndex;
				}
				else
				{
					failCnt[failIndex] = rs.getInt(2);
					++failIndex;
				}
				
				if(!isPassCnt && i==2)
					isPassCnt = true;
				++i;
			}
			
			for(i=0; i<passCnt.length; ++i)
			{
				pieChartInfoList.add(new PieChartInfo(passCnt[i], failCnt[i]));
			}
			
		} catch (SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().error("SQLException encountered while executing resultset, preparedStatement or getting connection in LogFileParser.getPassFailCounts()");
		} catch (ClassNotFoundException e) {
		} finally {
			try {
				if(rs != null)
					rs.close();
				if(ps != null)
					ps.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().error("SQLException encountered while closing resultset, preparedStatement or connection in LogFileParser.getPassFailCounts()");
			}
		}
		
		return pieChartInfoList;
	}
	
	/*public static void main(String [] args)
	{
		LogFileParser lfp = new LogFileParser();
		
		ArrayList<ModuleTimeInfo> table1ExecutionTimes = new ArrayList<ModuleTimeInfo>();
		ArrayList<ModuleTimeInfo> table2ExecutionTimes = new ArrayList<ModuleTimeInfo>();
		ArrayList<ModuleTimeInfo> table3ExecutionTimes = new ArrayList<ModuleTimeInfo>();
		
		lfp.tableWiseExecutionTimes(table1ExecutionTimes, table2ExecutionTimes, table3ExecutionTimes);
		
		System.out.println("\nSource/Target 1:\n");
		for(ModuleTimeInfo module : table1ExecutionTimes)
			System.out.println("Module Name: " + module.getModuleName() + ", Execution time: " + module.getExecutionTime());
		
		System.out.println("\nSource/Target 2:\n");
		for(ModuleTimeInfo module : table2ExecutionTimes)
			System.out.println("Module Name: " + module.getModuleName() + ", Execution time: " + module.getExecutionTime());
		
		System.out.println("\nSource/Target 3:\n");
		for(ModuleTimeInfo module : table3ExecutionTimes)
			System.out.println("Module Name: " + module.getModuleName() + ", Execution time: " + module.getExecutionTime());
	}*/
}
