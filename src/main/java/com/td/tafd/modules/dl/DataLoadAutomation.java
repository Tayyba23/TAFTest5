package com.td.tafd.modules.dl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
/*import java.net.URI;
import java.net.URISyntaxException;*/
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.poi.ss.usermodel.Sheet;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.core.JobTypeParser.extensions;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.modules.di.DistinctCountTestAutomator;
import com.td.tafd.modules.di.MinMaxTestAutomator;
import com.td.tafd.modules.di.NullCountTestAutomator;
import com.td.tafd.modules.di.RowCountTestAutomator;
import com.td.tafd.modules.di.SumAvgTestAutomator;
import com.td.tafd.vo.TestScriptExecutionInfo;

public class DataLoadAutomation 
{
	private static DataLoadAutomation dataLoadAutomator = null;
	
	private int streamId;
	private int subStreamId;
	private String environment;
	private String procType;		// Nullable
	private String procName;
	private String procStartTime;
	private String procEndTime;
	private String execStatus;
	private String recordsInserted;	// Nullable
	private String recordsUpdated;	// Nullable
	private String recordsDeleted;	// Nullable
	private int userId;
	
	private DataLoadAutomation() {	
		streamId = 0;
		subStreamId = 0;
		
		environment = "";
		procType = "";
		procName = "";
		procStartTime = "";
		procEndTime = "";
		execStatus = "";
		
		recordsInserted = "";
		recordsUpdated = "";
		recordsDeleted = "";
		userId = 0;
	}
	
	public static DataLoadAutomation getInstance()
	{
		if(dataLoadAutomator == null)
			dataLoadAutomator = new DataLoadAutomation();
		
		return dataLoadAutomator;
	}
	
	public String retrieveQuery()
	{
		Sheet execCheckSheet = JobTypeParser.getWorkbook().getSheet("Execution Status Check");
		String query = execCheckSheet.getRow(0).getCell(0).getStringCellValue();
		return query;
	}
	
	public boolean queryExecuted() {
		return (!procStartTime.equals(""));		// if environment != null, the query has already been executed
	}
	
	public void executeOnRemoteServer(String scriptName)
	{
		String USERNAME ="root"; // username for remote host
		String PASSWORD ="root"; // password of the remote host
		String host = "153.65.144.59"; // remote host address
		int port=22;
		
		//File file;
		BufferedWriter writer = null;
		
		File file = new File(System.getProperty("user.dir") + "\\output log\\testing2.txt");
		
		try {
			writer = new BufferedWriter(new FileWriter(file));
		} catch (FileNotFoundException e1) {
		} catch (IOException e) {
		}

		try
		{
			/**
			 * Create a new Jsch object
			 * This object will execute shell commands or scripts on server
			 */
			JSch jsch = new JSch();

			/* session.connect() opens a new connection to remote SSH server.
			 * Once the connection is established, you can initiate a new channel.
			 * this channel is needed to connect to remotely execution program
			 */
			Session session = jsch.getSession(USERNAME, host, port);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(PASSWORD);
			session.connect();

			//create the execution channel over the session
			
			Channel channel=session.openChannel("exec");
			
			// Set the command i.e. the remote shell script in this case

            ((ChannelExec) channel).setCommand("bash -s < ." + scriptName);
			//ChannelExec channelExec = (ChannelExec)session.openChannel("exec");

			// Gets an InputStream for this channel. All data arriving in as messages from the remote side can be read from this stream.
			InputStream in = channel.getInputStream();
			
			// Execute the command
			JobTypeParser.getApplicationlogger().info("connecting");
			channel.connect();
			JobTypeParser.getApplicationlogger().info("connected");
			// Read the output from the input stream set above
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;

			//Read each line from the buffered reader and add it to result list
			while ((line = reader.readLine()) != null)
			{
				writer.write(line + "\n");
			}

			//retrieve the exit status of the remote command corresponding to this channel
			int exitStatus = channel.getExitStatus();

			//Safely disconnect channel and disconnect session
			channel.disconnect();
			session.disconnect();

			if(exitStatus < 0) {
				 System.out.println("Executed, but exit status not set. Exit Status: " + exitStatus);
			}
			else if(exitStatus > 0) {
				System.out.println("Executed with error. Exit Status: " + exitStatus);
			}
			else {
				System.out.println("Executed Successfully");
			}
			
			if(writer!=null)
				writer.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
		//return result;
	}
	/** 
	 * Executes the file name passed to it and maintains logs of the execution 
	 * by the same name as the file passed to it
	 * Supported file types: .tpt, .bat, .fld, .btq
	 */

	public void runFile(String file_name)
	{
		JobTypeParser.getLogger().debug("\t\t------------------------------------------------------------------------------\n");
		JobTypeParser.getLogger().debug("\t\t\tExecuting File \'"+ file_name + "\' \n");
		JobTypeParser.getLogger().debug("\t\t------------------------------------------------------------------------------\n");

		boolean file_format_determined = false;
		boolean osIsWindows = ApplicationDatabaseStructure.isOSWindows();
		
		String curr_dir = System.getProperty("user.dir").toString();
		String log_folder = curr_dir + "\\output log\\";
		String f_name = "";
		//URL url = null;
		
		try 
		{
			new URL(file_name.toString());
		} 
		
		catch (MalformedURLException e) 
		{
			//e.printStackTrace();
			f_name = new File(file_name).getName();
		}		
		
		try 
		{			
			File file = null;
			Process p = null;
			
			if(file_name.endsWith("bat"))								// Handling for .bat file
			{
				p = Runtime.getRuntime().exec("cmd /c start /wait " + file_name);	// Running the .bat file
			}
			
			else														// Handling for files other than .bat files
			{
				//try {
					//file = new File(new URI("file:////153.65.144.59/root/Desktop")  + "/temp.bat");
					
					if(osIsWindows)
						file = new File(System.getProperty("user.dir") + "/temp.bat");	// batch file for windows system
					else
						file = new File(System.getProperty("user.dir") + "/temp.sh");	// shell script for linux
					
					//System.out.println("file.getAbsolutePath(): " + file.getAbsolutePath());
					
					file.createNewFile();
				/*} catch (URISyntaxException e) {
					e.printStackTrace();
				}*/		// create a new temporary batch file
				//file.createNewFile();
				PrintWriter writer = new PrintWriter(file, "UTF-8");			// create PrintWriter for writing to newly created batch file or shell script
				
				if(!osIsWindows)
				{
					writer.println("#!/bin/bash");
				}
				
				for(String b : extensions.BTEQ.getValue())						// handling for BTEQ execution
				{
					if(file_name.endsWith(b))	
					{							
						writer.println("bteq < \""+ file_name +"\" > \"" + log_folder + f_name + "3.txt\" 2>&1");
						file_format_determined = true;
					}
				}
				
				for(String t : extensions.TPT.getValue())						// handling for TPT execution
				{
					if(file_name.endsWith(t))
					{
						writer.println("tbuild > \"" + log_folder + f_name + ".txt\" -f \""+ file_name +"\"");
						file_format_determined = true;
					}
				}
				
				for(String f : extensions.FLD.getValue())						// handling for FLD execution
				{
					if(file_name.endsWith(f))
					{
						writer.println("fastload < \""+ file_name +"\" > \"" + log_folder + f_name + ".txt\" 2>&1");
						file_format_determined = true;
					}
				}
				
				for(String m : extensions.MLD.getValue())						// handling for MLD execution
				{
					if(file_name.endsWith(m))
					{
						writer.println("multiload < \""+ file_name +"\" > \"" + log_folder + f_name + ".txt\" 2>&1");
						file_format_determined = true;
					}
				}
				
				if(file_name.endsWith("sh"))									// handling for shell script execution
				{
					if(osIsWindows)
					{
						JobTypeParser.getLogger().error("Shell script cannot be run on Windows. Exiting from module");
						writer.close();
						if(file != null)
							file.delete();					// delete the temporary batch file, if it exists
						return;
					}
					
					writer.println("./" + file_name);
					file_format_determined = true;
				}
				
				if(!file_format_determined)					// if file format cannot be determined, exit from module
				{
					JobTypeParser.getLogger().debug("File format not supported");
					writer.close();
					if(file != null)
						file.delete();						// delete the temporary batch file, if it exists
					return;
				}

				if(osIsWindows)
					writer.println("exit");				// 'exit' causes terminal window to close after temporary batch file's execution
				
				writer.close();
				//System.out.println(file.getPath() + "/" + file.getName());
				
				if(osIsWindows)
					p = Runtime.getRuntime().exec("cmd /c start \"\" /b /wait \"" + file.getPath() + "\"");	// Running the file to execute commands written to it
				else
				{
					final ProcessBuilder pb = new ProcessBuilder("/bin/sh", "temp.sh");
					pb.directory(new File(System.getProperty("user.dir").toString()));
					// redirect stdout, stderr, etc
					p = pb.start();
				}
			}
			
			int return_code = p.waitFor();

			if(return_code != 0)			// checking for unsuccessful execution
			{
				JobTypeParser.getLogger().debug("Unsuccessful execution");
			}
			
			if(file != null)
				file.delete();					// delete the temporary batch file, if it exists
		} 

		catch (IOException e) 				// IOException Handling
		{
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().error("Exception in Test Script Execution. Exception is: " + e);
		} 

		catch (InterruptedException e) 		// Interrupted Exception Handling for Process
		{
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().error("Exception in Test Script Execution. Exception is: " + e);
		}
	}
	
	public boolean assignValues(final String query)
	{	
		TeradataDataSource tdSource = new TeradataDataSource();
		Connection conn = null;
		try {
			conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
			PreparedStatement ps = conn.prepareStatement(query);
			ResultSet rs = ps.executeQuery();
			
			if(rs.next())
			{
				streamId = Integer.parseInt(rs.getString(1));
				subStreamId = Integer.parseInt(rs.getString(2));
				environment = rs.getString(3);
				
				procType = rs.getString(4);				// Nullable
				procName = rs.getString(5);
				procStartTime = rs.getString(6);
				procEndTime = rs.getString(7);
				execStatus = rs.getString(8);
				
				recordsInserted = rs.getString(9);		// Nullable
				recordsUpdated = rs.getString(10);		// Nullable
				recordsDeleted = rs.getString(11);		// Nullable
				
				userId = Integer.parseInt(rs.getString(12));
				rs.close();
				ps.close();
				
				conn.close();
				
				JobTypeParser.getApplicationlogger().debug("Values retrieved from query entered by user:");
				JobTypeParser.getApplicationlogger().debug("streamId: " + streamId);
				JobTypeParser.getApplicationlogger().debug("subStreamId: " + subStreamId);
				JobTypeParser.getApplicationlogger().debug("environment: " + environment);
				JobTypeParser.getApplicationlogger().debug("procType: " + procType);
				JobTypeParser.getApplicationlogger().debug("procName: " + procName);
				JobTypeParser.getApplicationlogger().debug("procStartTime: " + procStartTime);
				JobTypeParser.getApplicationlogger().debug("procEndTime: " + procEndTime);
				JobTypeParser.getApplicationlogger().debug("execStatus: " + execStatus);
				JobTypeParser.getApplicationlogger().debug("recordsInserted: " + recordsInserted);
				JobTypeParser.getApplicationlogger().debug("recordsUpdated: " + recordsUpdated);
				JobTypeParser.getApplicationlogger().debug("recordsDeleted: " + recordsDeleted);
				JobTypeParser.getApplicationlogger().debug("userId: " + userId);
				return true;
			}
			
			else
			{
				JobTypeParser.getLogger().error("The query entered in sheet \'Execution Status Check\' did not return any rows. Module \'Execution Status Check\' cannot be executed");
				return false;
			}
		} catch (SQLException e) {	
			JobTypeParser.getApplicationlogger().error("Exception in DataLoadAutomation.assignValues(): " + e);
			return false;
		} finally {
			try {
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().error("Exception in DataLoadAutomation.assignValues(): " + e);
			}
		}
			
	}
	
	// Module 19
	public void DataLoadScheduler()
	{
		for(TestScriptExecutionInfo testScriptInfo : JobTypeParser.getTestScriptInfoList())
		{
			if(!testScriptInfo.getIntegratedLayerScript().equals("") && testScriptInfo.getIntegratedLayerScript() != null)	// running data filtering script only if it has been entered
				runFile(testScriptInfo.getIntegratedLayerScript());
			
			runFile(testScriptInfo.getTestScript());		// running test script
		}
		
		List<String> sourcesBackup = JobTypeParser.getReader().getSources();	// storing original values for reversion later
		List<String> targetsBackup = JobTypeParser.getReader().getTargets();
		
		JobTypeParser.getReader().getSources().clear();		// clearing old values
		JobTypeParser.getReader().getTargets().clear();
		
		String testResultTbl;
		String intLayerTbl;
		
		for(TestScriptExecutionInfo testScriptInfo : JobTypeParser.getTestScriptInfoList())		// updating sources and targets for Row Count & Metrics Collection tests
		{
			testResultTbl = testScriptInfo.getTestResultsetDb() + "." + testScriptInfo.getTestResultsetTbl();
			intLayerTbl = testScriptInfo.getIntegratedLayerDb() + "." + testScriptInfo.getIntegratedLayerTbl();
			
			JobTypeParser.getReader().getSources().add(testResultTbl);
			JobTypeParser.getReader().getTargets().add(intLayerTbl);
		}
		
		runRowCountAndMetricsCollectionTests();		// run the row count and metrics collection tests on the two tables
		
		JobTypeParser.getReader().getSources().clear();		// clearing old values
		JobTypeParser.getReader().getTargets().clear();
		
		JobTypeParser.getReader().setSources(sourcesBackup);	// resetting original values
		JobTypeParser.getReader().setTargets(targetsBackup);
		
		//runFile("/root/Desktop/mirror.btq");
		//runFile("C:/Users/kt186036/Desktop/Transferred/Desktop/Assignment/mirror.btq");
	}
	
	public void runRowCountAndMetricsCollectionTests()
	{
		ExecutorService service = null;
		
		try {
			service = Executors.newFixedThreadPool(5);
		} catch(Exception e){
			JobTypeParser.getApplicationlogger().error("Exception in DataLoadAutomation.runRowCountAndMetricsCollectionTests(): " + e);
		} finally {
		}

		Set<Callable<Void>> callables = new HashSet<Callable<Void>>();
		
		callables.add(new RowCountTestAutomator("TRN_TST_Row_Count_Rslt", false));
		callables.add(new DistinctCountTestAutomator("TRN_TST_Distinct_Value_Count_Rslt", false));
		callables.add(new SumAvgTestAutomator("TRN_TST_Sum_Avg_Recon_Rslt", false));
		callables.add(new MinMaxTestAutomator("TRN_TST_Min_Max_Recon_Rslt", false));
		callables.add(new NullCountTestAutomator("TRN_TST_Null_Value_Count_Rslt", false));
		
		try {
			List<Future<Void>> futures = service.invokeAll(callables);	// invoking all threads

			for(Future<Void> future : futures) {
				JobTypeParser.getApplicationlogger().info("future.get() = " + future.get());
			}
		} catch (InterruptedException | ExecutionException e) {
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().error("Exception in DataLoadAutomation.runRowCountAndMetricsCollectionTests(): " + e);
		}
		
		service.shutdown();
	}
}
