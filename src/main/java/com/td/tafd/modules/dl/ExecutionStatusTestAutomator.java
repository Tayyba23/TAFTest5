/**
 * 
 */
package com.td.tafd.modules.dl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

import org.apache.poi.ss.usermodel.Sheet;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.validation.Validator;

/**
 * @author kt186036
 *
 */

public class ExecutionStatusTestAutomator implements Callable<Void>
{
	private Connection dbConn;
	private TeradataDataSource tdSource;
	
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
	
	private String moduleStatus;
	/**
	 * @return the dbConn
	 */
	public Connection getDbConn() {
		return dbConn;
	}
	
	/**
	 * @param dbConn the dbConn to set
	 */
	public void setDbConn(Connection dbConn) {
		this.dbConn = dbConn;
	}
	
	/**
	 * @return the tdSource
	 */
	public TeradataDataSource getTdSource() {
		return tdSource;
	}
	
	/**
	 * @param tdSource the tdSource to set
	 */
	public void setTdSource(TeradataDataSource tdSource) {
		this.tdSource = tdSource;
	}
	
	public ExecutionStatusTestAutomator()
	{
		tdSource = new TeradataDataSource();
		dbConn = null;
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
		
		moduleStatus = "Unsuccessful";
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
				
				return true;
			}
			
			else
			{
				JobTypeParser.getLogger().error("The query entered in sheet \'Execution Status Check\' did not return any rows. Module \'Execution Status Check\' cannot be executed");
				return false;
			}
		} catch (SQLException e) {	
			e.printStackTrace();
			return false;
		}
			
	}
	
	@Override
	public Void call() throws Exception 
	{
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'EXECUTION STATUS CHECK\'			|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");

		long startTime = System.currentTimeMillis();
		long endTime = 0;
		String query = retrieveQuery();
		
		TeradataDataSource tdSource = new TeradataDataSource();
		Connection conn = null;
		PreparedStatement ps = null;
		
		try {
			if(!queryExecuted())
			{
				if(!assignValues(query))
				{
					JobTypeParser.getLogger().error("The query entered in sheet \'Execution Status Check\' did not return any rows. Module \'Execution Status Check\' cannot be executed");
					return null;
				}
			}
			
			if(procType == null)
				procType = "NA";
			
			String testStatus = (execStatus.trim().equalsIgnoreCase("successful")) ? "Passed" : "Failed";
			
			String insertionQuery = new StringBuilder().append("Insert into ").append(ApplicationDatabaseStructure.getInstance().getDbName()).append(".Exec_Status_Check_Rslt (Stream_id, Sub_Stream_Id, Test_Cycle_Id, Test_Type_Cd, Test_Status, Process_Type, Process_Name, Process_Exec_Start_TS, Process_Exec_End_TS, Process_Exec_Status, Records_Inserted, Records_Updated, Records_Deleted, Script_Text, Business_Date, User_Id, Env_Id, execution_timestamp) VALUES (").append(streamId).append(", ").append(subStreamId).append(", \'").append(environment).append("_").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', ").append(ConfigurationManager.getInstance().getAppConfig().getExecCheck()).append(", \'").append(testStatus).append("\', \'").append(procType).append("\', \'").append(procName).append("\', \'").append(procStartTime).append("\', \'").append(procEndTime).append("\', \'").append(execStatus).append("\', \'").append(recordsInserted).append("\', \'").append(recordsUpdated).append("\', \'").append(recordsDeleted).append("\', \'").append(query).append("\', \'").append(ConfigurationManager.getInstance().getUserConfig().getBusinessDate()).append("\', \'").append(userId).append("\', \'").append(environment).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\');").toString();
			conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
			JobTypeParser.getExecchecklogger().info("Module 3 query: " + insertionQuery);
			ps = conn.prepareStatement(insertionQuery);
			ps.execute();
			ps.close();
			conn.close();
			
			QueryGenerator.insertIntoSummary(0, "3", "", "Successful", "", "", "");
			moduleStatus = "Successful";
			endTime = System.currentTimeMillis();
			//JobTypeParser.getLogger().info("Module \'Execution Status Check\' completed successfully");
			
		} catch (SQLException e1) {
			JobTypeParser.getExecchecklogger().error("Exception encountered in ExecutionStatusTestAutomator: " + e1);
		} finally {
			Validator.printModuleCompletionPrompt("Execution Status Check", startTime, endTime, moduleStatus);
			try {
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
			}
		}
		
		return null;
	}

}
