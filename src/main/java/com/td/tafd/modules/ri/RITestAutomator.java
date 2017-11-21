/**
 * 
 */
package com.td.tafd.modules.ri;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.exceptions.InvalidInformationException;
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.exceptions.ObjectNotFoundException;
import com.td.tafd.modules.dd.DuplicateRecordTestAutomator;
import com.td.tafd.validation.Validator;

/**
 * @author kt186036
 *
 */
public class RITestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfRITables;
	
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
	
	public RITestAutomator()
	{
		tdSource = new TeradataDataSource();
		numOfRITables = 0;
	}

	@Override
	public Void call() throws Exception 
	{
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'RI VERIFICATION\'			|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
		
		TeradataDataSource tdSource = new TeradataDataSource();

		int i=0;	
		int violation_count = 0;
		String summaryStatus = "Unsuccessful";
		
		long startTime = System.currentTimeMillis();
		long endTime = 0;
		
		for(i=0; i<JobTypeParser.getRiVerification().size(); ++i)
		{
			//System.out.println("i = " + i);
			
			if(JobTypeParser.getRiVerification().get(i).getParentDbName().trim() == "" || JobTypeParser.getRiVerification().get(i).getParentDbName().trim().equals("") || JobTypeParser.getRiVerification().get(i).getParentDbName().equals(null))
				continue;
			
			if(JobTypeParser.getReader().getObjectType(JobTypeParser.getRiVerification().get(i).getParentDbName()) == 'f' && JobTypeParser.getReader().getObjectType(JobTypeParser.getRiVerification().get(i).getChildDbName()) == 't')
			{
				JobTypeParser.getLogger().error("Error: Parent object \'" + JobTypeParser.getRiVerification().get(i).getParentTableName() + "\' is a file - Module \'RI Verification cannot be executed\'");
				continue;
			}
			
			else if(JobTypeParser.getReader().getObjectType(JobTypeParser.getRiVerification().get(i).getParentDbName()) == 't' && JobTypeParser.getReader().getObjectType(JobTypeParser.getRiVerification().get(i).getChildDbName()) == 'f')
			{
				JobTypeParser.getLogger().error("Error: Child object \'" + JobTypeParser.getRiVerification().get(i).getChildTableName() + "\' is a file - Module \'RI Verification cannot be executed\'");
				continue;
			}
			
			else if(JobTypeParser.getReader().getObjectType(JobTypeParser.getRiVerification().get(i).getParentDbName()) == 'f' && JobTypeParser.getReader().getObjectType(JobTypeParser.getRiVerification().get(i).getChildDbName()) == 't')
			{
				JobTypeParser.getLogger().error("Error: Parent and Child objects \'" + JobTypeParser.getRiVerification().get(i).getParentDbName() + "\' and \'" + JobTypeParser.getRiVerification().get(i).getChildDbName() + "\' are files - Module \'RI Verification cannot be executed\'");
				continue;
			}
			
			String env = JobTypeParser.getRiVerification().get(i).getEnvId();	
			
			PreparedStatement ps;
			Connection conn = null;
			
			summaryStatus = "Unsuccessful";
			
			try
			{
				//System.out.println("Getting connection");
				conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
				
				if(!JobTypeParser.getRiVerification().get(i).getParentTableName().equals(""))
				{
					boolean parent_exists = tdSource.objectExists(JobTypeParser.getRiVerification().get(i).getParentDbName()+"."+JobTypeParser.getRiVerification().get(i).getParentTableName(), conn);
					boolean child_exists = tdSource.objectExists(JobTypeParser.getRiVerification().get(i).getChildDbName()+"."+JobTypeParser.getRiVerification().get(i).getChildTableName(), conn);
					
					//System.out.println("parent_exists: " + parent_exists + ", child_exists: " + child_exists);
					
					if(conn != null)
						conn.close();
					
					if(parent_exists && child_exists)
					{	
						conn= tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
						if(!tdSource.columnExists(JobTypeParser.getRiVerification().get(i).getParentDbName(), JobTypeParser.getRiVerification().get(i).getParentTableName(), JobTypeParser.getRiVerification().get(i).getPkColumns(), conn))
						{
							try {
								throw new ObjectNotFoundException(DuplicateRecordTestAutomator.class, new Exception(), "PK Column \'" + JobTypeParser.getRiVerification().get(i).getPkColumns() + "\' does not exist in Parent object \'" + JobTypeParser.getRiVerification().get(i).getParentDbName() + "." + JobTypeParser.getRiVerification().get(i).getParentTableName() + "\' - Module \'RI Verification\' cannot be executed");
							} catch (ObjectNotFoundException e) {
							}
							continue;
						}
						
						if(conn != null)
							conn.close();
						
						conn= tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
						if(!tdSource.columnExists(JobTypeParser.getRiVerification().get(i).getChildDbName(), JobTypeParser.getRiVerification().get(i).getChildTableName(), JobTypeParser.getRiVerification().get(i).getFkColumns(), conn))
						{
							try {
								throw new ObjectNotFoundException(DuplicateRecordTestAutomator.class, new Exception(), "FK Column \'" + JobTypeParser.getRiVerification().get(i).getFkColumns() + "\' does not exist in Child object \'" + JobTypeParser.getRiVerification().get(i).getChildDbName() + "." + JobTypeParser.getRiVerification().get(i).getChildTableName() + "\' - Module \'RI Verification\' cannot be executed");
							} catch (ObjectNotFoundException e) {
							}
							continue;
						}
						
						if(conn != null)
							conn.close();
						
						if(!JobTypeParser.getRiVerification().get(i).getParentDbName().equals(""))
						{
							if(conn != null)
								conn.close();
							conn= tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
							String query = "SELECT count (*) RI_Violation_Count FROM ( Select A." + JobTypeParser.getRiVerification().get(i).getFkColumns() + " from " + JobTypeParser.getRiVerification().get(i).getChildDbName() + "." + JobTypeParser.getRiVerification().get(i).getChildTableName() + " A LEFT OUTER JOIN " + JobTypeParser.getRiVerification().get(i).getParentDbName() + "." + JobTypeParser.getRiVerification().get(i).getParentTableName() + " B ON COALESCE(A." + JobTypeParser.getRiVerification().get(i).getFkColumns() + ", \'\') = COALESCE(B." + JobTypeParser.getRiVerification().get(i).getPkColumns() + ", \'\') WHERE B." + JobTypeParser.getRiVerification().get(i).getPkColumns() + " IS NULL GROUP BY A." + JobTypeParser.getRiVerification().get(i).getFkColumns() + ") X;";	// query returning number of RI violations
							
							//System.out.println("RI query: " + query);
							//long queryStartTime = System.currentTimeMillis();
							
							ps = conn.prepareStatement(query);
							ResultSet rs = ps.executeQuery();

							//JobTypeParser.getLogger().info("Query execution (RI Verification) completed in " + (System.currentTimeMillis() - queryStartTime) + " (milliseconds)");
							
							String vc = "";

							while(rs.next())
							{
								vc = rs.getString(1);
								violation_count = Integer.parseInt(vc);
							}

							String insertion_query = "";
							String status = "";

							if(violation_count > 0)
								status = "Failed";
							else
								status = "Passed";
							
							insertion_query = "Insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Ri_Rslt (test_status, execution_timestamp, parent_db, child_db, parent_table_name, child_table_name, primary_key, foreign_key, no_of_violations, business_date, test_cycle_id, test_type_cd, user_id, stream_id, sub_stream_id, env_id, script_text) VALUES (";
							insertion_query += "\'" + status + "\', \'" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "\', \'" + JobTypeParser.getRiVerification().get(i).getParentDbName() + "\', \'" + JobTypeParser.getRiVerification().get(i).getChildDbName() + "\', \'" + JobTypeParser.getRiVerification().get(i).getParentTableName() + "\', \'" + JobTypeParser.getRiVerification().get(i).getChildTableName() + "\', \'" + JobTypeParser.getRiVerification().get(i).getPkColumns() + "\', \'" + JobTypeParser.getRiVerification().get(i).getFkColumns() + "\', " + violation_count + ", \'" + ConfigurationManager.getInstance().getUserConfig().getBusinessDate() + "\', \'" + env + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getRiVer()) + ", " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getUserId()) + ", " + JobTypeParser.getRiVerification().get(i).getStreamId() + ", " + JobTypeParser.getRiVerification().get(i).getSubStreamId() + ", \'" + env + "\', \'" + query.replace("'", "''") + "\');";

							//System.out.println("insertion_query: " + insertion_query);
							//System.out.println("Adding to batch");
							
							JobTypeParser.addToBatch(JobTypeParser.getRiBatchStatements(), insertion_query);

							//System.out.println("Added");
							
							insertion_query = "";
							
							env = JobTypeParser.getRiVerification().get(i).getEnvId();
							
							summaryStatus = "Successful";
							
							numOfRITables += 1;
						}
					}

					else
					{
						String errorMsg = "";
						
						if(parent_exists && !child_exists)
						{
							if(JobTypeParser.getRiVerification().get(i).getChildTableName().equals(""))
							{
								try {
									throw new MissingInformationException(JobTypeParser.class, new Exception(), "Child object name and/or database name not found in input file. Please check Child's database and Name columns in row " + (i+1) + " of sheet \'RI Verification\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
								} catch (MissingInformationException e) {
								}
							}
					
							else
								errorMsg = "Child object \'" + JobTypeParser.getRiVerification().get(i).getChildDbName() + "." + JobTypeParser.getRiVerification().get(i).getChildTableName() + "\' does not exist - Module \'RI Verification\' cannot be executed";
						}
						
						else if(!parent_exists && child_exists)
						{
							if(JobTypeParser.getRiVerification().get(i).getParentTableName().equals(""))
							{
								try {
									throw new MissingInformationException(JobTypeParser.class, new Exception(), "parent object name and/or database name not found in input file. Please check Parent's database and Name columns in row " + (i+1) + " of sheet \'RI Verification\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
								} catch (MissingInformationException e) {
								}
								
							}
							else
								errorMsg = "Parent object \'" + JobTypeParser.getRiVerification().get(i).getParentDbName() + "." + JobTypeParser.getRiVerification().get(i).getParentTableName() + "\' does not exist - Module \'RI Verification\' cannot be executed";
						}
						
						else if(!parent_exists && !child_exists)
							errorMsg = "Error: Parent object \'" + JobTypeParser.getRiVerification().get(i).getParentDbName() + "." + JobTypeParser.getRiVerification().get(i).getParentTableName() + "\' and Child object \'" + JobTypeParser.getRiVerification().get(i).getChildDbName() + "." + JobTypeParser.getRiVerification().get(i).getChildTableName() + "\' does not exist - Module \'RI Verification\' cannot be executed";
						
						if(!errorMsg.equals(""))
						{
							try {
								throw new ObjectNotFoundException(RITestAutomator.class, new Exception(), errorMsg);
							} catch (ObjectNotFoundException e) {
							}
						}
					}
				}
				
			} catch(SQLException e) {
				//e.printStackTrace();
				if(e.getSQLState().equals("HY000")) {
					try {
						throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".summary_tbl\' or object \'" + JobTypeParser.getRiVerification().get(i).getParentDbName() + "." + JobTypeParser.getRiVerification().get(i).getParentTableName() + "\' or object \'" + JobTypeParser.getRiVerification().get(i).getChildDbName() + "." + JobTypeParser.getRiVerification().get(i).getChildTableName() + "\' or object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Ri_Rslt\'");
					} catch (AccessViolationException e1) {
					}
				}
				else {
					try {
						throw new InvalidInformationException(JobTypeParser.class, e, "Ri_Rslt");
					} catch (InvalidInformationException e1) {
					}
				}
			} catch(Exception e) {
				//e.printStackTrace();
				JobTypeParser.getRichecklogger().error("Exception encountered in Module Referential Integrity Check");
				JobTypeParser.getRichecklogger().error("Exception is: ", e);
			} finally {
				try {
					QueryGenerator.insertIntoSummary(i, ConfigurationManager.getInstance().getAppConfig().getRiVer(), "", summaryStatus, JobTypeParser.getRiVerification().get(i).getParentDbName()+"."+JobTypeParser.getRiVerification().get(i).getParentTableName(), JobTypeParser.getRiVerification().get(i).getChildDbName()+"."+JobTypeParser.getRiVerification().get(i).getChildTableName(), JobTypeParser.getRiVerification().get(i).getEnvId());
					if(conn != null)
						conn.close();
				} catch (SQLException e) {
					JobTypeParser.getRichecklogger().error("Insertion into summary encountered an exception");
				} 				
			}
		}
		
		//System.out.println("outside loop");
		
		try {
			//System.out.println("Executing batch");
			JobTypeParser.executeBatchStatements(JobTypeParser.getRiBatchStatements());
			//System.out.println("Batch executed");
			
			/*if(numOfRITables > 0)
			{
				numOfRITables = 0;
			}*/
			
		} catch (SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getRichecklogger().error("Exception encountered while executing RI batch inserts");
			JobTypeParser.getRichecklogger().error("Exception is: ", e);
		} catch(Exception e) {
			//e.printStackTrace();
			JobTypeParser.getRichecklogger().error("Exception encountered in Module Referential Integrity Check");
			JobTypeParser.getRichecklogger().error("Exception is: ", e);
		} finally {
			endTime = System.currentTimeMillis();
			Validator.printModuleCompletionPrompt("RI Verification", startTime, endTime, summaryStatus, numOfRITables);
		}
		
		//System.out.println("Exiting from RI test");
		return null;
	}
}
