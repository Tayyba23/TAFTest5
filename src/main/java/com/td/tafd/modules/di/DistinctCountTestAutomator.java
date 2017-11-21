/**
 * 
 */
package com.td.tafd.modules.di;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.DataSource;
import com.td.tafd.ds.DataSourceFactory;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.exceptions.InvalidInformationException;
import com.td.tafd.exceptions.ObjectNotFoundException;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.DistinctColumnInfo;

/**
 * @author kt186036
 *
 */
public class DistinctCountTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfDCTables;
	private String resultTableName;
	private boolean useDefaultBatch;
	
	/**
	 * @return the tdSource
	 */
	public TeradataDataSource getTdSource() {
		return tdSource;
	}
	
	/**
	 * @return the resultTableName
	 */
	public String getResultTableName() {
		return resultTableName;
	}

	/**
	 * @param resultTableName the resultTableName to set
	 */
	public void setResultTableName(String resultTableName) {
		this.resultTableName = resultTableName;
	}

	/**
	 * @param tdSource the tdSource to set
	 */
	public void setTdSource(TeradataDataSource tdSource) {
		this.tdSource = tdSource;
	}
	
	/**
	 * @return the useDefaultBatch
	 */
	public boolean useDefaultBatch() {
		return useDefaultBatch;
	}

	/**
	 * @param useDefaultBatch the useDefaultBatch to set
	 */
	public void setUseDefaultBatch(boolean useDefaultBatch) {
		this.useDefaultBatch = useDefaultBatch;
	}
	
	public DistinctCountTestAutomator(String resultTable, boolean useDefaultBatch)
	{
		tdSource = new TeradataDataSource();
		numOfDCTables = 0;
		setResultTableName(resultTable);
		setUseDefaultBatch(useDefaultBatch);
	}
	
	/**
	 * Compares distinct values for all tables listed in input parameter file 
	 */
	
	@Override
	public Void call() 
	{
		long startTime = 0;
		boolean executeBatch = false;
		int numOfRecords = Math.min(JobTypeParser.getReader().getSources().size(), JobTypeParser.getReader().getTargets().size());
		String summaryStatus = "Unsuccessful";
		int prevNumOfDCTables = 0;
		int maxTableLimit = 150;
		
		for(int x = 0; x < numOfRecords; ++x) 
		{
			//System.out.println("In distinct count loop. x: " + x);
			JobTypeParser.getDistinctcountlogger().info("In function \'distinctCountModuleManager\', parameter values: \'int x\' = " + x);
			
			executeBatch = (x == (numOfRecords - 1));
			
			String condition = JobTypeParser.getRowCountAndMetrics().get(x).getDistinctValueCount();
			
			//List<String> conditions = JobTypeParser.getReader().readColumnWithHeading("Distinct_Value_Count", JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection"), 1, 0);  // Yes, No flags
			Connection conn = null;
			summaryStatus = "Unsuccessful";
			//startTime = 0;
			
			try {
				if(!condition.trim().equalsIgnoreCase("N") && !condition.trim().equalsIgnoreCase(""))
				{
					if(x == 0)
					{
						JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'DISTINCT COUNT\'				|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
					}
				}

				char sourceType = 'x';
				char targetType = 'x';

				startTime = System.currentTimeMillis();

				String env = JobTypeParser.getRowCountAndMetrics().get(x).getEnvId();
				boolean enterUnsuccessful = false;

				DataSource sDataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(x).getSourceTypeCd());
				DataSource tDataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(x).getTargetTypeCd());

				if(sDataSource != null && tDataSource != null)
				{
					sourceType = sDataSource.getDataSourceType().equals("teradataDataSource") ? 't' : 'f';
					targetType = tDataSource.getDataSourceType().equals("teradataDataSource") ? 't' : 'f';
				}

				if(condition.toUpperCase().equals("Y"))
				{	
					if( (JobTypeParser.getReader().getSources().get(x).trim().equals(".") || JobTypeParser.getReader().getSources().get(x).trim().equals("")) && (JobTypeParser.getReader().getTargets().get(x).trim().equals(".") || JobTypeParser.getReader().getTargets().get(x).trim().equals(""))) {
						continue;
					}

					try {
						conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
					} catch (SQLException e2) {
						JobTypeParser.getDistinctcountlogger().error("Connection Retrieval encountered an exception");
					}

					if(!sDataSource.objectExists(JobTypeParser.getReader().getSources().get(x), conn))	// i.e. target database or target name is missing in input file
					{
						try {
							throw new ObjectNotFoundException(DataIntegrityAutomation.class, new Exception(), "Source object \'" + JobTypeParser.getReader().getSources().get(x) + "\' does not exist. Please check Source's Path and Name columns in row " + (x+1) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
						} catch (ObjectNotFoundException e) {
						}
						enterUnsuccessful = true;
					}

					else if(!tDataSource.objectExists(JobTypeParser.getReader().getTargets().get(x), conn))	// i.e. target database or target name is missing in input file
					{
						try {
							throw new ObjectNotFoundException(DataIntegrityAutomation.class, new Exception(), "Target object \'" + JobTypeParser.getReader().getTargets().get(x) + "\' does not exist. Please check Target's Path and Name columns in row " + (x+1) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
						} catch (ObjectNotFoundException e) {
						}
						enterUnsuccessful = true;
					}
				}

				if(enterUnsuccessful)		// if either source or target is a file, or does not exist, insert unsuccessful into summary table against this record
				{		
					summaryStatus = "Unsuccessful";
					continue;
				}

				//ArrayList<String> distinctObjects = new ArrayList<String>();		// distinct tables from both columns stored into a HashMap so as to avoid accessing the database repeatedly for the same table

				List<DistinctColumnInfo> distinctColumnInfoList1;
				List<DistinctColumnInfo> distinctColumnInfoList2;
				
				HashMap<String, List<DistinctColumnInfo>> distinct_vals = new HashMap<String, List<DistinctColumnInfo>>();

				// filtering on the basis of "Distinct_Value_Count" flag column in Input Parameter File
				/*if(condition.toUpperCase().equals("Y"))
				{
					distinctObjects.add(JobTypeParser.getReader().getSources().get(x));
					distinctObjects.add(JobTypeParser.getReader().getTargets().get(x));
				}*/

				if(!condition.toUpperCase().equals("Y"))				// if 'N' or blank encountered against this Module in input metadata file, insert 'Not Run'
				{
					if(!condition.equalsIgnoreCase("N") && (!JobTypeParser.getReader().getSources().get(x).trim().equals(".") || !JobTypeParser.getReader().getTargets().get(x).trim().equals(".")))
					{
						try {
							throw new InvalidInformationException(JobTypeParser.class, new Exception(), "Invalid command \'" + condition + "\' entered at row \'" + (x+1) + "\' in sheet \'Row Count & Metrics Collection\'. Module \'Distinct Value Count Verification\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(x).trim() + "\' and target \'" + JobTypeParser.getReader().getTargets().get(x).trim() + "\'.");
						} catch (InvalidInformationException e1) {
						}
					}
					summaryStatus = "Not Run";

					JobTypeParser.getDistinctcountlogger().debug("DistinctCountComparison execution time: " + (System.currentTimeMillis() - startTime) + "(ms)");
					continue;
				}

				ArrayList<String> temp_s_cols = new ArrayList<String>();
				ArrayList<String> temp_t_cols = new ArrayList<String>();
				
				QueryGenerator.determineColumnNames(temp_s_cols, temp_t_cols, x, "distinct", JobTypeParser.getRowCountAndMetrics().get(x).getMappingSpecifications());
				
				distinctColumnInfoList1 = sDataSource.getDistinctValueCount(JobTypeParser.getReader().getSources().get(x).trim(), temp_s_cols);	
				distinct_vals.put(JobTypeParser.getReader().getSources().get(x).trim(), distinctColumnInfoList1);
		
				distinctColumnInfoList2 = tDataSource.getDistinctValueCount(JobTypeParser.getReader().getTargets().get(x).trim(), temp_t_cols);
				distinct_vals.put(JobTypeParser.getReader().getTargets().get(x).trim(), distinctColumnInfoList2);

				String temp_s_table = JobTypeParser.getReader().getSources().get(x);
				String temp_t_table = JobTypeParser.getReader().getTargets().get(x);

				env = JobTypeParser.getRowCountAndMetrics().get(x).getEnvId();
				String status = "";

				String insertion_query = "";

				List<DistinctColumnInfo> sourceList = distinct_vals.get(temp_s_table);
				List<DistinctColumnInfo> targetList = distinct_vals.get(temp_t_table);

				/*System.out.println("Source list: ");
				
				for(DistinctColumnInfo col : sourceList)
				{
					System.out.print(col.getName() + ", ");
				}
				
				System.out.println();

				System.out.println("Target list: ");
				
				for(DistinctColumnInfo col : targetList)
				{
					System.out.print(col.getName() + ", ");
				}
				
				System.out.println();*/
				
				/*if(!JobTypeParser.re_order_cols(sourceList, targetList, temp_s_table, temp_t_table))
				{
					JobTypeParser.getLogger().debug("Module \'Distinct Count\' cannot be executed");
					return null;
				}*/

				if(sourceList == null)
					JobTypeParser.getLogger().error("Error: Source object \'" + JobTypeParser.getReader().getSources().get(x) + "\' and/or Target object \'" + JobTypeParser.getReader().getTargets().get(x) + "\' does not exist - Module \'Distinct Value Check\' cannot be executed");
				else
				{
					for(int k=0; k< sourceList.size(); ++k)
					{
						insertion_query = "Insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + "." + getResultTableName() + " (test_status, execution_timestamp, source_name, target_name, source_path, target_path, source_column_name, target_column_name, source_distinct_count, target_distinct_count, business_date, test_cycle_id, test_type_cd, user_id, stream_id, sub_stream_id, env_id, source_type_cd, target_type_cd, script_text) VALUES (";

						if(sourceList.get(k).getCount() == targetList.get(k).getCount())		// 'status' set to 'passed' if source and target row_count match 
							status = "Passed";
						else
							status = "Failed";

						String script_text_query = "";

						if(sourceType == 't' && targetType == 't')
							script_text_query = "SELECT (SELECT COUNT(*) x FROM (SELECT " + sourceList.get(k).getName() + " FROM " + temp_s_table + " GROUP BY " + sourceList.get(k).getName() + ") A) as source_distinct_count, (SELECT COUNT(*) x FROM (SELECT " + targetList.get(k).getName() + " FROM " + temp_t_table + " GROUP BY " + targetList.get(k).getName() + ") B) as target_distinct_count;";

						else
							script_text_query = "Source and/or target is a file - query cannot be generated";

						insertion_query += "\'" + status + "\', \'" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "\', \'" + ((sourceType == 'f') ? new File(temp_s_table).getName() : temp_s_table.split("\\.")[1]) + "\', \'" + ((targetType == 'f') ? new File(temp_t_table).getName() : temp_t_table.split("\\.")[1]) + "\', \'" + ((sourceType == 'f') ? new File(temp_s_table).getParent() : temp_s_table.split("\\.")[0]) + "\', \'" + ((targetType == 'f') ? new File(temp_t_table).getParent() : temp_t_table.split("\\.")[0]) + "\', \'" + sourceList.get(k).getName() + "\', \'" + targetList.get(k).getName() + "\', " + sourceList.get(k).getCount() + ", " + targetList.get(k).getCount() + ", \'" + ConfigurationManager.getInstance().getUserConfig().getBusinessDate() + "\', \'" + env + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getDistinctCount()) + ", " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getUserId()) + ", " + JobTypeParser.getRowCountAndMetrics().get(x).getStreamId() + ", " + JobTypeParser.getRowCountAndMetrics().get(x).getSubStreamId() + ", \'" + env + "\', " + JobTypeParser.getRowCountAndMetrics().get(x).getSourceTypeCd() + ", " + JobTypeParser.getRowCountAndMetrics().get(x).getTargetTypeCd() + ", \'" + script_text_query + "\');";

						if(useDefaultBatch())
						{
							if((numOfDCTables - prevNumOfDCTables) > maxTableLimit)
							{
								JobTypeParser.executeBatchStatements(JobTypeParser.getDistinctCountBatchStatements());
								prevNumOfDCTables = numOfDCTables;
							}
							
							JobTypeParser.addToBatch(JobTypeParser.getDistinctCountBatchStatements(), insertion_query);
						}
						else
						{
							if((numOfDCTables - prevNumOfDCTables) > maxTableLimit)
							{
								JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
								prevNumOfDCTables = numOfDCTables;
							}
							
							JobTypeParser.addToBatch(JobTypeParser.getScriptExecTestBatchStatements(), insertion_query);
						}
						insertion_query = "";
					}

					summaryStatus = "Successful";
					++numOfDCTables;
				}

			} catch(SQLException e) {
				//e.printStackTrace();
				if(e.getSQLState().equals("HY000"))
				{
					try {
						throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".summary_tbl\' or object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Distinct_Value_Count_Rslt\'");
					} catch (AccessViolationException e1) {
					}
				}
				
				else if(e.getSQLState().equals("28000"))
					JobTypeParser.getLogger().error("Error: Invalid Username and Password. Please check username and password in file \"config.properties\"");
				
				else if(e.getSQLState().equals("23000"))
					JobTypeParser.getDistinctcountlogger().debug("Error: SQL Error - Duplicate Primary key - cannot insert record into Distinct_Value_Count_Rslt");
				
				else
				{
					String [] msg = e.getMessage().split("] ");
					JobTypeParser.getLogger().error("Error: SQL Error - " + msg[msg.length-1]);
				}
			} catch(Exception e) {
				//e.printStackTrace();
				JobTypeParser.getDistinctcountlogger().error("Exception encountered in Module Distinct Value Check");
				JobTypeParser.getDistinctcountlogger().error("Exception is: ", e);
			} finally {		
				//JobTypeParser.getLogger().debug("Yes, finally block executes. iteration: " + x);
				
				if(!summaryStatus.equals("Not Run"))
					QueryGenerator.insertIntoSummary(x, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getDistinctCount()), "", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(x).getDistinctValueCount(), JobTypeParser.getRowCountAndMetrics().get(x).getEnvId()); 	// insert unsuccessful status into summary table
				else
					QueryGenerator.insertIntoSummary(x, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getDistinctCount()), "Distinct_Value_Count", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(x).getDistinctValueCount(), JobTypeParser.getRowCountAndMetrics().get(x).getEnvId());
				
				if(executeBatch)
				{	
					//System.out.println("In distinct count. Executing batch");
					try {
						if(useDefaultBatch())
							JobTypeParser.executeBatchStatements(JobTypeParser.getDistinctCountBatchStatements());
						else
							JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
					} catch (SQLException e) {
						//e.printStackTrace();
						JobTypeParser.getDistinctcountlogger().error("Error encountered while inserting into Distinct Count Result Table: " + e);
					}
				}
				
				/*if(x == (JobTypeParser.getNumOfRecords() - 1) && numOfDCTables > 0)
					JobTypeParser.getLogger().info("Module \'Distinct Count\' completed successfully for " + numOfDCTables + " inputs");*/
				
				if(conn != null)
					try {
						conn.close();
					} catch (SQLException e) {
						JobTypeParser.getDistinctcountlogger().error("Connection Closing encountered an exception");
					}
			}
					
		}
		
		long endTime = System.currentTimeMillis();
		JobTypeParser.getDistinctcountlogger().debug("DistinctCountComparison execution time: " + (endTime - startTime) + "(ms)");
		Validator.printModuleCompletionPrompt("Distinct Count", startTime, endTime, summaryStatus, numOfDCTables);
		
		//System.out.println("Exiting from distinct count");
		return null;
	}
}
