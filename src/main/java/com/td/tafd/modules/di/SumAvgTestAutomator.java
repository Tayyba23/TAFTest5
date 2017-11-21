/**
 * 
 */
package com.td.tafd.modules.di;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
import com.td.tafd.vo.MetricsRecord;
import com.td.tafd.vo.MetricsRecordUtil;

/**
 * @author kt186036
 *
 */
public class SumAvgTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfSATables;
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
	
	public SumAvgTestAutomator(String resultTable, boolean useDefaultBatch)
	{
		tdSource = new TeradataDataSource();
		numOfSATables = 0;
		setResultTableName(resultTable);
		setUseDefaultBatch(useDefaultBatch);
	}

	@Override
	public Void call() throws Exception 
	{
		boolean executeBatch = false;
		int numOfRecords = Math.min(JobTypeParser.getReader().getSources().size(), JobTypeParser.getReader().getTargets().size());
		String summaryStatus = "Unsuccessful";
		long startTime = System.currentTimeMillis();
		int prevNumOfSATables = 0;
		int maxTableLimit = 150;
		
		for(int i = 0; i < numOfRecords; ++i) 
		{	
			JobTypeParser.getSumavglogger().info("In function \'sumAvgModuleManager\', parameter values: \'int i\' = " + i);
			executeBatch = (i == (numOfRecords - 1));
			
			startTime = System.currentTimeMillis();
			DataSource dataSource;	// reference of DataSource interface - this will hold the appropriate DataSource (File, Teradata objects etc.) based on the source or target object being analyzed
			
			String condition = JobTypeParser.getRowCountAndMetrics().get(i).getSumAvgValue();
			//List<String> conditions = JobTypeParser.getReader().readColumnWithHeading("Sum_Avg", JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection"), 1, 0);
			
			if(!condition.equalsIgnoreCase("N") && i==0)
			{
				JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'SUM / AVG\'				|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
			}
			
			/*env_col_id = JobTypeParser.getReader().getColId("Env_Id", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			stream_col_id = JobTypeParser.getReader().getColId("Stream_Id", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			sub_stream_col_id = JobTypeParser.getReader().getColId("Sub_Stream_Id", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			
			int source_type_col_id = JobTypeParser.getReader().getColId("Source_Type_Cd", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			int target_type_col_id = JobTypeParser.getReader().getColId("Target_Type_Cd", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());*/
			
			boolean s_obj_exists = false;
			boolean t_obj_exists = false;
			summaryStatus = "Unsuccessful";
			
			try {
				if(condition.equals("Y"))		// check if the test for Sum_Avg needs to be performed or not
				{
					String env = "";

					String [] sourceInfo = new String [3];
					String [] targetInfo = new String [3];
					
					sourceInfo = Validator.getInstance().getObjectInformation(JobTypeParser.getReader().getObjectType(JobTypeParser.getReader().getSources().get(i)), "Source", JobTypeParser.getReader().getSources().get(i), i);
					targetInfo = Validator.getInstance().getObjectInformation(JobTypeParser.getReader().getObjectType(JobTypeParser.getReader().getTargets().get(i)), "Target", JobTypeParser.getReader().getTargets().get(i), i);
					
					if(sourceInfo[0] == null || targetInfo[0] == null)
						continue;

					HashMap<String, Double[]> sourceSumAvg = new HashMap<String, Double[]>();
					HashMap<String, Double[]> targetSumAvg = new HashMap<String, Double[]>();

					ArrayList<String> temp_s_cols = new ArrayList<String>();
					ArrayList<String> temp_t_cols = new ArrayList<String>();
					
					QueryGenerator.determineColumnNames(temp_s_cols, temp_t_cols, i, "sum_avg", JobTypeParser.getRowCountAndMetrics().get(i).getMappingSpecifications());
					
					/*System.out.println("temp_s_cols: " + temp_s_cols);
					System.out.println("temp_t_cols: " + temp_t_cols);*/
					
					dataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(i).getSourceTypeCd());
					
					s_obj_exists = dataSource.getSumAvg(JobTypeParser.getReader().getSources().get(i), i, temp_s_cols);
			
					/*if(dataSource.getDataSourceType().equals("fileDataSource"))
					{
						temp_s_cols.clear();
						temp_s_cols.addAll(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), JobTypeParser.getReader().getSources().get(i), true));
					}*/
					
					for(String key : temp_s_cols)
					{ 
						//System.out.println("key being checked in metricsRecords is: " + key);
						MetricsRecord metRecord = MetricsRecordUtil.getRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(JobTypeParser.getReader().getSources().get(i)), key.toString());
						/*if(col_map.containsKey(key))
						{*/
							sourceSumAvg.put(key.toString(), new Double [] {metRecord.getSum(), metRecord.getAverage()});
						//}
					}
					
					boolean useSourceColumns = dataSource.getDataSourceType().equals("fileDataSource");
					
					dataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(i).getTargetTypeCd());
					
					if(useSourceColumns && dataSource.getDataSourceType().equals("teradataDataSource") && JobTypeParser.getRowCountAndMetrics().get(i).getMappingSpecifications().equalsIgnoreCase("NA"))					// if source is a file and target is a table, use the columns of the source for sum, only if the mapping specification is NA
						t_obj_exists = dataSource.getSumAvg(JobTypeParser.getReader().getTargets().get(i), i, temp_s_cols);
					else
						t_obj_exists = dataSource.getSumAvg(JobTypeParser.getReader().getTargets().get(i), i, temp_t_cols);

					if((temp_s_cols.isEmpty() && temp_t_cols.isEmpty()) || temp_s_cols.contains("no_numeric_cols") || temp_s_cols.contains("table_is_empty") || ( temp_t_cols!=null && !temp_t_cols.isEmpty() && (temp_t_cols.contains("no_numeric_cols") || temp_t_cols.contains("table_is_empty"))))
					{
						JobTypeParser.getLogger().error("No Numeric Columns found in Source and/or Target, or Source and/or target table is empty. Module \'Sum/Avg Reconciliation\' cannot be executed for source/target pair : \'" + JobTypeParser.getReader().getSources().get(i) + "/" + JobTypeParser.getReader().getTargets().get(i) + "\'");
						continue;
					}
					
					/*temp_t_cols.clear();
					temp_t_cols.addAll(temp_s_cols);*/
					
					for(String key : temp_t_cols)
					{ 
						MetricsRecord metRecord = MetricsRecordUtil.getRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(JobTypeParser.getReader().getTargets().get(i)), key.toString());
						/*if(col_map.containsKey(key))
						{*/
							targetSumAvg.put(key.toString(), new Double [] {metRecord.getSum(), metRecord.getAverage()});
						//}
					}
					
					/*System.out.println("SOURCE");
					for(String key : sourceSumAvg.keySet())
					{
						System.out.println("key: " + key + ", sourceSumAvg.get(key)[0]: " + sourceSumAvg.get(key)[0] + ", sourceSumAvg.get(key)[1]: " + sourceSumAvg.get(key)[1]);
					}
					
					System.out.println("TARGET");
					for(String key : targetSumAvg.keySet())
					{
						System.out.println("key: " + key + ", targetSumAvg.get(key)[0]: " + targetSumAvg.get(key)[0] + ", targetSumAvg.get(key)[1]: " + targetSumAvg.get(key)[1]);
					}*/
					
					if(s_obj_exists && t_obj_exists)		// if both source and target objects exist
					{	
						if(JobTypeParser.getRowCountAndMetrics().get(i).getMappingSpecifications().equalsIgnoreCase("NA") && !JobTypeParser.re_order_cols(temp_s_cols, temp_t_cols, JobTypeParser.getReader().getSources().get(i), JobTypeParser.getReader().getTargets().get(i)))		// if there is a mismatch in source and target columns, return right away
						{
							JobTypeParser.getLogger().info("Module \'Sum/Avg Value\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(i) + "\' and target \'" + JobTypeParser.getReader().getTargets().get(i) + "\'");
							JobTypeParser.getSumavglogger().debug("SumAvgModule execution time: " + (System.currentTimeMillis() - startTime) + "(ms)");
							continue;
						}
						
						String insertion_query1 = "Insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + "." + getResultTableName() + " (test_status, execution_timestamp, source_name, target_name, source_path, target_path, source_column_name, target_column_name, source_sum, target_sum, business_date, test_cycle_id, test_type_cd, user_id, stream_id, sub_stream_id, source_avg, target_avg, env_id, source_type_cd, target_type_cd, script_text) VALUES (";
						
						for(int j=0; j< Math.min(temp_s_cols.size(), temp_t_cols.size()); ++j)
						{	
							env = JobTypeParser.getRowCountAndMetrics().get(i).getEnvId();
							String insertion_query2 = "";
							String status = "";

							if( (sourceSumAvg.get(temp_s_cols.get(j))[0].equals(targetSumAvg.get(temp_t_cols.get(j))[0])) && (sourceSumAvg.get(temp_s_cols.get(j))[1].equals(targetSumAvg.get(temp_t_cols.get(j))[1])) )
								status = "Passed";
							else
								status = "Failed";

							String script_text_query = "";

							if(sourceInfo[0].equals(".") && targetInfo[0].equals("."))
								script_text_query = "Select sum(cast(s." + temp_s_cols.get(j) + " as BIGINT)) as source_sum, sum(cast(t." + temp_t_cols.get(j) + " as BIGINT)) as target_sum, avg(cast(s." + temp_s_cols.get(j) + " as BIGINT)) as source_avg, avg(cast(t." + temp_t_cols.get(j) + " as BIGINT)) as target_avg from " + JobTypeParser.getReader().getSources().get(i) + " s, " + JobTypeParser.getReader().getTargets().get(i) + " t;";
							else 
								script_text_query = "Source and/or Target is a file - query cannot be generated";
							
							insertion_query2 = insertion_query1 + "\'" + status + "\', \'"	+ new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "\', \'" + sourceInfo[2] + "\', \'" + targetInfo[2] + "\', \'" + sourceInfo[1] + "\', \'" + targetInfo[1] + "\', \'" 
							+ temp_s_cols.get(j) + "\', \'" 
									+ temp_t_cols.get(j) + "\', \'" 
							+ sourceSumAvg.get(temp_s_cols.get(j))[0].floatValue() + "\', \'" 
									+ targetSumAvg.get(temp_t_cols.get(j))[0].floatValue() 
									+ "\', \'" + ConfigurationManager.getInstance().getUserConfig().getBusinessDate() + "\', \'" + env + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getSumAvg()) + ", " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getUserId()) + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getStreamId() + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getSubStreamId() + ", \'" + sourceSumAvg.get(temp_s_cols.get(j))[1].floatValue() + "\', \'" + targetSumAvg.get(temp_t_cols.get(j))[1].floatValue() + "\', \'" + env + "\', " + JobTypeParser.getRowCountAndMetrics().get(i).getSourceTypeCd() + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getTargetTypeCd() + ", \'" + script_text_query + "\');";						
							
							if(useDefaultBatch())
							{
								if((numOfSATables - prevNumOfSATables) > maxTableLimit)
								{
									JobTypeParser.executeBatchStatements(JobTypeParser.getSumAvgBatchStatements());
									prevNumOfSATables = numOfSATables;
								}
								
								JobTypeParser.addToBatch(JobTypeParser.getSumAvgBatchStatements(), insertion_query2);
							}
							else
							{
								if((numOfSATables - prevNumOfSATables) > maxTableLimit)
								{
									JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
									prevNumOfSATables = numOfSATables;
								}
								
								JobTypeParser.addToBatch(JobTypeParser.getScriptExecTestBatchStatements(), insertion_query2);
							}
							insertion_query2 = "";
						}
						
						summaryStatus = "Successful";
						++numOfSATables;
					}

					else		// else, source and/or target objects do not exist, so log this information for them
					{
						try {
							throw new ObjectNotFoundException(s_obj_exists, t_obj_exists, "Sum Value", i);
						} catch (ObjectNotFoundException e) {
						}
					}

				}

				else if(!condition.equals("Y") && !JobTypeParser.getReader().getSources().get(i).trim().equals(".") && !JobTypeParser.getReader().getTargets().get(i).trim().equals(".") && !JobTypeParser.getReader().getSources().get(i).trim().equals("\\") && !JobTypeParser.getReader().getTargets().get(i).trim().equals("\\"))		// if 'N' or blank encountered against this Module in input metadata file, insert 'Not Run'
				{
					if(!condition.equalsIgnoreCase("N"))
					{
						try {
							throw new InvalidInformationException(JobTypeParser.class, new Exception(), "Invalid command \'" + condition + "\' entered at row \'" + (i+2) + "\' in sheet \'Row Count & Metrics Collection\'. Module \'Sum Value Check\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(i).trim() + "\' and target \'" + JobTypeParser.getReader().getTargets().get(i).trim() + "\'.");
						} catch (InvalidInformationException e1) {
						}
					}
					summaryStatus = "Not Run";
				}
			} catch(SQLException e) {
				if(e.getSQLState().equals("HY000"))
				{
					try {
						throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".summary_tbl\' or object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Sum_Avg_Recon_Rslt\'");
					} catch (AccessViolationException e1) {
					}
				}
				else
				{
					try {
						throw new InvalidInformationException(JobTypeParser.class, e, "Sum_Avg_Recon_Rslt");
					} catch (InvalidInformationException e1) {
					}
				}
			} catch(Exception e) {
				//e.printStackTrace();
				JobTypeParser.getSumavglogger().error("Exception encountered in Module Sum / Avg Reconciliation");
				JobTypeParser.getSumavglogger().error("Exception is: ", e);
			} finally {
				if(!summaryStatus.equals("Not Run"))
					QueryGenerator.insertIntoSummary(i, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getSumAvg()),"", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(i).getSumAvgValue(), JobTypeParser.getRowCountAndMetrics().get(i).getEnvId()); 	// insert unsuccessful status into summary table
				else
					QueryGenerator.insertIntoSummary(i, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getSumAvg()), "Sum_Avg", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(i).getSumAvgValue(), JobTypeParser.getRowCountAndMetrics().get(i).getEnvId());
				
				if(executeBatch)
				{	
					try {
						if(useDefaultBatch())
							JobTypeParser.executeBatchStatements(JobTypeParser.getSumAvgBatchStatements());
						else
							JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
					} catch (SQLException e) {
						//e.printStackTrace();
						JobTypeParser.getSumavglogger().error("Error encountered while inserting into Sum/Avg Result Table");
					}
				}
				
				/*if(i == (JobTypeParser.getNumOfRecords() - 1) && numOfSATables > 0)
					JobTypeParser.getLogger().info("Module \'Sum / Avg Reconciliation\' completed successfully for " + numOfSATables + " inputs");*/
			}
			
			
		}
		
		long endTime = System.currentTimeMillis();
		JobTypeParser.getSumavglogger().debug("SumAvgModule execution time: " + (System.currentTimeMillis() - startTime) + "(ms)");
		Validator.printModuleCompletionPrompt("Sum / Avg Reconciliation", startTime, endTime, summaryStatus, numOfSATables);
		
		//System.out.println("Exiting from sum/avg");
		return null;
	}
}
