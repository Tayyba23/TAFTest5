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

import org.apache.commons.lang.StringUtils;

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
public class MinMaxTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfMMTables;
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
	
	public MinMaxTestAutomator(String resultTable, boolean useDefaultBatch)
	{
		tdSource = new TeradataDataSource();
		numOfMMTables = 0;
		setResultTableName(resultTable);
		setUseDefaultBatch(useDefaultBatch);
	}

	@Override
	public Void call() throws Exception 
	{
		boolean executeBatch = false;
		int numOfRecords = Math.min(JobTypeParser.getReader().getSources().size(), JobTypeParser.getReader().getTargets().size());
		String summaryStatus = "Unsuccessful";
		long startTime = 0;
		int prevNumOfMMTables = 0;
		int maxTableLimit = 150;
		
		for(int i = 0; i < numOfRecords; ++i) 
		{	
			JobTypeParser.getMinmaxlogger().info("In function \'MinMaxModuleManager\', parameter values: \'int i\' = " + i);
			executeBatch = (i == (numOfRecords - 1));
			
			startTime = System.currentTimeMillis();
			String condition = JobTypeParser.getRowCountAndMetrics().get(i).getMinMaxValue();
			
			if(!condition.equalsIgnoreCase("N") && i==0)
			{
				JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'MIN / MAX\'				|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
			}
			
			boolean s_obj_exists = false;
			boolean t_obj_exists = false;
			summaryStatus = "Unsuccessful";
			
			DataSource dataSource;
			try {
				if(condition.equals("Y"))		// check if the test for Min_Max needs to be performed or not
				{
					String env = "";

					String [] sourceInfo = new String [3];
					String [] targetInfo = new String [3];
					
					sourceInfo = Validator.getInstance().getObjectInformation(JobTypeParser.getReader().getObjectType(JobTypeParser.getReader().getSources().get(i)), "Source", JobTypeParser.getReader().getSources().get(i), i);
					targetInfo = Validator.getInstance().getObjectInformation(JobTypeParser.getReader().getObjectType(JobTypeParser.getReader().getTargets().get(i)), "Target", JobTypeParser.getReader().getTargets().get(i), i);
					
					if(sourceInfo[0] == null || targetInfo[0] == null)
						continue;

					HashMap<String, String[]> sourceMinMax = new HashMap<String, String[]>();
					HashMap<String, String[]> targetMinMax = new HashMap<String, String[]>();

					ArrayList<String> temp_s_cols = new ArrayList<String>();
					ArrayList<String> temp_t_cols = new ArrayList<String>();
					
					QueryGenerator.determineColumnNames(temp_s_cols, temp_t_cols, i, "min_max", JobTypeParser.getRowCountAndMetrics().get(i).getMappingSpecifications());
					
					dataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(i).getSourceTypeCd());
					s_obj_exists = dataSource.getMinMax(JobTypeParser.getReader().getSources().get(i), i, temp_s_cols);
					
					Object [] keys = temp_s_cols.toArray();

					/*if(dataSource.getDataSourceType().equals("fileDataSource"))
					{
						temp_s_cols.clear();
						temp_s_cols.addAll(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), JobTypeParser.getReader().getSources().get(i), false));
					}*/
					
					for(String key : temp_s_cols)
					{ 
						//System.out.println("Adding source column \'" + key.toString() + "\' to sourceMinMax");
						MetricsRecord metRecord = MetricsRecordUtil.getRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(JobTypeParser.getReader().getSources().get(i)), key.toString());
						//System.out.println(metRecord);
						sourceMinMax.put(key.toString(), new String []{metRecord.getMinimum(), metRecord.getMaximum()});
						//System.out.println("Added");
					}
					
					boolean useSourceColumns = dataSource.getDataSourceType().equals("fileDataSource");
					
					dataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(i).getTargetTypeCd());
					
					if(useSourceColumns && dataSource.getDataSourceType().equals("teradataDataSource") && JobTypeParser.getRowCountAndMetrics().get(i).getMappingSpecifications().equalsIgnoreCase("NA"))					// if source is a file and target is a table, use the columns of the source for sum, only if the mapping specification is NA)					// if source is a file and target is a table, use the columns of the source for sum
						t_obj_exists = dataSource.getMinMax(JobTypeParser.getReader().getTargets().get(i), i, temp_s_cols);
					else
						t_obj_exists = dataSource.getMinMax(JobTypeParser.getReader().getTargets().get(i), i, temp_t_cols);	
					
					/*temp_t_cols.clear();
					temp_t_cols.addAll(temp_s_cols);*/
					
					keys = temp_t_cols.toArray();
					
					//System.out.println("Current source and target are: \'" + JobTypeParser.getReader().getSources().get(i) + "\' and \'" + JobTypeParser.getReader().getTargets().get(i) + "\'");
					for(Object key : keys)
					{ 
						//System.out.println("Adding target column \'" + key.toString() + "\' to targetMinMax");
						
						MetricsRecord metRecord = MetricsRecordUtil.getRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(JobTypeParser.getReader().getTargets().get(i)), key.toString());
						//System.out.println(metRecord);
						targetMinMax.put(key.toString(), new String []{metRecord.getMinimum(), metRecord.getMaximum()});
						//System.out.println("Added");
					}
					
					//System.out.println("temp_s_cols: " + temp_s_cols);
					//System.out.println("temp_t_cols: " + temp_t_cols);
					
					if(s_obj_exists && t_obj_exists)
					{	
						if(JobTypeParser.getRowCountAndMetrics().get(i).getMappingSpecifications().equalsIgnoreCase("NA") && !JobTypeParser.re_order_cols(temp_s_cols, temp_t_cols, JobTypeParser.getReader().getSources().get(i), JobTypeParser.getReader().getTargets().get(i)))		// if there is a mismatch in source and target columns, return right away
						{
							JobTypeParser.getLogger().info("Module \'Maximum Value\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(i) + "\' and target \'" + JobTypeParser.getReader().getTargets().get(i) + "\'");
							JobTypeParser.getMinmaxlogger().debug("maxModule execution time: " + (System.currentTimeMillis() - startTime) + "(ms)");
							continue;
						}
						
						String insertion_query1 = "Insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + "." + getResultTableName() + " (test_status, execution_timestamp, source_name, target_name, source_path, target_path, source_column_name, target_column_name, source_max, target_max, business_date, test_cycle_id, test_type_cd, user_id, stream_id, sub_stream_id, source_min, target_min, env_id, source_type_cd, target_type_cd, script_text) VALUES (";
						
						int nullCount = 0;
						
						for(int j=0; j< Math.min(temp_s_cols.size(), temp_t_cols.size()); ++j)
						{	
							env = JobTypeParser.getRowCountAndMetrics().get(i).getEnvId();
							String insertion_query2 = "";

							String status = "";
							
							//System.out.println("target column name: " + temp_t_cols.get(j));
							
							if( (!sourceMinMax
									.get(temp_s_cols.get(j))[0].equals("") 
									&& !sourceMinMax
										.get(temp_s_cols.get(j))[1].equals("") 
									&& !targetMinMax
										.get(temp_t_cols.get(j))[0].equals("")
									&& !targetMinMax
										.get(temp_t_cols.get(j))[1].equals("")) 
									&& (StringUtils.isNumeric(sourceMinMax
											.get(temp_s_cols.get(j))[0]) 
									&& StringUtils.isNumeric(sourceMinMax
											.get(temp_s_cols.get(j))[1])) 
									&& (StringUtils.isNumeric(targetMinMax
											.get(temp_t_cols.get(j))[0]) 
									&& StringUtils.isNumeric(targetMinMax
											.get(temp_t_cols.get(j))[1])) )
							{
								if(sourceMinMax.get(temp_s_cols.get(j))[0] == null || targetMinMax.get(temp_t_cols.get(j))[0] == null || sourceMinMax.get(temp_s_cols.get(j))[1] == null || targetMinMax.get(temp_t_cols.get(j))[1] == null)
								{
									++nullCount;
									continue;
								}
								
								//System.out.println("sourceMinMax.get(temp_s_cols.get(j))[0]: \'" + sourceMinMax.get(temp_s_cols.get(j))[0] + "\', targetMinMax.get(temp_t_cols.get(j))[0]: \'" + targetMinMax.get(temp_t_cols.get(j))[0] + "\', sourceMinMax.get(temp_s_cols.get(j))[1]: \'" + sourceMinMax.get(temp_s_cols.get(j))[1] + "\', targetMinMax.get(temp_t_cols.get(j))[1]: \'" + targetMinMax.get(temp_t_cols.get(j))[1] + "\'");
								
								if( (Double.parseDouble(sourceMinMax.get(temp_s_cols.get(j))[0]) == Double.parseDouble(targetMinMax.get(temp_t_cols.get(j))[0])) && (Double.parseDouble(sourceMinMax.get(temp_s_cols.get(j))[1]) == Double.parseDouble(targetMinMax.get(temp_t_cols.get(j))[1])) )	// check equality of both max and min
									status = "Passed";
								else
									status = "Failed";
							}

							else
							{
								if(sourceMinMax.get(temp_s_cols.get(j))[0] == null || targetMinMax.get(temp_t_cols.get(j))[0] == null)
								{
									++nullCount;
									continue;
								}
								
								if(FileDataRetriever.isTimeStamp(sourceMinMax.get(temp_s_cols.get(j))[0]) && FileDataRetriever.isTimeStamp(targetMinMax.get(temp_t_cols.get(j))[0]))
								{
									SimpleDateFormat sdf = FileDataRetriever.getTimeStampFormat(sourceMinMax.get(temp_s_cols.get(j))[0]);
									
									status = ( (sdf.parse(sourceMinMax.get(temp_s_cols.get(j))[0]).equals(sdf.parse(targetMinMax.get(temp_t_cols.get(j))[0]))) && (sdf.parse(sourceMinMax.get(temp_s_cols.get(j))[1]).equals(sdf.parse(targetMinMax.get(temp_t_cols.get(j))[1]))) ) ? "Passed" : "Failed";
								}
								
								else
								{
									if( (sourceMinMax.get(temp_s_cols.get(j))[0].equals(targetMinMax.get(temp_t_cols.get(j))[0])) && (sourceMinMax.get(temp_s_cols.get(j))[1].equals(targetMinMax.get(temp_t_cols.get(j))[1])) )
										status = "Passed";
									else
										status = "Failed";
								}
							}
							
							String script_text = "";

							if(sourceInfo[0].equals(".") && targetInfo[0].equals("."))
								script_text = "Select max(s." + temp_s_cols.get(j) + ") as source_max, max(t." + temp_t_cols.get(j) + ") as target_max, min(s." + temp_s_cols.get(j) + ") as source_min, min(t." + temp_t_cols.get(j) + ") as target_min from " + JobTypeParser.getReader().getSources().get(i) + " s, " + JobTypeParser.getReader().getTargets().get(i) + " t;";
							else
								script_text = "Source and/or target is a file - query cannot be generated";
							
							insertion_query2 = insertion_query1 + "\'" + status + "\', \'" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "\', \'" + sourceInfo[2] + "\', \'" + targetInfo[2] + "\', \'" + sourceInfo[1] + "\', \'" + targetInfo[1] + "\', \'" + temp_s_cols.get(j) + "\', \'" + temp_t_cols.get(j) + "\', \'" + (sourceMinMax.get(temp_s_cols.get(j))[1].startsWith("\'") ? sourceMinMax.get(temp_s_cols.get(j))[1].replace("'","''") : sourceMinMax.get(temp_s_cols.get(j))[1]) + "\', \'" + (targetMinMax.get(temp_t_cols.get(j))[1].startsWith("\'") ? targetMinMax.get(temp_t_cols.get(j))[1].replace("'","''") : targetMinMax.get(temp_t_cols.get(j))[1]) + "\', \'" + ConfigurationManager.getInstance().getUserConfig().getBusinessDate() + "\', \'" + env + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', \'" + ConfigurationManager.getInstance().getAppConfig().getMinMax() + "\', " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getUserId()) + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getStreamId() + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getSubStreamId() + ", \'" + (sourceMinMax.get(temp_s_cols.get(j))[0].startsWith("\'") ? sourceMinMax.get(temp_s_cols.get(j))[0].replace("'","''") : sourceMinMax.get(temp_s_cols.get(j))[0]) + "\', \'" + (targetMinMax.get(temp_t_cols.get(j))[0].startsWith("\'") ? targetMinMax.get(temp_t_cols.get(j))[0].replace("'","''") : targetMinMax.get(temp_t_cols.get(j))[0]) + "\', \'" + env + "\', " + JobTypeParser.getRowCountAndMetrics().get(i).getSourceTypeCd() + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getTargetTypeCd() + ", \'" + script_text + "\');";
							
							//System.out.println(insertion_query2);
							
							//stmt.addBatch(insertion_query2);		// add current insert statement into batch
							if(useDefaultBatch())
							{
								if((numOfMMTables - prevNumOfMMTables) > maxTableLimit)
								{
									JobTypeParser.executeBatchStatements(JobTypeParser.getMinMaxBatchStatements());
									prevNumOfMMTables = numOfMMTables;
								}
								
								JobTypeParser.addToBatch(JobTypeParser.getMinMaxBatchStatements(), insertion_query2);
							}
							else
							{
								if((numOfMMTables - prevNumOfMMTables) > maxTableLimit)
								{
									JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
									prevNumOfMMTables = numOfMMTables;
								}
								
								JobTypeParser.addToBatch(JobTypeParser.getScriptExecTestBatchStatements(), insertion_query2);
							}
							insertion_query2 = "";
						}
						
						if(nullCount == sourceMinMax.size() )
							JobTypeParser.getLogger().error("Source and/or Target table is empty - Module \'Min/Max\' cannot be executed");
						else
						{
							//stmt.executeBatch();
							summaryStatus = "Successful";
							++numOfMMTables;
						}
					}

					else
					{	
						try {
							throw new ObjectNotFoundException(s_obj_exists, t_obj_exists, "Max Value", i);
						} catch (ObjectNotFoundException e) {
						}
					}
				}

				else if(!condition.equals("Y") && !JobTypeParser.getReader().getSources().get(i).trim().equals(".") && !JobTypeParser.getReader().getTargets().get(i).trim().equals(".")&& !JobTypeParser.getReader().getSources().get(i).trim().equals("\\") && !JobTypeParser.getReader().getTargets().get(i).trim().equals("\\"))		// if 'N' or blank encountered against this Module in input metadata file, insert 'Not Run'
				{
					if(!condition.equalsIgnoreCase("N"))
					{
						try {
							throw new InvalidInformationException(JobTypeParser.class, new Exception(), "Invalid command \'" + condition + "\' entered at row \'" + (i+2) + "\' in sheet \'Row Count & Metrics Collection\'. Module \'Max Value Check\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(i).trim() + "\' and target \'" + JobTypeParser.getReader().getTargets().get(i).trim() + "\'.");
						} catch (InvalidInformationException e1) {
						}
					}
					summaryStatus = "Not Run";
				}
			} catch(SQLException e) {
				if(e.getSQLState().equals("HY000"))
				{
					try {
						throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".summary_tbl\' or object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Min_Max_Recon_Rslt\'");
					} catch (AccessViolationException e1) {
					}
				}
				else
				{
					try {
						throw new InvalidInformationException(JobTypeParser.class, e, "Min_Max_Recon_Rslt");
					} catch (InvalidInformationException e1) {
					}
				}
			} catch(NumberFormatException e) {
				JobTypeParser.getLogger().error("Data in source or target objects is in quotes - data cannot be entered into result table");
				JobTypeParser.getLogger().error("Module Min/Max cannot be executed");
				JobTypeParser.getMinmaxlogger().error("NumberFormatException is: ", e);
				
			} catch(Exception e) {
				//e.printStackTrace();
				JobTypeParser.getMinmaxlogger().error("Exception encountered in Module Min / Max Reconciliation");
				JobTypeParser.getMinmaxlogger().error("Exception is: ", e);
			} finally {
				if(!summaryStatus.equals("Not Run"))
					QueryGenerator.insertIntoSummary(i, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getMinMax()),"", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(i).getMinMaxValue(), JobTypeParser.getRowCountAndMetrics().get(i).getEnvId()); 	// insert unsuccessful status into summary table
				else
					QueryGenerator.insertIntoSummary(i, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getMinMax()), "Min_Max", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(i).getMinMaxValue(), JobTypeParser.getRowCountAndMetrics().get(i).getEnvId());
				
				if(executeBatch)
				{	
					try {
						if(useDefaultBatch())
							JobTypeParser.executeBatchStatements(JobTypeParser.getMinMaxBatchStatements());
						else
							JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
					} catch (SQLException e) {
						JobTypeParser.getMinmaxlogger().error("Error encountered while inserting into Min/Max Result Table: " + e);
					}
				}
				
				/*if(i == (JobTypeParser.getNumOfRecords() - 1) && numOfMMTables > 0)
					JobTypeParser.getLogger().info("Module \'Min/Max Reconciliation\' completed successfully for " + numOfMMTables + " inputs");*/
			}
				
		}
		
		long endTime = System.currentTimeMillis();
		JobTypeParser.getMinmaxlogger().debug("MinMaxModule execution time: " + (endTime - startTime) + "(ms)");
		Validator.printModuleCompletionPrompt("Min/Max Reconciliation", startTime, endTime, summaryStatus, numOfMMTables);
		
		//System.out.println("Exiting from min max");
		return null;
	}
	
}
