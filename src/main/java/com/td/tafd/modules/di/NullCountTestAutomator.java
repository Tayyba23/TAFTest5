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
import com.td.tafd.exceptions.InvalidInformationException;
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.exceptions.ObjectNotFoundException;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.MetricsRecord;
import com.td.tafd.vo.MetricsRecordUtil;

/**
 * @author kt186036
 *
 */
public class NullCountTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfNCTables;
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
	
	public NullCountTestAutomator(String resultTable, boolean useDefaultBatch)
	{
		tdSource = new TeradataDataSource();
		numOfNCTables = 0;
		setResultTableName(resultTable);
		setUseDefaultBatch(useDefaultBatch);
	}

	@Override
	public Void call() throws Exception 
	{
		boolean executeBatch = false;
		int numOfRecords = Math.min(JobTypeParser.getReader().getSources().size(), JobTypeParser.getReader().getTargets().size());
		String summaryStatus = "";
		long startTime = System.currentTimeMillis();
		int prevNumOfNCTables = 0;
		int maxTableLimit = 150;
		
		for(int index = 0; index < numOfRecords; ++index) 
		{
			JobTypeParser.getNullcountlogger().info("In function \'nullValueModuleManager\', parameter: \'index\' = " + index);
			executeBatch = (index == (numOfRecords - 1));
			
			DataSource dataSource;
			
			boolean s_obj_exists = false;
			boolean t_obj_exists = false;
			
			ArrayList<String> s_cols = new ArrayList<String>();
			ArrayList<String> t_cols = new ArrayList<String>();
			
			String condition = JobTypeParser.getRowCountAndMetrics().get(index).getNullCount();
			summaryStatus = "";
			
			if(!condition.equalsIgnoreCase("N") && index == 0)
			{
				JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'NULL VALUE CHECK\'			|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
			}
			
			if(condition.equalsIgnoreCase("Y"))	// check if column was marked Y
			{			
				String env = "";

				summaryStatus = "Unsuccessful";
				
				String [] sourceInfo = new String [3];
				String [] targetInfo = new String [3];
				
				sourceInfo = Validator.getInstance().getObjectInformation(JobTypeParser.getReader().getObjectType(JobTypeParser.getReader().getSources().get(index)), "Source", JobTypeParser.getReader().getSources().get(index), index);
				targetInfo = Validator.getInstance().getObjectInformation(JobTypeParser.getReader().getObjectType(JobTypeParser.getReader().getTargets().get(index)), "Target", JobTypeParser.getReader().getTargets().get(index), index);
				
				if(sourceInfo[0] == null || targetInfo[0] == null)
					continue;
				
				HashMap<String, String> source_nulls = new HashMap<String, String>();
				HashMap<String, String> target_nulls = new HashMap<String, String>();

				QueryGenerator.determineColumnNames(s_cols, t_cols, index, "null", JobTypeParser.getRowCountAndMetrics().get(index).getMappingSpecifications());
				
				dataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd());
				s_obj_exists = dataSource.getNullCount(JobTypeParser.getReader().getSources().get(index), index, s_cols);
				
				ArrayList<String> temp_s_cols = new ArrayList<String>();
				

				/*if(dataSource.getDataSourceType().equals("fileDataSource"))
					temp_s_cols.addAll(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), JobTypeParser.getReader().getSources().get(index), false));
				
				else*/
					temp_s_cols.addAll(s_cols);
				
				Object [] keys = temp_s_cols.toArray();	
				
				for(String key : temp_s_cols)
				{ 
					MetricsRecord metRecord = MetricsRecordUtil.getRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(JobTypeParser.getReader().getSources().get(index)), key.toString()); 
					source_nulls.put(key.toString(), "" + metRecord.getNullCount());
				}
				
				boolean useSourceColumns = dataSource.getDataSourceType().equals("fileDataSource");
				
				dataSource = DataSourceFactory.getDataSource(JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd());
				
				ArrayList<String> temp_t_cols = new ArrayList<String>();
				
				if(useSourceColumns && dataSource.getDataSourceType().equals("teradataDataSource") && JobTypeParser.getRowCountAndMetrics().get(index).getMappingSpecifications().equalsIgnoreCase("NA"))	// if source is a file and target is a table, use the columns of the source, only if the mapping specification is NA
				{
					t_obj_exists = dataSource.getNullCount(JobTypeParser.getReader().getTargets().get(index), index, s_cols);
					temp_t_cols.clear();
					temp_t_cols.addAll(s_cols);
				}
				
				else
				{
					t_obj_exists = dataSource.getNullCount(JobTypeParser.getReader().getTargets().get(index), index, t_cols);
					temp_t_cols.clear();
					temp_t_cols.addAll(t_cols);
				}
				
				keys = temp_t_cols.toArray();
				
				/*System.out.println("temp_s_cols: " + temp_s_cols);
				System.out.println("temp_t_cols: " + temp_t_cols);*/
				
				for(Object key : keys)
				{
					MetricsRecord metRecord = MetricsRecordUtil.getRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(JobTypeParser.getReader().getTargets().get(index)), key.toString()); 
					target_nulls.put(key.toString(), "" + metRecord.getNullCount());
				}
				
				//System.out.println("target_nulls: " + target_nulls);
				
				/*System.out.println("temp_s_cols: " + temp_s_cols);
				System.out.println("temp_t_cols: " + temp_t_cols);*/
				
				if(s_obj_exists && t_obj_exists)
				{	
					if(JobTypeParser.getRowCountAndMetrics().get(index).getMappingSpecifications().equalsIgnoreCase("NA") && !JobTypeParser.re_order_cols(temp_s_cols, temp_t_cols, JobTypeParser.getReader().getSources().get(index), JobTypeParser.getReader().getTargets().get(index)))		// if there is a mismatch in source and target columns, return right away
					{
						if(!JobTypeParser.getReader().getSources().get(index).trim().equals(".") && !JobTypeParser.getReader().getTargets().get(index).trim().equals("."))
							JobTypeParser.getLogger().debug("Module \'Null Value\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(index) + "\' and target \'" + JobTypeParser.getReader().getTargets().get(index) + "\'");
						continue;
					}
					
					/*System.out.println("After re-ordering");
					System.out.println("temp_s_cols: " + temp_s_cols);
					System.out.println("temp_t_cols: " + temp_t_cols);*/
					
					String insertion_query1 = "Insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + "." + getResultTableName() + " (test_status, execution_timestamp, source_name, target_name, source_path, target_path, source_column_name, target_column_name, source_null_count, target_null_count, business_date, test_cycle_id, test_type_cd, user_id, stream_id, sub_stream_id, env_id, source_type_cd, target_type_cd, script_text) VALUES (";

					for(int j=0; j< Math.min(temp_s_cols.size(), temp_t_cols.size()); ++j)
					{
						env = JobTypeParser.getRowCountAndMetrics().get(index).getEnvId();
						String insertion_query2 = "";

						String status = "";

						/*System.out.println("source_nulls: " + source_nulls);
						System.out.println("target_nulls: " + target_nulls);*/
						
						if((StringUtils.isNumeric(source_nulls.get(temp_s_cols.get(j))) && !source_nulls.get(temp_s_cols.get(j)).equals("")) && (StringUtils.isNumeric(target_nulls.get(temp_t_cols.get(j))) && !target_nulls.get(temp_t_cols.get(j)).equals("")) )
						{
							if(Double.parseDouble(source_nulls.get(temp_s_cols.get(j))) == Double.parseDouble(target_nulls.get(temp_t_cols.get(j))))
								status = "Passed";
							else
								status = "Failed";
						}
						
						else
						{
							//System.out.println("temp_s_cols.get(j): " + temp_s_cols.get(j) + ", temp_t_cols.get(j): " + temp_t_cols.get(j));
							if(source_nulls.get(temp_s_cols.get(j)).equals(target_nulls.get(temp_t_cols.get(j))))
								status = "Passed";
							else
								status = "Failed";
						}

						String script_text = "";

						if(sourceInfo[0].equals(".") && targetInfo[0].equals("."))
							script_text = "SELECT ( SELECT COUNT(*) x FROM " + JobTypeParser.getReader().getSources().get(index) + " WHERE " + temp_s_cols.get(j) + " IS NULL) as source_null_count, (SELECT COUNT(*) x FROM " + JobTypeParser.getReader().getTargets().get(index) + " WHERE " + temp_t_cols.get(j) + " IS NULL) as target_null_count;";

						else
							script_text = "Source and/or target is a file - query cannot be generated";
						
						insertion_query2 = insertion_query1 + "\'" + status + "\', \'" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "\', \'" + sourceInfo[2] + "\', \'" + targetInfo[2] + "\', \'" + sourceInfo[1] + "\', \'" + targetInfo[1] + "\', \'" + temp_s_cols.get(j) + "\', \'" + temp_t_cols.get(j) + "\', \'" + source_nulls.get(temp_s_cols.get(j)) + "\', \'" + target_nulls.get(temp_t_cols.get(j)) + "\', \'" + ConfigurationManager.getInstance().getUserConfig().getBusinessDate() + "\', \'" + env + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', \'" + ConfigurationManager.getInstance().getAppConfig().getNullValue() + "\', " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getUserId()) + ", " + JobTypeParser.getRowCountAndMetrics().get(index).getStreamId() + ", " + JobTypeParser.getRowCountAndMetrics().get(index).getSubStreamId() + ", \'" + env + "\', " + JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd() + ", " + JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd() + ", \'" + script_text + "\');";
						
						try {
							if(useDefaultBatch())
							{
								if((numOfNCTables - prevNumOfNCTables) > maxTableLimit)
								{
									JobTypeParser.executeBatchStatements(JobTypeParser.getNullValueBatchStatements());
									prevNumOfNCTables = numOfNCTables;
								}
								
								JobTypeParser.addToBatch(JobTypeParser.getNullValueBatchStatements(), insertion_query2);
							}
							else
							{
								if((numOfNCTables - prevNumOfNCTables) > maxTableLimit)
								{
									JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
									prevNumOfNCTables = numOfNCTables;
								}
								
								JobTypeParser.addToBatch(JobTypeParser.getScriptExecTestBatchStatements(), insertion_query2);
							}
						} catch (SQLException e) {
							JobTypeParser.getNullcountlogger().error("Error adding insert statement \"" + insertion_query2 + "\" to batch");
						} catch(Exception e) {
							JobTypeParser.getNullcountlogger().error("Exception encountered in Module Null Value Check");
							JobTypeParser.getNullcountlogger().error("Exception is: ", e);
						}				
					}
					
					summaryStatus = "Successful";
					++numOfNCTables;
				}
				
				else
				{	
					if(!s_obj_exists && JobTypeParser.getReader().getSources().get(index).trim().equals("."))
					{
						try {
							throw new MissingInformationException(JobTypeParser.class, new Exception(), "Source object name and/or path not found in input file. Please check Source's Path and Name columns in row " + (index+2) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
						} catch (MissingInformationException e) {
						}
					}
					
					else if(!t_obj_exists && JobTypeParser.getReader().getTargets().get(index).trim().equals("."))
					{
						try {
							throw new MissingInformationException(JobTypeParser.class, new Exception(), "Target object name and/or path not found in input file. Please check Target's Path and Name columns in row " + (index+2) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
						} catch (MissingInformationException e) {
						}
					}
					
					else
					{
						try {
							throw new ObjectNotFoundException(s_obj_exists, t_obj_exists, "Null Value", index);
						} catch (ObjectNotFoundException e) {
						}
					}
				}
			}
			
			else if(!condition.equalsIgnoreCase("Y") && !JobTypeParser.getReader().getSources().get(index).trim().equals(".") && !JobTypeParser.getReader().getTargets().get(index).trim().equals("."))				// if 'N' or blank encountered against this Module in input metadata file, insert 'Not Run'
			{
				if(!condition.equalsIgnoreCase("N"))
				{
					try {
						throw new InvalidInformationException(JobTypeParser.class, new Exception(), "Invalid command \'" + condition + "\' entered at row \'" + (index+2) + "\' in sheet \'Row Count & Metrics Collection\'. Module \'Null Value Check\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(index).trim() + "\' and target \'" + JobTypeParser.getReader().getTargets().get(index).trim() + "\'.");
					} catch (InvalidInformationException e1) {
					}
				}
				
				summaryStatus = "Not Run";
			}

			if(!summaryStatus.equals("Not Run"))
				QueryGenerator.insertIntoSummary(index, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getNullValue()), "", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(index).getNullCount(), JobTypeParser.getRowCountAndMetrics().get(index).getEnvId()); 	// insert unsuccessful status into summary table
			else
				QueryGenerator.insertIntoSummary(index, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getNullValue()), "Null_Count", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(index).getNullCount(), JobTypeParser.getRowCountAndMetrics().get(index).getEnvId());
			
			if(executeBatch)
			{	
				try {
					if(useDefaultBatch())
						JobTypeParser.executeBatchStatements(JobTypeParser.getNullValueBatchStatements());
					else
						JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
				} catch (SQLException e) {
					//e.printStackTrace();
					JobTypeParser.getNullcountlogger().error("Error encountered while inserting into Null Value Count Result Table: " + e);
				} catch(Exception e) {
					JobTypeParser.getNullcountlogger().error("Exception encountered in Module Null Value Check");
					JobTypeParser.getNullcountlogger().error("Exception is: ", e);
				}
			}
			
			/*if(index == (JobTypeParser.getNumOfRecords() - 1) && numOfNCTables > 0)
				JobTypeParser.getLogger().info("Module \'Null Value Check\' completed successfully for " + numOfNCTables + " inputs");*/
		}
		
		long endTime = System.currentTimeMillis();
		JobTypeParser.getNullcountlogger().debug("Null Value Check execution time: " + (endTime - startTime) + "(ms)");
		Validator.printModuleCompletionPrompt("Null Value Check", startTime, endTime, summaryStatus, numOfNCTables);
		
		//System.out.println("Exiting from null count");
		return null;
	}
}
