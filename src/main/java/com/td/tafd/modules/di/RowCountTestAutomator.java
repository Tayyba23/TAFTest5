/**
 * 
 */
package com.td.tafd.modules.di;

import java.io.File;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.exceptions.ObjectNotFoundException;
import com.td.tafd.validation.Validator;

/**
 * @author kt186036
 *
 */
public class RowCountTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfRCTables;
	private String resultTableName;
	private boolean useDefaultBatch;
	
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

	public RowCountTestAutomator(String resultTable, boolean useDefaultBatch)
	{
		tdSource = new TeradataDataSource();
		numOfRCTables = 0;
		setResultTableName(resultTable);
		setUseDefaultBatch(useDefaultBatch);
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
	 * Function row_count_comparison compares row counts of source and target passed to it, and
	 * enters the results into Row_Count_Rslt table
	 */
	
	public String rowCountComparison(String source_name, char source_type, String target_name, char target_type, int i, int prevNumOfRCTables, int maxTableLimit)
	{
		JobTypeParser.getRowcountlogger().info("In function \'rowCountComparison\', parameter values: \'source_name\' = " + source_name + ", \'source_type\' = " + source_type + ", \'target_name\' = " + target_name + ", \'target_type\' = " + target_type + ", \'int i\' = " + i);
		DataSource dataSource;
		
		int [] source_row_count = {0};
		int [] target_row_count = {0};
		
		String s_name = "";
		String t_name = "";
		
		String s_db = "";
		String t_db = "";
		
		boolean s_obj_exists = false;
		boolean t_obj_exists = false;
		
		if(source_type == 't' || source_type == 'v')
		{
			if(source_name.endsWith(".") || source_name.startsWith("."))	// i.e. source database or source name is missing in input file
			{
				try {
					throw new MissingInformationException(JobTypeParser.class, new Exception(), "Source object name and/or path not found in input file. Please check Source's path and Name columns in row " + (i+2) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				} catch (MissingInformationException e) {
				}
				return "Unsuccessful";
			}
			
			dataSource = DataSourceFactory.getDataSource(1);
			s_obj_exists = dataSource.getRowCount(source_name, source_row_count);
			s_name = source_name.split("\\.")[1];
			s_db = source_name.split("\\.")[0];
		}

		else if(source_type == 'f')
		{
			if(source_name.endsWith("\\") || source_name.startsWith("\\"))	// i.e. source database or source name is missing in input file
			{
				try {
					throw new MissingInformationException(JobTypeParser.class, new Exception(), "Source object name and/or path not found in input file. Please check Source's path and Name columns in row " + (i+2) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				} catch (MissingInformationException e) {
				}
				return "Unsuccessful";
			}
			
			source_row_count[0] = FileDataRetriever.getInstance().metadata_line_count(source_name, true);
			s_name = new File(source_name).getName();
			s_db = new File(source_name).getParent();
			
			if((new File(s_db + "\\" + s_name)).exists())
				s_obj_exists = true;
		}

		if(target_type == 't' || target_type == 'v')
		{
			if(target_name.endsWith(".") || target_name.startsWith("."))	// i.e. target database or target name is missing in input file
			{
				try {
					throw new MissingInformationException(JobTypeParser.class, new Exception(), "Target object name and/or path not found in input file. Please check Target's path and Name columns in row " + (i+2) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				} catch (MissingInformationException e) {
				}
				return "Unsuccessful";
			}
			
			dataSource = DataSourceFactory.getDataSource(1);
			t_obj_exists = dataSource.getRowCount(target_name, target_row_count);
			t_name = target_name.split("\\.")[1];
			t_db = target_name.split("\\.")[0];
		}

		else if(target_type == 'f')
		{
			if(target_name.endsWith("\\") || target_name.startsWith("\\"))	// i.e. target database or target name is missing in input file
			{
				try {
					throw new MissingInformationException(JobTypeParser.class, new Exception(), "Target object name and/or path not found in input file. Please check Target's path and Name columns in row " + (i+2) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				} catch (MissingInformationException e) {
				}
				return "Unsuccessful";
			}
			
			target_row_count[0] = FileDataRetriever.getInstance().metadata_line_count(target_name, true);
			t_name = new File(target_name).getName();
			t_db = new File(target_name).getParent();
			
			if((new File(target_name)).exists())
				t_obj_exists = true;
		}
		
		String status = "";
		String insertion_query = "";
				
		if(s_obj_exists && t_obj_exists)		// if table or file does not exist, do not make entry in output database
		{	
			insertion_query = "Insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + "." + getResultTableName() + " (test_status, execution_timestamp, source_name, target_name, source_path, target_path, source_total_count, target_total_count, business_date, test_cycle_id, test_type_cd, user_id, stream_id, sub_stream_id, env_id, source_type_cd, target_type_cd, script_text) VALUES (";

			if(source_row_count[0] == target_row_count[0])
				status = "Passed";
			else
				status = "Failed";

			String env = JobTypeParser.getRowCountAndMetrics().get(i).getEnvId();
			
			//int source_type_col_id = JobTypeParser.getReader().getColId("Source_Type_Cd", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			//int target_type_col_id = JobTypeParser.getReader().getColId("Target_Type_Cd", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			
			String script_text = "";

			if(source_type == 't' && target_type == 't')
				script_text = "Select (Select count(*) from " + s_db + "." + s_name + ") as source_count, (Select count(*) from " + t_db + "." + t_name + ") as target_count;";

			else
				script_text = "Source and/or target is a file - query cannot be generated";
			
			insertion_query += "\'" + status + "\', \'" + new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()) + "\', \'" + s_name + "\', \'" + t_name + "\', \'" + s_db + "\', \'" + t_db + "\', " + source_row_count[0] + ", " + target_row_count[0] + ", \'" + ConfigurationManager.getInstance().getUserConfig().getBusinessDate() + "\', \'" + env + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getRowCount()) + ", " + Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getUserId()) + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getStreamId() + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getSubStreamId() + ", \'" + env + "\', " + JobTypeParser.getRowCountAndMetrics().get(i).getSourceTypeCd() + ", " + JobTypeParser.getRowCountAndMetrics().get(i).getTargetTypeCd() + ", \'" + script_text + "\');";

			try {
				//System.out.println("JobTypeParser.getRowCountBatchStatements()[0]: " + JobTypeParser.getRowCountBatchStatements()[0]);
				if(useDefaultBatch())
				{
					if((numOfRCTables - prevNumOfRCTables) > maxTableLimit)
					{
						JobTypeParser.executeBatchStatements(JobTypeParser.getRowCountBatchStatements());
						prevNumOfRCTables = numOfRCTables;
					}
					
					JobTypeParser.addToBatch(JobTypeParser.getRowCountBatchStatements(), insertion_query);
				}
				else
				{
					if((numOfRCTables - prevNumOfRCTables) > maxTableLimit)
					{
						JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
						prevNumOfRCTables = numOfRCTables;
					}
					
					JobTypeParser.addToBatch(JobTypeParser.getScriptExecTestBatchStatements(), insertion_query);
				}
				insertion_query = "";		
				return "Successful";
				
			} catch(SQLException e) {
				JobTypeParser.getRowcountlogger().error("Exception: " + e);
				//e.printStackTrace();
				if(e.getSQLState().equals("HY000"))
				{
					try {
						throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'select\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Row_Count_Rslt\' or object \'" + s_name + "\' or object \'" + t_name + "\'");
					} catch (AccessViolationException e1) {
					}
				}
				
				else
				{
					try {
						throw new InvalidInformationException(JobTypeParser.class, e, "Row_Count_Rslt");
					} catch (InvalidInformationException e1) {
					}
				}
			}
		}
		
		else
		{	
			try {
				throw new ObjectNotFoundException(s_obj_exists, t_obj_exists, "Row Count", i);
			} catch (ObjectNotFoundException e) {
			}
			return "Unsuccessful";
		}
		
		return "Unsuccessful";
	}
	
	/*public void launchService() {
		ExecutorService service = null;
		try {
			service = Executors.newSingleThreadExecutor();
			service.submit(new Thread(this));
			service.shutdown();
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch(Exception e){
			e.printStackTrace();
		} finally {
			// service.shutdown();
		}
	}*/
	
	/**
	 * encapsulates the row count comparison module's implementation through calling row_count_comparison
	 */
	
	@Override
	public Void call() 
	{
		boolean executeBatch = false;
		int numOfRecords = Math.min(JobTypeParser.getReader().getSources().size(), JobTypeParser.getReader().getTargets().size());
		String summaryStatus = "Unsuccessful";
		long startTime = System.currentTimeMillis();
		int prevNumOfRCTables = 0;
		int maxTableLimit = 300;
		
		for(int i = 0; i < numOfRecords; ++i) 
		{
			JobTypeParser.getRowcountlogger().info("In function \'rowCountModuleManager\', parameter values: \'int i\' = " + i);

			executeBatch = (i == (numOfRecords - 1));
			
			List<String> conditions = JobTypeParser.getReader().readColumnWithHeading("Row_Count", JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection"), 1, 0);

			/*env_col_id = JobTypeParser.getReader().getColId("Env_Id", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			stream_col_id = JobTypeParser.getReader().getColId("Stream_Id", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());
			sub_stream_col_id = JobTypeParser.getReader().getColId("Sub_Stream_Id", "Row Count & Metrics Collection", JobTypeParser.getWorkbook());*/

			String s_name = "";
			String t_name= "";

			s_name = JobTypeParser.getReader().getSources().get(i);
			t_name = JobTypeParser.getReader().getTargets().get(i);

			summaryStatus = "Unsuccessful";

			if(!s_name.equals(null) && !t_name.equals(null) && conditions.get(i).toUpperCase().equals("Y") && !s_name.trim().equals(".") && !t_name.trim().equals("."))		// check both source and target for nulls, and verify that this test needs to be performed or not through Input Parameter File
			{
				if(i == 0)
				{
					JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'ROW COUNT\'				|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
				}

				summaryStatus = rowCountComparison(s_name, JobTypeParser.getReader().getObjectType(s_name), t_name, JobTypeParser.getReader().getObjectType(t_name), i, prevNumOfRCTables, maxTableLimit);
			}

			else if(!conditions.get(i).toUpperCase().equals("Y") && !s_name.trim().equals(".") && !t_name.trim().equals("."))	// if 'N' or blank encountered against this Module in input metadata file, insert 'Not Run'
			{
				if(!conditions.get(i).equalsIgnoreCase("N"))
				{
					if(i == 0)
					{
						JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'ROW COUNT\'				|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
					}

					try {
						throw new InvalidInformationException(JobTypeParser.class, new Exception(), "Invalid command \'" + conditions.get(i) + "\' entered at row \'" + (i+2) + "\' in sheet \'Row Count & Metrics Collection\'. Module \'Row Count Verification\' cannot be executed for source \'" + JobTypeParser.getReader().getSources().get(i).trim() + "\' and target \'" + JobTypeParser.getReader().getTargets().get(i).trim() + "\'.");
					} catch (InvalidInformationException e1) {
					}
				}			
				summaryStatus = "Not Run";

			}
			if(!summaryStatus.equals("Not Run"))
				QueryGenerator.insertIntoSummary(i, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getRowCount()), "", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(i).getRowCount(), JobTypeParser.getRowCountAndMetrics().get(i).getEnvId());
			else
				QueryGenerator.insertIntoSummary(i, Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getRowCount()), "Row_Count", summaryStatus, JobTypeParser.getRowCountAndMetrics().get(i).getRowCount(), JobTypeParser.getRowCountAndMetrics().get(i).getEnvId());

			if(summaryStatus.equals("Successful"))
				++numOfRCTables;

			/*if(i == (JobTypeParser.getNumOfRecords() - 1) && numOfRCTables > 0)
				JobTypeParser.getLogger().info("Module \'Row Count\' completed successfully for " + numOfRCTables + " inputs");*/

			if(executeBatch)
			{	
				//System.out.println("In row count. Executing batch");
				//System.out.println("iteration is: " + i);
				try {
					if(useDefaultBatch())
						JobTypeParser.executeBatchStatements(JobTypeParser.getRowCountBatchStatements());
					else
						JobTypeParser.executeBatchStatements(JobTypeParser.getScriptExecTestBatchStatements());
				} catch (SQLException e) {
					JobTypeParser.getRowcountlogger().error("Error encountered while inserting into Row Count Result Table");
				}
			}

			//JobTypeParser.getRowcountlogger().debug("rowCountComparison execution time: " + (System.currentTimeMillis() - startTime) + "(ms)");
		}
		
		long endTime = System.currentTimeMillis();
		JobTypeParser.getRowcountlogger().debug("rowCountComparison execution time: " + (endTime - startTime) + "(ms)");
		Validator.printModuleCompletionPrompt("Row Count", startTime, endTime, summaryStatus, numOfRCTables);
		
		//System.out.println("Exiting from row count");
		return null;
		
	}
}
