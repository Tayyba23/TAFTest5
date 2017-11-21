package com.td.tafd;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Sheet;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.core.JobTypeParser.datatypes;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.modules.di.DataIntegrityAutomation;
import com.td.tafd.modules.di.FileDataRetriever;
import com.td.tafd.vo.ColumnAndDatatypeInfo;
import com.td.tafd.vo.ColumnMapping;
import com.td.tafd.vo.DistinctColumnInfo;
import com.td.tafd.vo.MetricsRecordUtil;
import com.td.tafd.vo.TableAndDuplicateTypeInfo;

public class QueryGenerator 
{
	private static Connection queryConn;
	private static String currentSourceSystem;
	
	/**
	 * @return the queryConn
	 */
	public static Connection getQueryConn() {
		return queryConn;
	}

	/**
	 * @param queryConn the queryConn to set
	 */
	public static void setQueryConn(Connection queryConnection) {
		queryConn = queryConnection;
	}
	
	/**
	 * @return the currentSourceSystem
	 */
	public static String getCurrentSourceSystem() {
		return currentSourceSystem;
	}

	/**
	 * @param currentSourceSystem the currentSourceSystem to set
	 */
	public static void setCurrentSourceSystem(String currentSourceSystem) {
		QueryGenerator.currentSourceSystem = currentSourceSystem;
	}

	/**
	 * sets the queryConnection object to the relevant source system, retrieving it from the pool
	 * @param sourceSystemIP
	 */
	
	public static void setConnectionForSourceSystem(String sourceSystemIP)
	{
		setCurrentSourceSystem(sourceSystemIP);
		try {
			if(queryConn == null)
				queryConn = new TeradataDataSource().getConnection(sourceSystemIP);
		} catch (SQLException e) {
			JobTypeParser.getApplicationlogger().error("Error getting connection in function QueryGenerator.setConnectionForSourceSystem. " + e);
		}
	}
	
	/**
	 * Function getMaxTestId retrieves the current maximum value of test_id column in table specified by parameter tableName
	 */
	
	public static int getMaxTestId(final Connection conn, final String tableName)
	{
		JobTypeParser.getApplicationlogger().info("In function \'getMaxTestId\', parameters: \'conn\' = " + conn + ", \'tableName\' = " + tableName);
		int maxTestId = 0;
		
		String testIdRetrievalQuery = new StringBuilder("select max(test_id) from ").append(tableName).append(";").toString();
		//JobTypeParser.getApplicationlogger().debug(testIdRetrievalQuery);
		PreparedStatement ps  = null;
		ResultSet rs = null;
		
		try {
			ps = conn.prepareStatement(testIdRetrievalQuery);
			rs = ps.executeQuery();
			
			if(rs.next())
			{
				String maxId = rs.getString(1);
				if(maxId != null)
					maxTestId = Integer.parseInt(maxId);
			}
			
			ps.close();
			rs.close();
			
		} catch (SQLException e) {
			JobTypeParser.getApplicationlogger().debug("maxTestId retrieval encountered an exception: " + e);
		} finally {
			try {
				if(ps != null)
					ps.close();
				if(rs != null)
					rs.close();
				} catch (SQLException e) {
					JobTypeParser.getApplicationlogger().debug("maxTestId retrieval encountered an exception: " + e);
				}
		}
		
		JobTypeParser.getApplicationlogger().info("In function \'getMaxTestId\', max TestId being returned for table \'" + tableName + "\': " + maxTestId);
		return maxTestId;
	}
	
	/** 
	 * Function 'get_query' creates the required query type (identified by 'query_type' variable) and returns it
	 * Examples are sum(column1), sum(column2) | min(column1), min(column2) etc.
	 */
	
	public static String getQuery(Connection conn, String obj_name, String query_type, ArrayList<String> cols2, boolean sum_avg)
	{
		JobTypeParser.getApplicationlogger().info("In function \'getQuery\', parameters: \'conn\' = " + conn + ", \'obj_name\' = " + obj_name + ", \'query_type\' = " + query_type + ", \'sum_avg\' = " + sum_avg);
		
		if(query_type.equalsIgnoreCase("duplicate"))
		{
			cols2.addAll(getColumns(conn, obj_name, query_type, sum_avg, true));		// if duplicate check is being executed, technical columns need to be removed
			if(cols2.isEmpty())
				return "invalid query";
		}
		
		/*else if(cols2.isEmpty())		// retrieve columns only when they have not already been received
			cols2.addAll(getColumns(conn, obj_name, query_type, sum_avg, true));		// otherwise they stay as they are
*/		
		if(cols2.isEmpty())
			return "invalid query";
		
		String type_query = "SELECT ";

		if(query_type.equalsIgnoreCase("null"))		// if null query is to be generated, update the string "query_type" - instead of 'sum' or 'avg', the query_type now becomes 'COUNT(*)-COUNT' as this is the equivalent of conceptual null check, just as 'sum()' and 'avg()' are Teradata functions equivalent to conceptual sums and averages
		{
			
			for(int i=0; i < cols2.size(); ++i)
			{
				if(i != 0)
				{
					type_query += ",";
				}

				type_query += " (SELECT COUNT(*) x FROM " + obj_name + " WHERE " + cols2.get(i) + " IS NULL) A";	// build query based on all the column names of the table
			}
			
			type_query += ";";
		}

		else if (!query_type.equalsIgnoreCase("null") && !query_type.equalsIgnoreCase("sum_avg") && !query_type.equalsIgnoreCase("min_max"))		// for duplicate query generation follow the following procedure
		{
			type_query += "COUNT(*) FROM ( SELECT ";
			
			for(int j=0; j < cols2.size(); ++j)
			{
				if(j != 0)
				{
					type_query += ",";
				}

				type_query += " " + cols2.get(j);
			}

			type_query += ", count(*) as cnt from " + obj_name;
			
			for (TableAndDuplicateTypeInfo ti : JobTypeParser.getTableAndDupTypeInfo())
			{
				if(obj_name.equalsIgnoreCase(ti.getDbName() + "." + ti.getTableName()))
				{
					if(ti.getOpenRecords().equalsIgnoreCase("Y"))
						type_query += " where " + ti.getEndDateColumn() + " = (SELECT max(" + ti.getEndDateColumn() + ") from " + obj_name + ")";
				}
			}
			
			type_query += " group by ";

			for(int j=0; j < cols2.size(); ++j)
			{
				if(j != 0)
				{
					type_query += ",";
				}

				type_query += " " + cols2.get(j);
			}

			type_query += " having cnt > 1) A;";
		}
		
		else if (query_type.equalsIgnoreCase("sum_avg"))	// query for sum and average together
		{
			for(int j=0; j < cols2.size(); ++j)
			{
				if(j != 0)
				{
					type_query += ",";
				}

				type_query += " sum( cast(" + cols2.get(j) + " as BIGINT)), avg( cast(" + cols2.get(j) + " as BIGINT))";
			}

			type_query += " from " + obj_name + ";";
		}
		
		else if (query_type.equalsIgnoreCase("min_max"))	// query for minimum and maximum together
		{
			for(int j=0; j < cols2.size(); ++j)
			{
				if(j != 0)
				{
					type_query += ",";
				}

				type_query += " max(" + cols2.get(j) + "), min(" + cols2.get(j) + ")";
			}

			type_query += " from " + obj_name + ";";
		}
		
		//System.out.println(type_query);
		JobTypeParser.getApplicationlogger().info("In function \'getQuery\', returning query = " + type_query);
		return type_query;
	}
	
	/**
	 * Function getColumns returns all the columns of the object passed to it, after accessing the database
	 * If the parameter 'removeTechnicalCols' is true, the technical columns are removed before the columns are returned
	 */
	
	public static ArrayList<String> getColumns(Connection conn, String obj_name, String query_type, boolean sum_avg, boolean removeTechnicalCols)
	{	
		JobTypeParser.getApplicationlogger().info("In function \'getColumns\', parameters: \'conn\' = " + conn + ", \'obj_name\' = " + obj_name + ", \'query_type\' = " + query_type + ", \'sum_avg\' = " + sum_avg + ", \'removeTechnicalCols\' = " + removeTechnicalCols);
		//JobTypeParser.getLogger().info("In function \'getColumns\', parameters: \'conn\' = " + conn + ", \'obj_name\' = " + obj_name + ", \'query_type\' = " + query_type + ", \'sum_avg\' = " + sum_avg + ", \'removeTechnicalCols\' = " + removeTechnicalCols);
		
		ArrayList<String> cols2 = new ArrayList<String>();
		boolean isView[] = new boolean[1];
		isView[0] = false;						// initial value
		
		if(!JobTypeParser.getTableToColumnsMap().containsKey(obj_name))
		{
			JobTypeParser.getApplicationlogger().info("Query type: " + query_type + ". Querying for columns");
			try {
				ArrayList<String> cols = new ArrayList<String>();	// holds all column names retrieved from database
				ArrayList<String> d_types = new ArrayList<String>();
				ArrayList<String> d_types2 = new ArrayList<String>();

				String query="select ColumnName, ColumnType from dbc.columnsv where TableName = ? and databasename = ?";		// retrieving all the column names present in the table from the schema (dbc.columnsv in the case of Teradata)
				
				PreparedStatement ps = conn.prepareStatement(query);
				ps.setString(1, obj_name.split("\\.")[1]);
				ps.setString(2, obj_name.split("\\.")[0]);

				//System.out.println(query);
				
				ResultSet rs = ps.executeQuery();

				while(rs.next())
				{
					cols.add(rs.getString(1));
					d_types.add(rs.getString(2));
				}	

				ColumnAndDatatypeInfo cdi  = null;
				
				for(int i=0; i<d_types.size(); ++i)
				{
					if(sum_avg)
					{
						if(containsValue(datatypes.numerics.getValue(), d_types.get(i), isView))
						{	
							d_types2.add(d_types.get(i));
							cols2.add(cols.get(i));
						}
					}

					else
					{
						d_types2.add(d_types.get(i));
						cols2.add(cols.get(i));
					}
				}
				
				cdi = new ColumnAndDatatypeInfo();
				cdi.setColumnNames(cols);
				cdi.setDatatypes(d_types);
				
				JobTypeParser.getTableToColumnsMap().put(obj_name, cdi);
				
			} catch(SQLException e) {
				JobTypeParser.getApplicationlogger().error("Exception in getColumns(): " + e);
				JobTypeParser.getApplicationlogger().debug("Columns for \'" + query_type + "\' from object \'" + obj_name + "\' cannot be retrieved");
			} catch(Exception e) {
				JobTypeParser.getApplicationlogger().error("Exception in getColumns(): " + e);
				return new ArrayList<String>();
			}
		}
		
		else
		{
			JobTypeParser.getApplicationlogger().info("Query type: " + query_type + ". Did not query for columns");
			
			if(sum_avg)
			{
				ArrayList<String> dTypes = JobTypeParser.getTableToColumnsMap().get(obj_name).getDatatypes();
				
				if(dTypes == null)
					isView[0] = true;
				
				else
				{
					for(int i=0; i<dTypes.size(); ++i)
					{
						if(containsValue(datatypes.numerics.getValue(), dTypes.get(i), isView))
						{
							if(isView[0])	// if the object is a view, the datatypes will be null, so return empty columns
							{
								break;
							}
							cols2.add(JobTypeParser.getTableToColumnsMap().get(obj_name).getColumnNames().get(i));
						}
					}
				}
			}
			else
				cols2 = JobTypeParser.getTableToColumnsMap().get(obj_name).getColumnNames();
		}
		
		if(isView[0])	// return empty column list if object is view
		{
			JobTypeParser.getApplicationlogger().info("In function \'getColumns\', for object: \'" + obj_name + "\' and queryType: \'" + query_type + "\' returning empty column list as object is a view");
			cols2.clear();
			return cols2;
		}
		
		if(removeTechnicalCols)
		{
			return removeTechnicalColumns(cols2, obj_name);
		}
		
		ArrayList<String> temp = new ArrayList<String>();
		
		temp.addAll(cols2);
		
		cols2.clear();
		
		for(String t : temp)						// convert all the columns in the final list to lower case
			cols2.add(t.toLowerCase());
		
		temp.clear();

		JobTypeParser.getApplicationlogger().info("In function \'getColumns\', for object: \'" + obj_name + "\' and queryType: \'" + query_type + "\' returning columns " + cols2);
		return cols2;
	}
	
	/** 
	 * Function removeTechnicalColumns removes the technical columns defined in the input metadata sheet from the columns passed to it
	 */
	
	public static ArrayList<String> removeTechnicalColumns(final ArrayList<String> cols, final String objectName)
	{
		ArrayList<String> allCols = new ArrayList<String>();
		
		for(String col : cols)
			allCols.add(col.toLowerCase());
		
		Sheet sheet = JobTypeParser.getWorkbook().getSheet("Exception List");
		
		List<String> technicalCols =  JobTypeParser.getReader().readColumnWithHeading("Exception_List", sheet, 1, 3);
		JobTypeParser.getApplicationlogger().debug("Technical columns: " + technicalCols);
		
		ArrayList<String> temp = new ArrayList<String>();
		temp.addAll(technicalCols);
		
		technicalCols.clear();
		
		for(String tc : temp)
			technicalCols.add(tc.trim().toLowerCase());
		
		temp.clear();
		
		JobTypeParser.getApplicationlogger().debug("Before removing technical columns: " + allCols);
		allCols.removeAll(technicalCols);		
		JobTypeParser.getApplicationlogger().debug("After removing technical columns: " + allCols);
		
		return allCols;
	}
	
	/**
	 * Function determines and finalizes the column names for source and target after taking into account the code 
	 * (Full or NA) and the column-name mapping specified in the Input Sheet
	 * @param sourceCols - list of source object's columns retrieved from dbc (for tables) or from file
	 * @param targetCols - list of target object's columns retrieved from dbc (for tables) or from file
	 * */
	
	public static boolean determineColumnNames(ArrayList<String> sourceCols, ArrayList<String> targetCols, int index, String queryType, String mappingSpecification)
	{
		boolean reorderingStatus = false;
		
		String currSourceObject = JobTypeParser.getReader().getSources().get(index).trim();
		String currTargetObject = JobTypeParser.getReader().getTargets().get(index).trim();
		
		String currSourceObjectName = "";
		String currTargetObjectName = "";
		
		if(JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd() == 1)
			currSourceObjectName = JobTypeParser.getReader().getSources().get(index).split("\\.")[1];
		else if(JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd() == 2)
			currSourceObjectName = new File(JobTypeParser.getReader().getSources().get(index)).getName();
		
		if(JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd() == 1)
			currTargetObjectName = JobTypeParser.getReader().getTargets().get(index).split("\\.")[1];
		else if(JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd() == 2)
			currTargetObjectName = new File(JobTypeParser.getReader().getTargets().get(index)).getName();
		
		sourceCols.clear();
		targetCols.clear();
		
		switch(mappingSpecification)	// get the mapping specification for current source and target
		{
			case "Full":
				//System.out.println("Full");
				for(ColumnMapping colMapping : JobTypeParser.getColumnMapping())
				{
					if(!colMapping.getSourceName().equalsIgnoreCase(currSourceObjectName) || !colMapping.getTargetName().equalsIgnoreCase(currTargetObjectName)) // if either the source or target table names are different, the current mapping has ended
					{
						/*System.out.println("Here's the issue: ");
						System.out.println("colMapping.getSourceName(): \"" + colMapping.getSourceName() + "\", and currSourceObjectName: \"" + currSourceObjectName + "\"");
						*/continue;
					}
					
					if(queryType.equalsIgnoreCase("sum_avg"))	// for sum avg reconciliation
					{
						if(colMapping.isNumeric())				// consider only numeric columns
						{	
							sourceCols.add(colMapping.getSourceColumn());
							targetCols.add(colMapping.getTargetColumn());
						}
					}
					
					else
					{
						sourceCols.add(colMapping.getSourceColumn());
						targetCols.add(colMapping.getTargetColumn());
					}
				}
				
				reorderingStatus = true;
				
				break;
				
			case "NA":
				
				if(JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd() == 2 && JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd() == 1)	// if target is a file, and mapping specification is not provided (or required), use the target's columns as source's columns
				{
					sourceCols.addAll(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), currTargetObject, (queryType.equalsIgnoreCase("sum_avg"))));
				}
				
				else
				{
					if(JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd() == 1)
					{
						setConnectionForSourceSystem(ConfigurationManager.getInstance().getUserConfig().getHostname());
						sourceCols.addAll(getColumns(queryConn, currSourceObject, queryType, queryType.equalsIgnoreCase("sum_avg"), true));
					} 

					else
						try {
							sourceCols.addAll(FileDataRetriever.getFileColumns(currSourceObject, index));
						} catch (IOException e) {
							JobTypeParser.getApplicationlogger().error("Exception in QueryGenerator.determineColumnNames(): " + e);
						}
				}
				
				if(JobTypeParser.getRowCountAndMetrics().get(index).getSourceTypeCd() == 2 && JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd() == 1)	// if source is a file, and mapping specification is not provided (or required), use the source's columns as target's columns
				{
					targetCols.addAll(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), currSourceObject, (queryType.equalsIgnoreCase("sum_avg"))));
				}
				
				else
				{
					if(JobTypeParser.getRowCountAndMetrics().get(index).getTargetTypeCd() == 1)
					{
						setConnectionForSourceSystem(ConfigurationManager.getInstance().getUserConfig().getHostname());
						targetCols.addAll(getColumns(queryConn, currTargetObject, queryType, queryType.equalsIgnoreCase("sum_avg"), true));
					} 

					else
						try {
							targetCols.addAll(FileDataRetriever.getFileColumns(currTargetObject, index));
						} catch (IOException e) {
							JobTypeParser.getApplicationlogger().error("Exception in QueryGenerator.determineColumnNames(): " + e);
						}
				}
				
				// if there is an issue in retrieving columns (because of null column type, for example)
				// it is handled here
				
				if(sourceCols == null || sourceCols.isEmpty())	// if Source Columns could not be retrieved
				{
					JobTypeParser.getApplicationlogger().info("Source columns could not be retrieved");
					
					if(!(targetCols == null) && !targetCols.isEmpty())
					{
						JobTypeParser.getApplicationlogger().info("Assigning target columns to source columns");
						sourceCols.addAll(targetCols);
					}
					
					else
					{
						JobTypeParser.getApplicationlogger().info("Cannot assign target columns to source columns, as target columns could not be retrieved. Returning false");
						reorderingStatus = false;
					}
				}
				
				else if(targetCols == null || targetCols.isEmpty())	// if Target Columns could not be retrieved
				{
					JobTypeParser.getApplicationlogger().info("Target columns could not be retrieved");
					
					if(!(sourceCols == null) && !sourceCols.isEmpty())
					{
						JobTypeParser.getApplicationlogger().info("Assigning source columns to target columns");
						targetCols.addAll(sourceCols);
					}
					
					else
					{
						JobTypeParser.getApplicationlogger().info("Cannot assign source columns to target columns, as source columns could not be retrieved. Returning false");
						reorderingStatus = false;
					}
				}
				
				else
					reorderingStatus = JobTypeParser.re_order_cols(sourceCols, targetCols, currSourceObject, currTargetObject);	// this will call the function which considers the object with the lesser number of columns as the master table
				
				break;
		}
		
		/*System.out.println(currSourceObject + "/" + currTargetObject);
		System.out.println("sourceCols: " + sourceCols);
		System.out.println("targetCols: " + targetCols);*/
		
		return reorderingStatus;
	}
	
	/** 
	 * Logs SQL error related messages into the log file
	 */
	
	public static void logSQLErrors(SQLException e, String tableName)
	{
		if(e.getSQLState() == null && e.getErrorCode() == 0)
		{
			JobTypeParser.getLogger().error("Error: Connection error - connection to \'" + e.getMessage().substring(e.getMessage().indexOf("socket orig=") + 12).split(" ")[0] + "\' cannot be established");
		}
		
		else if(e.getSQLState().equals("28000"))
			JobTypeParser.getLogger().error("Error: Invalid Username and Password. Please check username and password in file \"config.properties\"");
		
		else if(e.getSQLState().equals("23000"))
			JobTypeParser.getLogger().error("Error: SQL Error - Duplicate Primary key - cannot insert record into \'" + tableName + "\'");
		
		else if(e.getSQLState().equals("HY000"))
		{
			try {
				throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Summary_Tbl\'");
			} catch (AccessViolationException e1) {
			}
		}
		
		else
		{
			String [] msg = e.getMessage().split("] ");
			JobTypeParser.getLogger().error("Error: SQL Error - " + msg[msg.length-1]);
		}
	}
	
	/**
	 * Returns true if array contains the value specified 
	 */
	
	public static boolean containsValue(String [] arr, String val, boolean [] isView)
	{	
		//System.out.println("looking for value: " + val);
		try {
			for(int i=0; i< arr.length; ++i)
			{	
				if(arr[i].trim().equals(val.trim()))
				{
					isView[0] = false;	// object is not a view, as datatype is not null
					return true;
				}
			}

			isView[0] = false;	// object is not a view, as datatype is not null
			return false;
		} catch (NullPointerException e) {
			isView[0] = true;
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().error("Exception in containsValue (Parameters - arr: \'" + arr + "\', val = \'" + val + "\'.\n Exception is: "  + e);
			return false;
		}
	}
	
	public static void insertIntoSummary(int i, int testTypeCode, String testTypeColumnName, String status, String mtdInput, String envId)
	{
		JobTypeParser.getApplicationlogger().info("In function \'insertIntoSummary (overloaded)\', parameters: \'i\' = " + i + ", \'testTypeCode\' = " + testTypeCode + ", \'status\' = " + status);
		
		String add_summary_row = "";
		
		if(testTypeColumnName.equals(""))
			add_summary_row = "insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Summary_Tbl (test_type_cd, test_cycle_id, source, target, metadata_file_input, application_output, status) values (" + "\'" + testTypeCode + "\', \'" + envId + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', \'" + JobTypeParser.getReader().getSources().get(i) + "\', \'" + JobTypeParser.getReader().getTargets().get(i) + "\', \'Y\', \'" + (status.equals("Successful") ? "Y" : "N") + "\', \'" + status + "\');";
		else
			add_summary_row = "insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Summary_Tbl (test_type_cd, test_cycle_id, source, target, metadata_file_input, application_output, status) values (" + "\'" + testTypeCode + "\', \'" + envId + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', \'" + JobTypeParser.getReader().getSources().get(i) + "\', \'" + JobTypeParser.getReader().getTargets().get(i) + "\', \'" + mtdInput + "\', \'N\', \'" + status + "\');";
			
		try {
			long insertStartTime = System.currentTimeMillis();
			JobTypeParser.addToBatch(JobTypeParser.getSummaryBatchStatements(), add_summary_row);
			JobTypeParser.getApplicationlogger().info("One row Insertion ('" + status + "' Summary_Tbl) completed in " + (System.currentTimeMillis() - insertStartTime) + " (milliseconds)");
		} catch (SQLException e) {
			JobTypeParser.getApplicationlogger().error("Exception encountered while adding to summary batch");
			JobTypeParser.getApplicationlogger().error(e.getMessage());
		}
	}
	
	/** 
	 * overloaded insertIntoSummary designed for modules other than preliminary checks
	*/
	
	public static void insertIntoSummary(int i, String testTypeCode, String testTypeColumnName, String status, String source, String target, String envId) throws SQLException
	{
		JobTypeParser.getApplicationlogger().info("In function \'insertIntoSummary (overloaded for other modules)\', parameters: \'i\' = " + i + ", \'testTypeCode\' = " + testTypeCode + ", \'status\' = " + status);

		String add_summary_row = "";

		add_summary_row = "insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Summary_Tbl (test_type_cd, test_cycle_id, source, target, metadata_file_input, application_output, status) values (" + "\'" + testTypeCode + "\', \'" + envId + "_" + ConfigurationManager.getInstance().getAppConfig().getTestCycle() + "\', \'" + source + "\', \'" + target + "\', \'" + "NA" + "\', \'" + (status.equals("Successful") ? "Y" : "N") + "\', \'" + status + "\');";

		long insertStartTime = System.currentTimeMillis();
		JobTypeParser.addToBatch(JobTypeParser.getSummaryBatchStatements(), add_summary_row);
		JobTypeParser.getApplicationlogger().info("One row Insertion ('" + status + "' Summary_Tbl) completed in " + (System.currentTimeMillis() - insertStartTime) + " (milliseconds)");
	}
	
	public static ArrayList<String> convertToStringList(List<DistinctColumnInfo> dcil)
	{
		List<String> convertedList = new ArrayList<String>();
		
		for(DistinctColumnInfo dci : dcil)
			convertedList.add(dci.getName());
		
		return (ArrayList<String>)convertedList;
	}

	
}
