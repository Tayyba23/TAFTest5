package com.td.tafd.ds;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.dbcp2.BasicDataSource;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.modules.di.DataIntegrityAutomation;
import com.td.tafd.vo.DistinctColumnInfo;
import com.td.tafd.vo.MetricsRecord;
import com.td.tafd.vo.MetricsRecordUtil;

public class TeradataDataSource implements DataSource
{	
	private static HashMap<String, BasicDataSource> sourceMachines = null ;
	
	/* ------------------------------------------------------------------------------------------------------------------------------------------------------------------
	 * | Constructor gets all virtual machine information provided in the input to set up connection pools for each machine (e.g. Machine A has its own connection pool |
	 * | Machine or server B has its own connection pool)																											 	|
	 * ------------------------------------------------------------------------------------------------------------------------------------------------------------------ */
	
	public TeradataDataSource()
	{
		if(sourceMachines != null)		// to ensure the connection pools are created only once per application
			return;
			
		sourceMachines = new HashMap<String, BasicDataSource>();
		String [] users = ConfigurationManager.getInstance().getAppConfig().getAllTdUsers().split(",");
		String [] passwords = ConfigurationManager.getInstance().getAppConfig().getAllTdPasswords().split(",");
		
		int i=0;

		for(String machineIP : getMachines())
		{
			sourceMachines.put(machineIP, new BasicDataSource());
			sourceMachines.get(machineIP).setDriverClassName("com.teradata.jdbc.TeraDriver");
			sourceMachines.get(machineIP).setUrl("jdbc:teradata://"+machineIP);
			sourceMachines.get(machineIP).setUsername(users[i]);
			sourceMachines.get(machineIP).setPassword(passwords[i]);
			
			sourceMachines.get(machineIP).setInitialSize(15);	// initial pool size is 15, as there is a total of 13 modules, and two connections are kept for result table access
			++i;
		}
	}
	
	public static String[] getMachines()	// function gets the IP's of all virtual machines for Teradata Objects in the app_config.properties file
	{
		return ConfigurationManager.getInstance().getAppConfig().getAllTdHosts().split(",");
	}
	
	@Override
	public Connection getConnection (String host) throws SQLException		// returns a connection associated with the host's (parameter) connection pool
	{	
		long startTime = System.currentTimeMillis();
		Connection conn = sourceMachines.get(host).getConnection();
		JobTypeParser.getApplicationlogger().info("Connection getting took " + (System.currentTimeMillis() - startTime) + " (ms)");
		return conn;
	}

	@Override
	/** -----------------------------------------------------------------------------------
	 * | Function getDistinctValueCount retrieves number of distinct values of all columns |
	 * | of the Teradata object name passed to it 										   |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the Teradata object									   |
	 * 		-----------------------------------------------------------------------------
	 * | @return list of distinct value counts for each column				   			   |
	 * | @return prompt displayed if table does not exist								   |
	 * ------------------------------------------------------------------------------------ */
	
	public List<DistinctColumnInfo> getDistinctValueCount(String obj_name, ArrayList<String> cols)
	{	
		List<DistinctColumnInfo> list = new ArrayList<DistinctColumnInfo>();
		Connection conn= null;
		PreparedStatement ps2 = null;
		ResultSet rs2 = null;
		
		try {
			String sourceSystemIP = ConfigurationManager.getInstance().getUserConfig().getHostname();	// the current source system that needs to be accessed for executing the query
			
			if(QueryGenerator.getQueryConn() != null && QueryGenerator.getCurrentSourceSystem().equals(sourceSystemIP))	// if, while determining column names, the same source system was made a connection for, use the same one for executing query
			{
				//conn = QueryGenerator.getQueryConn();
			}
			
			else	// otherwise, get a new connection from the connection pool
			{
				try {
					conn = getConnection(sourceSystemIP);
				} catch(SQLException e) {
					JobTypeParser.getApplicationlogger().debug("Connection creation encountered an exception");
					return new ArrayList<DistinctColumnInfo>();		// return empty list
				}
			}
			
			/*Connection conn = getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());*/
			
			/*String query="select ColumnName from dbc.columnsv where TableName = ? and databasename = ?";		// retrieving all the column names present in the table from the schema (dbc.columnsv in the case of Teradata)
			PreparedStatement ps = conn.prepareStatement(query);
			ps.setString(1, obj_name.split("\\.")[1]);
			ps.setString(2, obj_name.split("\\.")[0]);

			ResultSet rs = ps.executeQuery();*/
			StringBuffer query2 = new StringBuffer("SELECT");
			int i = 0;

			for(String col : cols)
			{
				if(i != 0)
					query2.append( ",");
				String name = col.toLowerCase();
				DistinctColumnInfo info = new DistinctColumnInfo();
				info.setName(name);
				list.add(info);
				query2.append( " (SELECT COUNT(*) col1 FROM (SELECT " + name + " FROM " + obj_name + " GROUP BY " + name+ ")A ) A");	// build query based on all the column names of the table
				++i;
			}

			/*rs.close();
			ps.close();*/

			query2.append(";");   // query2 now has the query for all distinct values of a table

			//long queryStartTime = System.currentTimeMillis();
			ps2 = ( (conn==null) ? QueryGenerator.getQueryConn() : conn).prepareStatement(query2.toString());
			rs2 = ps2.executeQuery();							// execute distinct value query
			//JobTypeParser.getLogger().info("Query execution (distinct count) completed in " + (System.currentTimeMillis() - queryStartTime) + " (milliseconds)");
			Iterator<DistinctColumnInfo> it = null;
			if(list != null ) {it = list.iterator();}
			while(rs2.next())
			{
				if(it != null && it.hasNext()) {
					//DistinctColumnInfo dci = it.next();
					for(int j=1; j<=i; ++j)
					{
						//dci.setCount(Integer.parseInt(rs2.getString(j)));
						list.get(j-1).setCount(Integer.parseInt(rs2.getString(j)));
					}
				}
			}

			rs2.close();
			ps2.close();

			if(conn != null)
				conn.close();

		} catch(SQLException e) {
			JobTypeParser.getApplicationlogger().debug("Column not found for table " + obj_name.split("\\.")[1]);
			//e.printStackTrace();
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(ps2 != null)
					ps2.close();
				if(rs2 != null)
					rs2.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Exception in function \'getDistinctValueCount\' in class \'TeradataDataSource\' : " + e);
			}
		}
		
		return list;
	}

	@Override
	/** --------------------------------------------------------------------------------------------------
	 * | Function getRowCount accesses the database and retrieves number of rows of the given table name  |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the File object											   		      |
	 * | @param row_count - single index array to hold the resultant row count of the table		   		  |
	 * 		------------------------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   		      |
	 * | @return resultant row count of the table returned in the parameter row_count					  |
	 * --------------------------------------------------------------------------------------------------- */
	
	public boolean getRowCount(String obj_name, int [] row_count)
	{
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getRowCount\', parameters: \'obj_name\' = " + obj_name);
		boolean ret_val = false;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			conn = getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
		} catch(SQLException e) {
			JobTypeParser.getApplicationlogger().error("SQLException encountered. Exception is: " + e);
		}
		
		try {
			
			if(objectExists(obj_name, conn))		// check if table exists before creating and executing query
			{	
				String query="select count(*) from " + obj_name;
		
				//long queryStartTime = System.currentTimeMillis();
				
				ps = conn.prepareStatement(query);
				rs = ps.executeQuery();
					
				//JobTypeParser.getLogger().info("Query execution (row count) completed in " + (System.currentTimeMillis() - queryStartTime) + " (milliseconds)");
				
				while(rs.next()) 
				{
					String count=rs.getString(1);
					row_count[0] = Integer.parseInt(count);
				}
				
				rs.close();
				ps.close();
				conn.close();
				
				ret_val = true;
			}
			
			else
			{
				ret_val = false;
			}
			
			conn.close();
		} catch(SQLException e) {
			JobTypeParser.getApplicationlogger().debug("Connection creation encountered an exception: " + e);
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(ps != null)
					ps.close();
				if(rs != null)
					rs.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Exception in function \'getRowCount\' in class \'TeradataDataSource\' : " + e);
			}
		}
		
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getRowCount\', row_count = " + row_count[0]);
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getRowCount\', returning \'" + ret_val + "\'");
		return ret_val;
	}
	
	@Override
	/** ---------------------------------------------------------------------------------------------------------
	 * | Function getNullCount retrieves number of nulls of all columns of the Teradata object name passed to it |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the Teradata object										   				  	 |
	 * | @param index 	- index in Metadata File of the current source and target			   			  		 |
	 * | @param retCols - the columns retrieved from the object and will be returned		   			  		 |
	 * 		-----------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   			  		 |
	 * | @return list of column names returned in the parameter 'retCols'								  		 |
	 * ---------------------------------------------------------------------------------------------------------- */
	
	public boolean getNullCount(String objName, int index, ArrayList<String> retCols) 
	{
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getNullCount\', parameters: \'objName\' = " + objName + ", \'index\' = " + index);
		ArrayList<String> cols2 = new ArrayList<String>();
		boolean tblExists = false;
		
		PreparedStatement ps2 = null;
		ResultSet rs2 = null;
		
		String sourceSystemIP = ConfigurationManager.getInstance().getUserConfig().getHostname();	// the current source system that needs to be accessed for executing the query
		Connection conn= null;

		boolean newColAdded = false;
		
		if(QueryGenerator.getQueryConn() != null && QueryGenerator.getCurrentSourceSystem().equals(sourceSystemIP))	// if, while determining column names, the same source system was made a connection for, use the same one for executing query
		{
			//conn = QueryGenerator.getQueryConn();
		}
		
		else	// otherwise, get a new connection from the connection pool
		{
			try {
				conn = getConnection(sourceSystemIP);
			} catch(SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Connection creation encountered an exception");
				return false;
			}
		}
		
		try {
			if(objectExists(objName, ( (conn==null) ? QueryGenerator.getQueryConn() : conn)))		// check if table exists before creating and executing query
			{
				tblExists = true;
				
				if(!retCols.isEmpty())
				{
					cols2.addAll(retCols);		// add all the columns accessed from already identified columns
					retCols.clear();			// clear the received columns
				}
				
				String null_query = QueryGenerator.getQuery(( (conn==null) ? QueryGenerator.getQueryConn() : conn), objName, "null", cols2, false);
				//long queryStartTime = System.currentTimeMillis();
				/*if(conn == null)
					conn = getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());*/
				ps2 = ( (conn==null) ? QueryGenerator.getQueryConn() : conn).prepareStatement(null_query);	// create prepared statement for null value query
				rs2 = ps2.executeQuery();							// execute query
				//JobTypeParser.getLogger().info("Query execution (null count) completed in " + (System.currentTimeMillis() - queryStartTime) + " (milliseconds)");
				
				while(rs2.next())
				{
					for(int k = 1; k <= cols2.size(); ++k)
					{
						newColAdded = false;
						MetricsRecord metRecord =  new MetricsRecord();
						
						if(!DataIntegrityAutomation.getInstance().getMetricsRecords().containsKey(objName))	// if there is no record of metric collection for current object - i.e. this is the first test in Row Count & metrics to be run
						{
							DataIntegrityAutomation.getInstance().getMetricsRecords().put(objName, new ArrayList<MetricsRecord>());
						}
						
						if(!MetricsRecordUtil.recordForColumnExists(DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName) ,cols2.get(k-1)))
						{	
							//DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).add(metRecord);
							newColAdded = true;
						}
						
						metRecord.setColumnIsNumeric(false);
						metRecord.setColumnName(cols2.get(k-1));
						metRecord.setNullCount(Integer.parseInt(rs2.getString(k)));
						
						//DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setColumnName(cols2.get(k-1));
						if(newColAdded)
						{
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).add(metRecord);
						}
						
						else
						{
							//System.out.println("New column not added. Current column being considered is: \'" + cols2.get(k-1) + "\'");
							/*if(!DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).columnIsNumeric())
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setColumnIsNumeric(false);
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setMinimum(minVal);
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setMaximum(maxVal);*/
							
							MetricsRecordUtil.setRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName), metRecord);
						}
						
						retCols.add(cols2.get(k-1));
					}
				}
			}
			
			if(conn != null)
				conn.close();
		} catch (SQLException e) {
			JobTypeParser.getApplicationlogger().debug("Null count retrieval encountered an exception: " + e);
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(ps2 != null)
					ps2.close();
				if(rs2 != null)
					rs2.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Exception in function \'getNullCount\' in class \'TeradataDataSource\' : " + e);
			}
		}
		
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getNullCount\', retCols = " + retCols);
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getNullCount\', returning \'" + tblExists + "\'");
		return tblExists;
	}

	@Override

	/** ---------------------------------------------------------------------------
	 * | Function objectExists accesses the database to check for object existence |
	 * | Handles both Tables and Views											   |
	 * 		-------------------------------------------------------------------
	 * | @param objName - name of the Teradata object							   |
	 * | @param conn 	- Connection to Teradata database			   			   |
	 * 		-----------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 		  	   			   |
	 * ---------------------------------------------------------------------------- */

	public boolean objectExists(String objName, Connection conn)
	{
		JobTypeParser.getApplicationlogger().info("In function \'objectExists\', parameters: \'objName\' = " + objName + ", \'conn\' = " + conn);
		boolean exists = false;

		PreparedStatement ps = null;
		ResultSet rs = null;
		
		if(objName.endsWith(".") || objName.startsWith("."))
			return false;

		try {	
			//DatabaseMetaData meta_data = conn.getMetaData();
			ps = conn.prepareStatement("select tablename from dbc.tablesv where tablename = \'" + objName.split("\\.")[1] +  "\' and databasename = \'" + objName.split("\\.")[0] + "\';");
			//ResultSet rs = meta_data.getTables(obj_name.split("\\.")[0], null, obj_name.split("\\.")[1], null);		// splitting the fully qualified table name to separate the table name
			rs = ps.executeQuery();
			exists = rs.next();
		} catch(SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().debug("Checking for table existence encountered an error");
		} finally {
			try {
				if(ps != null)
					ps.close();
				if(rs != null)
					rs.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().error("Exception in TeradataDataSource.objectExists(): " + e);
			}
		}

		JobTypeParser.getApplicationlogger().info("In function \'objectExists\', returning \'" + exists + "\'");
		return exists;
	}
	
	/** ---------------------------------------------------------------------------
	 * | Function columnExists accesses the database to check for column existence |
	 * | Handles both Tables and Views											   |
	 * 		-------------------------------------------------------------------
	 * | @param dbName - name of the Teradata database the table/view belongs to   |
	 * | @param tableName - name of the Teradata table or view					   |
	 * | @param columnName - name of the column to be searched for				   |
	 * | @param conn 	- Connection to Teradata database			   			   |
	 * 		-----------------------------------------------------------------------
	 * | @return true if column exists, false otherwise 		  	   			   |
	 * ---------------------------------------------------------------------------- */
	
	public boolean columnExists(String dbName, String tableName, String columnName, Connection conn)
	{
		JobTypeParser.getApplicationlogger().info("In function \'columnExists\', parameters: \'dbName\' = " + dbName + ", \'tableName\'" + tableName + ", \'columnName\'" + columnName + ", \'conn\' = " + conn);
		boolean exists = false;

		PreparedStatement ps = null;
		ResultSet rs = null;
		
		if(tableName.trim().equals("."))
			return false;
		
		try {	
			ps = conn.prepareStatement("select columnname from dbc.columnsv where columnname = \'" + columnName + "\' and tablename = \'" + tableName +  "\' and databasename = \'" + dbName + "\';");
			rs = ps.executeQuery();
			exists = rs.next();
			//rs.close();
		} catch(SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().debug("Checking for column existence encountered an exception : " + e);
		} finally {
			try {
				if(ps != null)
					ps.close();
				if(rs != null)
					rs.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().error("Exception in TeradataDataSource.columnExists(): " + e);
			}
		}

		JobTypeParser.getApplicationlogger().info("In function \'columnExists\', returning \'" + exists + "\'");
		return exists;
	}

	@Override
	public String getDataSourceType() {
		return "teradataDataSource";
	}

	@Override
	
	/** --------------------------------------------------------------------------------------------------
	 * | Function getSumAvg retrieves sum and avg of all columns of the Teradata object name passed to it |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the File object											   		      |
	 * | @param index - index in Metadata File of the current source and target				   		      |
	 * | @param retCols - the columns retrieved from the object and will be returned		   		      |
	 * 		------------------------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   		      |
	 * | @return list of column names returned in the parameter 'retCols'								  |
	 * --------------------------------------------------------------------------------------------------- */
	
	public boolean getSumAvg(String objName, int index, ArrayList<String> retCols) 
	{
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getSumAvg\', parameters: \'objName\' = " + objName + ", \'index\' = " + index);
		ArrayList<String> cols2 = new ArrayList<String>();
		boolean tblExists = false;
		
		PreparedStatement ps2 = null;
		ResultSet rs2 = null;
		
		boolean newColAdded = false;
		String sourceSystemIP = ConfigurationManager.getInstance().getUserConfig().getHostname();	// the current source system that needs to be accessed for executing the query
		Connection conn= null;

		if(QueryGenerator.getQueryConn() != null && QueryGenerator.getCurrentSourceSystem().equals(sourceSystemIP))	// if, while determining column names, the same source system was made a connection for, use the same one for executing query
		{
			//System.out.println("queryConn is not null. Assigning its value to conn. IP is: \'" + sourceSystemIP + "\'");
			//conn = QueryGenerator.getQueryConn();
		}
		
		else	// otherwise, get a new connection from the connection pool
		{
			try {
				//System.out.println("queryConn is null or this is a new source system. getting new connection for IP: \'" + sourceSystemIP + "\'");
				conn = getConnection(sourceSystemIP);
			} catch(SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Connection creation encountered an exception");
				return false;
			}
		}
		
		try {
			if(objectExists(objName, ( (conn==null) ? QueryGenerator.getQueryConn() : conn)))		// check if table exists before creating and executing query
			{	
				tblExists = true;

				String sum_query = "";
				if(!retCols.isEmpty())
				{
					cols2.addAll(retCols);		// add all the columns accessed from already identified columns
					retCols.clear();			// clear the received columns
				}

				sum_query = QueryGenerator.getQuery(( (conn==null) ? QueryGenerator.getQueryConn() : conn), objName, "sum_avg", cols2, true);

				int nullCount = 0;

				if(!sum_query.startsWith("invalid"))
				{
					/*if(conn == null)
						conn = getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());*/
					
					//long queryStartTime = System.currentTimeMillis();
					ps2 = ( (conn==null) ? QueryGenerator.getQueryConn() : conn).prepareStatement(sum_query);	// create prepared statement for sum query
					rs2 = ps2.executeQuery();							// execute query
					
					//JobTypeParser.getLogger().info("Query execution (Sum/Avg count) completed in " + (System.currentTimeMillis() - queryStartTime) + " (milliseconds)");
					
					//System.out.println("objName: " + objName);
					
					while(rs2.next())
					{
						int l = 1;
						
						for(int k = 1; k <= cols2.size(); ++k)
						{
							newColAdded = false;
							MetricsRecord metRecord = new MetricsRecord();
							if(!DataIntegrityAutomation.getInstance().getMetricsRecords().containsKey(objName))	// if there is no record of metric collection for current object - i.e. this is the first test in Row Count & metrics to be run
							{
								//System.out.println("Object \'" + objName + "\' does not exist in map, adding it");
								DataIntegrityAutomation.getInstance().getMetricsRecords().put(objName, new ArrayList<MetricsRecord>());
								//System.out.println("Added object");
							}
							
							if(!MetricsRecordUtil.recordForColumnExists(DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName) ,cols2.get(k-1)))
							{
								//System.out.println("column \'" + cols2.get(k-1) + "\' for object \'" + objName + "\' does not exist in map, adding it");
								/*metRecord.setColumnName(cols2.get(k-1));
								metRecord.setColumnIsNumeric(true);*/
								//System.out.println("Added column");
								newColAdded = true;
							}

							String sumVal = rs2.getString(l);
							String avgVal = rs2.getString(l+1);

							if(sumVal == null)
								++nullCount;
							else
							{
								double sumValue = Double.parseDouble(sumVal);
								double avgValue = Double.parseDouble(avgVal);

								/*System.out.println("sumValue: " + sumValue);
								System.out.println("avgValue: " + avgValue);*/
								
								metRecord.setColumnName(cols2.get(k-1));
								metRecord.setColumnIsNumeric(true);
								metRecord.setSum(sumValue);
								metRecord.setAverage(avgValue);

								if(newColAdded)
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).add(metRecord);
								}
								
								else
								{
									//System.out.println("New column not added. Current column being considered is: \'" + cols2.get(k-1) + "\'");
									/*if(!DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).columnIsNumeric())
										DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setColumnIsNumeric(false);
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setMinimum(minVal);
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setMaximum(maxVal);*/
									
									MetricsRecordUtil.setRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName), metRecord);
								}
								
								//DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).add(metRecord);
								
								//System.out.println("metRecord.getColumnName(): " + metRecord.getColumnName() + ", metRecord.getSum(): " + metRecord.getSum() + ", metRecord.getAverage(): " + metRecord.getAverage());
								retCols.add(cols2.get(k-1));
							}
							
							l+=2;
						}

						if( (nullCount == (cols2.size() - 1) ) && nullCount != 0)
							retCols.add("table_is_empty");
					}
				}

				else
				{
					//System.out.println("Adding no numeric col");
					retCols.add("no_numeric_cols");
				}
			}

			if(conn != null)
				conn.close();

		}  catch(SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().debug("Sum_Avg query generated an exception in function \'getSumAvg\' in class \'TeradataDataSource\' : " + e);
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(ps2 != null)
					ps2.close();
				if(rs2 != null)
					rs2.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Exception in function \'getSumAvg\' in class \'TeradataDataSource\' : " + e);
			}
		}

		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getSumAvg\', retCols = " + retCols);
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getSumAvg\', returning \'" + tblExists + "\'");
		return tblExists;
	}

	@Override
	
	/** --------------------------------------------------------------------------------------------------
	 * | Function getMinMax retrieves min and max of all columns of the Teradata object name passed to it |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the File object											   		      |
	 * | @param index - index in Metadata File of the current source and target				   		      |
	 * | @param retCols - the columns retrieved from the object and will be returned		   		      |
	 * 		------------------------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   		      |
	 * | @return list of column names returned in the parameter 'retCols'								  |
	 * --------------------------------------------------------------------------------------------------- */
	
	public boolean getMinMax(String objName, int index, ArrayList<String> retCols) 
	{
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getMinMax\', parameters: \'objName\' = " + objName + ", \'index\' = " + index);
		ArrayList<String> cols2 = new ArrayList<String>();
		boolean tblExists = false;

		PreparedStatement ps2 = null;
		ResultSet rs2 = null;
		
		String sourceSystemIP = ConfigurationManager.getInstance().getUserConfig().getHostname();	// the current source system that needs to be accessed for executing the query
		Connection conn= null;

		boolean newColAdded = false;
		if(QueryGenerator.getQueryConn() != null && QueryGenerator.getCurrentSourceSystem().equals(sourceSystemIP))	// if, while determining column names, the same source system was made a connection for, use the same one for executing query
		{
			//conn = QueryGenerator.getQueryConn();
		}
		
		else	// otherwise, get a new connection from the connection pool
		{
			try {
				conn = getConnection(sourceSystemIP);
			} catch(SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Connection creation encountered an exception");
				return false;
			}
		}
		try {
			if(objectExists(objName, ( (conn==null) ? QueryGenerator.getQueryConn() : conn)))		// check if table exists before creating and executing query
			{	
				tblExists = true;

				String min_max_query = "";
				if(!retCols.isEmpty())
				{
					cols2.addAll(retCols);		// add all the columns accessed from already identified columns
					retCols.clear();			// clear the received columns
				}

				min_max_query = QueryGenerator.getQuery(( (conn==null) ? QueryGenerator.getQueryConn() : conn), objName, "min_max", cols2, false);
				
				int nullCount = 0;

				if(!min_max_query.startsWith("invalid"))
				{
					/*if(conn == null)
						conn = getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());*/
					
					//long queryStartTime = System.currentTimeMillis();
					
					ps2 = ( (conn==null) ? QueryGenerator.getQueryConn() : conn).prepareStatement(min_max_query);	// create prepared statement for min_max query
					rs2 = ps2.executeQuery();							// execute query
					
					//JobTypeParser.getLogger().info("Query execution (Min/Max count) completed in " + (System.currentTimeMillis() - queryStartTime) + " (milliseconds)");
					while(rs2.next())
					{
						int l = 1;
						
						for(int k = 1; k <= cols2.size(); ++k)
						{
							newColAdded = false;
							MetricsRecord metRecord = new MetricsRecord();
							
							if(!DataIntegrityAutomation.getInstance().getMetricsRecords().containsKey(objName))	// if there is no record of metric collection for current object - i.e. this is the first test in Row Count & metrics to be run
							{
								//System.out.println("Object \'" + objName + "\' does not exist in map, adding it");
								DataIntegrityAutomation.getInstance().getMetricsRecords().put(objName, new ArrayList<MetricsRecord>());
								//System.out.println("Added object");
							}
							
							if(!MetricsRecordUtil.recordForColumnExists(DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName) ,cols2.get(k-1)))
							{	
								//System.out.println("column \'" + cols2.get(k-1) + "\' for object \'" + objName + "\' does not exist in map, adding it");
								/*metRecord.setColumnName(cols2.get(k-1));
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).add(metRecord);*/
								//System.out.println("Added column");
								newColAdded = true;
							}

							String maxVal = rs2.getString(l);
							String minVal = rs2.getString(l+1);

							if(maxVal == null)
								++nullCount;
							else
							{
								metRecord.setColumnName(cols2.get(k-1));
								metRecord.setColumnIsNumeric(false);
								metRecord.setMinimum(minVal);
								metRecord.setMaximum(maxVal);
								
								if(newColAdded)
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).add(metRecord);
								}
								
								else
								{
									//System.out.println("New column not added. Current column being considered is: \'" + cols2.get(k-1) + "\'");
									/*if(!DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).columnIsNumeric())
										DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setColumnIsNumeric(false);
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setMinimum(minVal);
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName).get(k-1).setMaximum(maxVal);*/
									
									MetricsRecordUtil.setRecordsForColumn(DataIntegrityAutomation.getInstance().getMetricsRecords().get(objName), metRecord);
								}
								
								retCols.add(cols2.get(k-1));
							}
							
							l+=2;
						}

						if(nullCount == (cols2.size() - 1) )
						{
							JobTypeParser.getLogger().info("table is empty");
							retCols.add("table_is_empty");
						}
					}
				}

				else
				{
					//System.out.println("Adding no numeric col");
					retCols.add("no_numeric_cols");
				}
			}

			if(conn != null)
				conn.close();

		}  catch(SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getApplicationlogger().debug("Min_Max query generated an exception in function \'getMinMax\' in class \'TeradataDataSource\': " + e);
		} finally {
			try {
				if(conn != null)
					conn.close();
				if(ps2 != null)
					ps2.close();
				if(rs2 != null)
					rs2.close();
			} catch (SQLException e) {
				JobTypeParser.getApplicationlogger().debug("Exception in function \'getMinMax\' in class \'TeradataDataSource\': " + e);
			}
		}

		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getMinMax\', retCols = " + retCols);
		JobTypeParser.getApplicationlogger().info("In function \'TeradataDataSource.getMinMax\', returning \'" + tblExists + "\'");
		return tblExists;
	}
}
