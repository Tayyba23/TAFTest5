/**
 * 
 */
package com.td.tafd.modules.minus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.MinusTest;
import com.td.tafd.vo.MinusTestColumnMapping;

/**
 * @author KT186036
 *
 */
public class MinusTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfMTTables;
	
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
	
	public MinusTestAutomator()
	{
		tdSource = new TeradataDataSource();
		numOfMTTables = 0;
	}
	
	/**
	 * Retrieves and re-orders the columns for both tables entered as input
	 * @param objectA - Fully qualified Table A name
	 * @param objectB - Fully qualified Table B name
	 * @param tableACols - Output parameter to hold table A's columns
	 * @param tableBCols - Output parameter to hold table B's columns
	 * @param mappingSpecification - Current value for 'mapping specification' column in input sheet
	 * @param removeTechnicalCols - flag to indicate whether to remove technical columns or not
	 * @return True iff columns were retrieved and re-ordered successfully, False if either condition fails
	 */
	
	public boolean retrieveAndReOrderColumns(String objectA, String objectB, ArrayList<String> tableACols, ArrayList<String> tableBCols, String mappingSpecification, boolean removeTechnicalCols, Connection conn)
	{
		boolean reorderingStatus = false;
		boolean initialized = false;		// to check if currObjectNameA and currobjectNameB have been initialized
		
		String currObjectNameA = "";
		String currObjectNameB = "";
		
		String queryType = "minus";
		
		switch(mappingSpecification)
		{
			case "Full":
				
				for(MinusTestColumnMapping colMapping : JobTypeParser.getMinusTestColumnMapping())
				{
					if(!initialized)	// setting up initial Object Names for tables A and B
					{
						currObjectNameA = colMapping.getTblNameA();
						currObjectNameB = colMapping.getTblNameB();
						initialized = true;
					}
					
					if(!colMapping.getTblNameA().equalsIgnoreCase(currObjectNameA) || !colMapping.getTblNameB().equalsIgnoreCase(currObjectNameB)) // if either the table A or table B names are different, the current mapping has ended
						break;
					
					tableACols.add(colMapping.getColumnA());
					tableBCols.add(colMapping.getColumnB());
				}
				
				reorderingStatus = true;
				break;
				
			case "NA":
				
				/*TeradataDataSource tdSource = new TeradataDataSource();
				Connection queryConn = null;
				
				System.out.println("getting connection");
				try {
					queryConn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
				} catch (SQLException e) {
					JobTypeParser.getMinustestlogger().error("Exception in MinusTestAutomator: " + e);
				}*/
				//System.out.println("got connection");
				tableACols.addAll(QueryGenerator.getColumns(conn, objectA, queryType, queryType.equalsIgnoreCase("sum_avg"), removeTechnicalCols));
				tableBCols.addAll(QueryGenerator.getColumns(conn, objectB, queryType, queryType.equalsIgnoreCase("sum_avg"), removeTechnicalCols));
				
				// Handling any issues in retrieving columns (because of null column type, for example) here
				
				if(tableACols == null || tableACols.isEmpty())	// if table A Columns could not be retrieved
				{
					JobTypeParser.getMinustestlogger().error("Columns for table \'" + objectA + "\' could not be retrieved");
					JobTypeParser.getApplicationlogger().info("Columns for table \'" + objectA + "\' could not be retrieved");
					return false;
				}
				
				if(tableBCols == null || tableBCols.isEmpty())	// if table B Columns could not be retrieved
				{
					JobTypeParser.getMinustestlogger().error("Columns for table \'" + objectB + "\' could not be retrieved");
					JobTypeParser.getApplicationlogger().info("Columns for table \'" + objectB + "\' could not be retrieved");
					return false;
				}
				
				reorderingStatus = JobTypeParser.re_order_cols(tableACols, tableBCols, objectA, objectB);	// this will call the function which considers the object with the lesser number of columns as the master table
				break;
				
			default:
				JobTypeParser.getLogger().error("Invalid Command \'" + mappingSpecification + "\' in column \'Mapping_Specification\' in sheet \'Minus Test\' in the Input Metadata Sheet. Module \'Minus Test\' cannot be executed for this entry");
				reorderingStatus = false;
				break;	
		}
		
		return reorderingStatus;
	}
	
	public String getMinusTestQuery(String objectA, String objectB, String mappingSpecification, boolean removeTechnicalCols, Connection conn)
	{
		StringBuilder builder = new StringBuilder(); 
		
		ArrayList<String> tableACols = new ArrayList<String>();
		ArrayList<String> tableBCols = new ArrayList<String>();
		
		if(!retrieveAndReOrderColumns(objectA, objectB, tableACols, tableBCols, mappingSpecification, removeTechnicalCols, conn))
		{
			//System.out.println("Issue in retrieving and reordering columns");
			JobTypeParser.getApplicationlogger().error("Minus Test query cannot be generated");
			builder.append("invalid query");
		}
		
		else
		{
			builder.append("SELECT COUNT(*) FROM (");
			builder.append("SELECT ");								// select col1, col2,...
			for(int i=0; i < tableACols.size(); ++i)
			{
				if(i != 0)
					builder.append(", ");
				builder.append(tableACols.get(i));
			}
			builder.append(" FROM ").append(objectA);				// from table A
			
			builder.append(" MINUS ");								// MINUS
			
			builder.append("SELECT ");
			for(int i=0; i < tableBCols.size(); ++i)				// select col1, col2,...
			{
				if(i != 0)
					builder.append(", ");
				builder.append(tableBCols.get(i));
			}
			builder.append(" FROM ").append(objectB).append(") A;");	// from table B
		}
		
		//System.out.println("returning query: " + builder.toString());
		return builder.toString();
	}
	
	public boolean executeQuery(String query, Connection conn, int [] rows)
	{
		try {
			//System.out.println("query: " + query);
			PreparedStatement stmt = conn.prepareStatement(query);
			
			//System.out.println("executing query.");
			
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next())
				rows[0] = rs.getInt(1);
			
		} catch (SQLException e) {
			//e.printStackTrace();
			rows[0] = -1;					// set an invalid number to indicate failure
			JobTypeParser.getMinustestlogger().error("Exception in MinusTestAutomator.executeQuery(): " + e);
			return false;
		}
		
		return true;
	}
	
	public String runMinusTest(Connection conn, String tableA, String tableB, String mappingSpecification, boolean removeTechnicalCols, int [] rows)
	{
		String minusTestQuery = getMinusTestQuery(tableA, tableB, mappingSpecification, removeTechnicalCols, conn);

		//System.out.println("query retrieved: " + minusTestQuery);
		
		if(minusTestQuery.contains("invalid query"))
		{
			JobTypeParser.getLogger().error("Error retrieving columns or generating query. Minus Test cannot be run for \'" + tableA + "\' and \'" + tableB + "\'");
		}

		rows[0] = 0;

		// running query

		if(!executeQuery(minusTestQuery, conn, rows))
		{
			JobTypeParser.getLogger().error("Mismatch in column names. \'Minus Test\' cannot be run for \'" + tableA + "\' and \'" + tableB + "\'");
			minusTestQuery = "invalid query";
		}
		
		return minusTestQuery;
	}
	
	public int [] getRowCounts(Connection conn, String tableA, String tableB)
	{
		int [] rowCounts = new int[2];
		
		StringBuilder builder = new StringBuilder();
		builder.append("SELECT (SELECT COUNT(*) FROM ").append(tableA).append(") as countA, (SELECT COUNT(*) FROM ").append(tableB).append(") as countB;");
		
		try {
			PreparedStatement stmt = conn.prepareStatement(builder.toString());
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next())
			{
				rowCounts[0] = rs.getInt(1);
				rowCounts[1] = rs.getInt(2);
			}
			
		} catch (SQLException e) {
			JobTypeParser.getMinustestlogger().error("Exception in MinusTestAutomator: " + e);
		}
		
		return rowCounts;
	}
	
	public Void call()
	{
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
				"\t\t\t    |				Running Module \'MINUS TEST\'			|\n" + 
				"\t\t\t    --------------------------------------------------------------------------------------");
		
		long startTime = System.currentTimeMillis();
		String summaryStatus = "Unsuccessful";
		
		Connection conn = null;
		
		try {
			conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
		} catch (SQLException e) {
			JobTypeParser.getMinustestlogger().error("Exception in MinusTestAutomator: " + e);
		}
		
		// get input values from relevant object in JobTypeParser
		
		List<MinusTest> minusTestList = JobTypeParser.getMinusTestList();
		
		int i=0;
		for(MinusTest minusTest : minusTestList)	// iterating through the MinusTest list
		{	
			boolean runAMinusB = minusTest.getaMinusB().equalsIgnoreCase("Y");
			boolean runBMinusA = minusTest.getbMinusA().equalsIgnoreCase("Y");
			
			if(!runAMinusB && !runBMinusA)
				continue;
			// Ascertaining presence of objects in database
			
			String tableA = minusTest.getDbNameA() + "." + minusTest.getTblNameA();
			String tableB = minusTest.getDbNameB() + "." + minusTest.getTblNameB();
			
			//System.out.println("In minus test. i: " + i + "tableA: " + tableA + ", tableB: " + tableB);
			
			if(tableA.trim().equals(".") || tableB.trim().equals("."))	// empty rows in input MTD sheet
			{
				++i;
				continue;
			}
			
			if(!tdSource.objectExists(tableA, conn))
			{
				JobTypeParser.getLogger().error("Object \'" + tableA + "\' does not exist. Minus Test cannot be run for this input.");
				++i;
				continue;
			}
			
			if(!tdSource.objectExists(tableB, conn))
			{
				JobTypeParser.getLogger().error("Object \'" + tableB + "\' does not exist. Minus Test cannot be run for this input.");
				++i;
				continue;
			}
			
			summaryStatus = "Unsuccessful";
			
			int [] rowCounts = new int [2];
			rowCounts[0] = 0;
			rowCounts[1] = 0;
			
			// Getting Row Counts for both tables
			
			rowCounts = getRowCounts(conn, tableA, tableB);
			
			//System.out.println("got row counts.");
			
			int rowCountA = rowCounts[0];
			int rowCountB = rowCounts[1];
			
			// Retrieving query for Minus Test
			
			String minusTestQuery = "";
			
			String aMinusB = "NA";
			String bMinusA = "NA";
			
			int [] rows = new int[1];
			
			if(runAMinusB)
			{
				//System.out.println("getting a minus b query.");
				rows[0] = 0;

				minusTestQuery = runMinusTest(conn, tableA, tableB, minusTest.getMappingSpecification(), (minusTest.getExcludeTechnicalCols().equalsIgnoreCase("Y")), rows);
				//System.out.println("executed a minus b query.");
				JobTypeParser.getMinustestlogger().info(minusTestQuery);
				if(minusTestQuery.contains("invalid query"))
				{
					minusTestQuery = "";
					aMinusB = "NA";
				}
				
				else
					aMinusB = "" + rows[0];
			}
			
			if(runBMinusA)
			{
				//System.out.println("getting b minus a query.");
				rows[0] = 0;
				minusTestQuery += runMinusTest(conn, tableB, tableA, minusTest.getMappingSpecification(), (minusTest.getExcludeTechnicalCols().equalsIgnoreCase("Y")), rows);
				//System.out.println("executed a minus b query.");
				//System.out.println(minusTestQuery);
				if(minusTestQuery.contains("invalid query"))
				{
					minusTestQuery = "";
					bMinusA = "NA";
				}
				
				else
					bMinusA = "" + rows[0];
			}
			
			String status = "";
			
			// Setting the status of the Test according to the query results
			
			//System.out.println("setting test status.");
			if(!aMinusB.equalsIgnoreCase("NA") && bMinusA.equalsIgnoreCase("NA"))
				status = ( (aMinusB.equalsIgnoreCase("0")) && (rowCountA != 0 && rowCountB != 0) ) ? "Passed" : "Failed";	// if either Row Count is zero, or (A-B) > 0, test status fails
			else if(aMinusB.equalsIgnoreCase("NA") && !bMinusA.equalsIgnoreCase("NA"))
				status = ( (bMinusA.equalsIgnoreCase("0")) && (rowCountA != 0 && rowCountB != 0) ) ? "Passed" : "Failed";	// if either Row Count is zero, or (B-A) > 0, test status fails
			else if(!aMinusB.equalsIgnoreCase("NA") && !bMinusA.equalsIgnoreCase("NA"))
				status = ( (bMinusA.equalsIgnoreCase("0") && aMinusB.equalsIgnoreCase("0")) && (rowCountA != 0 && rowCountB != 0) ) ? "Passed" : "Failed";	// if either Row Count is zero, or either (A-B) > 0 or (B-A) > 0, test status fails
			
			// *** VALIDATE NULLABLE RESULTS ***
			
			StringBuilder builder = new StringBuilder();
			
			// Inserting results into result table
			
			//System.out.println("inserting into batch");
			
			builder.append("INSERT INTO ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(". Minus_Test_Rslt (stream_id, sub_stream_id, test_cycle_id, test_type_cd, test_status, db_name_a, tbl_name_a, db_name_b, tbl_name_b, a_minus_b, b_minus_a, row_count_a, row_count_b, script_text, business_date, user_id, env_id, execution_timestamp) VALUES (");
			
			builder.append(minusTest.getStreamId()).append(", ").append(minusTest.getSubStreamId()).append(", \'").append(minusTest.getEnvId()).append("_").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', ").append(ConfigurationManager.getInstance().getAppConfig().getMinusTest()).append(", \'").append(status).append("\', \'").append(minusTest.getDbNameA()).append("\', \'").append(minusTest.getTblNameA()).append("\', \'").append(minusTest.getDbNameB()).append("\', \'").append(minusTest.getTblNameB()).append("\', \'").append(aMinusB).append("\', \'").append(bMinusA).append("\', ").append(rowCountA).append(", ").append(rowCountB).append(", \'").append(minusTestQuery).append("\', \'").append(ConfigurationManager.getInstance().getUserConfig().getBusinessDate()).append("\', ").append(1).append(", \'").append(minusTest.getEnvId()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\');");
		
			try {
				JobTypeParser.addToBatch(JobTypeParser.getMinusTestBatchStatements(), builder.toString());
				summaryStatus = "Successful";
				++numOfMTTables;
				
				// Insertion into Summary table
				QueryGenerator.insertIntoSummary(i, ConfigurationManager.getInstance().getAppConfig().getMinusTest(), "", summaryStatus, tableA, tableB, minusTest.getEnvId());
			
			} catch (SQLException e) {
				e.printStackTrace();
				JobTypeParser.getMinustestlogger().error("Exception in MinusTestAutomator: " + e);
			}
			++i;
		}
		
		try {
			//System.out.println("In minus test. Executing batch");
			JobTypeParser.executeBatchStatements(JobTypeParser.getMinusTestBatchStatements());
		} catch (SQLException e) {
			JobTypeParser.getMinustestlogger().error("Exception in MinusTestAutomator: " + e);
		} finally {
			if(conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
				}
		}
		
		long endTime = System.currentTimeMillis();
		Validator.printModuleCompletionPrompt("Minus Test", startTime, endTime, summaryStatus, numOfMTTables);
	
		//System.out.println("Exiting from minus test");
		return null;
	}
}
