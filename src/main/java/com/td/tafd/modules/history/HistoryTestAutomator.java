/**
 * 
 */
package com.td.tafd.modules.history;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.constants.ConstantsInterface;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.exceptions.InvalidInformationException;
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.exceptions.ObjectNotFoundException;
import com.td.tafd.modules.dd.DuplicateRecordTestAutomator;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.HistoryTestDetail;

/**
 * @author kt186036
 *
 */

public class HistoryTestAutomator implements ConstantsInterface, Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfHHTables;
	
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
	
	public HistoryTestAutomator()
	{
		tdSource = new TeradataDataSource();
		numOfHHTables = 0;
	}

	public String getHistoryCaseQuery(final String caseType, final String tableName, final HashMap<String, String[]> tblToHistoryCount)
	{
		JobTypeParser.getHistorytestlogger().info("In function \'getHistoryCaseQuery\', parameters: \'caseType\' = " + caseType + ", \'tableName\' = " + tableName);
		ArrayList<String> cols = new ArrayList<String>();
		
		String testDetail = "";
		
		for(HistoryTestDetail hd : JobTypeParser.getHistoryTestDetail())
		{
			if(hd.getTableName().equalsIgnoreCase(tableName.split("\\.")[1]))
				testDetail = hd.getTestDetail();
		}
		
		if(testDetail.equalsIgnoreCase("full"))		// if 'Full' has been specified in the MTD against this table, all columns will be retrieved
		{
			Connection conn = null;
			try {
				conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
				cols.addAll(QueryGenerator.getColumns(conn, tableName, "history", false, false));
				
			} catch (SQLException e) {
				JobTypeParser.getHistorytestlogger().error("Exception in getHistoryCaseQuery(): " + e);
			}
		}
		
		else if(testDetail.equalsIgnoreCase("partial"))		// otherwise, for 'partial', only columns specified in MTD (and stored previously in tblToHistoryCount) are considered
		{
			String [] pk_cols = tblToHistoryCount.get(tableName)[0].split(",");

			for(String pk_col : pk_cols)
				cols.add(pk_col);
			cols.add(tblToHistoryCount.get(tableName)[4]);
			cols.add(tblToHistoryCount.get(tableName)[5]);
		}
		
		String query = "";
		
		if(tblToHistoryCount.get(tableName)[0].contains("NA"))
		{
			return "Invalid command entered in column \'PK_Column\' in sheet \'History Verification\'";
		}
		
		if(tblToHistoryCount.get(tableName)[4].equals("NA") && !tblToHistoryCount.get(tableName)[5].equals("NA"))
		{
			return "Invalid command entered in column \'Start_TS_Column\' in sheet \'History Verification\'";
		}
		
		else if(!tblToHistoryCount.get(tableName)[4].equals("NA") && tblToHistoryCount.get(tableName)[5].equals("NA"))
		{
			return "Invalid command entered in column \'End_TS_Column\' in sheet \'History Verification\'";
		}
		
		else if(tblToHistoryCount.get(tableName)[4].equals("NA") && tblToHistoryCount.get(tableName)[5].equals("NA"))
		{
			return "Invalid commands entered in columns \'Start_TS_Column\' and \'End_TS_Column\' in sheet \'History Verification\'";
		}
		
		switch(caseType)
		{
			case "Overlap":												// generate query for History Overlap Case
				StringBuilder strBuild = new StringBuilder("select ");
				
				for(int i=0; i<cols.size(); ++i)
				{
					if(i !=0)
						strBuild.append(",");
					strBuild.append("A.").append(cols.get(i)).append(" AS pk").append(i);
				}
				
				strBuild.append(", B.").append(tblToHistoryCount.get(tableName)[4]).append(" AS strt_dt1, B.").append(tblToHistoryCount.get(tableName)[5]).append(" AS end_dt1");	// B.start_date, B.end_date
				strBuild.append(" from ").append(tableName).append(" A INNER JOIN ").append(tableName).append(" B ON ");
				
				String [] pkCols = tblToHistoryCount.get(tableName)[0].split(",");
				
				for(int i=0; i < pkCols.length; ++i)
				{
					if(i!= 0)
						strBuild.append(" and ");
					strBuild.append("COALESCE (A.").append(pkCols[i]).append(", \'\') = COALESCE(B.").append(pkCols[i]).append(", \'\')");
				}
				
				strBuild.append(" where (A.").append(tblToHistoryCount.get(tableName)[4]).append(" <> B.").append(tblToHistoryCount.get(tableName)[4]).append(") and ((A.").append(tblToHistoryCount.get(tableName)[4]).append(", ").append("A.").append(tblToHistoryCount.get(tableName)[5]).append(") OVERLAPS (B.").append(tblToHistoryCount.get(tableName)[4]).append(", ").append("B.").append(tblToHistoryCount.get(tableName)[5]).append("))");
				
				strBuild.append(" GROUP BY ");
				
				for(int i=1; i<=cols.size()+2; ++i)		// GROUP BY 1,2,3...
				{
					if(i != 1)
						strBuild.append(", ");
					strBuild.append(i);
				}
				
				query = strBuild.append(";").toString();
				
				break;
				
			case "Reverse":												// generate query for History Reverse Case
				strBuild = new StringBuilder("select ");
				
				for(int i=0; i<cols.size(); ++i)
				{
					if(i !=0)
						strBuild.append(",");
					strBuild.append(tableName).append(".").append(cols.get(i)).append(" AS column").append(i);
				}
				
				strBuild.append(" from ").append(tableName);
				
				strBuild.append(" where ").append(tableName).append(".").append(tblToHistoryCount.get(tableName)[4]).append(" > ").append(tableName).append(".").append(tblToHistoryCount.get(tableName)[5]).append(" GROUP BY ");
				
				for(int i=0; i<cols.size(); ++i)
				{
					if(i !=0)
						strBuild.append(",");
					strBuild.append(tableName).append(".").append(cols.get(i));
				}
				
				query = strBuild.append(";").toString();
				break;
				
			case "Gap":													// generate query for History Gap Case
				
				strBuild = new StringBuilder("select ");
				pkCols = tblToHistoryCount.get(tableName)[0].split(",");
				
				for(int i=0; i<cols.size(); ++i)
				{
					if(i !=0)
						strBuild.append(", ");
					strBuild.append("A.").append(cols.get(i)).append(" AS column").append(i);			// A.pk1, A.pk2..., A.start_date, A.end_date
				}
				
				strBuild.append(", B.").append(tblToHistoryCount.get(tableName)[4]).append(" AS strt_date_1, B.").append(tblToHistoryCount.get(tableName)[5]).append(" AS end_date_1");		// B.start_date, B.end_date
				strBuild.append(", (CAST((CAST(B.").append(tblToHistoryCount.get(tableName)[4]).append(" AS DATE) - CAST(A.").append(tblToHistoryCount.get(tableName)[5]).append(" AS DATE)) AS DECIMAL(18,6)) * 60*60*24) + ");
				strBuild.append("((EXTRACT(  HOUR FROM B.").append(tblToHistoryCount.get(tableName)[4]).append(") - EXTRACT(  HOUR FROM A.").append(tblToHistoryCount.get(tableName)[5]).append(")) * 60*60) + ");
				strBuild.append("((EXTRACT(  MINUTE FROM B.").append(tblToHistoryCount.get(tableName)[4]).append(") - EXTRACT(  MINUTE FROM A.").append(tblToHistoryCount.get(tableName)[5]).append(")) * 60) + ");
				strBuild.append("(EXTRACT(  SECOND FROM B.").append(tblToHistoryCount.get(tableName)[4]).append(") - EXTRACT(  SECOND FROM A.").append(tblToHistoryCount.get(tableName)[5]).append(")) AS SEC ");
				
				strBuild.append(" from (");
				
				strBuild.append("SELECT ");
				
				for(int i=0; i<cols.size(); ++i)
				{
					if(i !=0)
						strBuild.append(", ");
					strBuild.append(cols.get(i));						// pk1, pk2..., start_date, end_date
				}
				strBuild.append(", ROW_NUMBER() OVER (PARTITION BY ");	// ROW_NUMBER() OVER (PARTITION BY pk1, pk2... ORDER BY Start_Date Asc) AS Rank_A
				for(int i=0; i < pkCols.length; ++i)
				{
					if(i!= 0)
						strBuild.append(", ");
					strBuild.append(pkCols[i]);
				}
				strBuild.append(" ORDER BY ").append(tblToHistoryCount.get(tableName)[4]).append(" ASC) AS Rank_A FROM ").append(tableName);
				
				strBuild.append(") A INNER JOIN (");
				
				strBuild.append("SELECT ");
				
				for(int i=0; i<cols.size(); ++i)
				{
					if(i !=0)
						strBuild.append(", ");
					strBuild.append(cols.get(i));						// pk1, pk2..., start_date, end_date
				}
				strBuild.append(", ROW_NUMBER() OVER (PARTITION BY ");	// ROW_NUMBER() OVER (PARTITION BY pk1, pk2... ORDER BY Start_Date Asc) AS Rank_B
				for(int i=0; i < pkCols.length; ++i)
				{
					if(i!= 0)
						strBuild.append(", ");
					strBuild.append(pkCols[i]);
				}
				strBuild.append(" ORDER BY ").append(tblToHistoryCount.get(tableName)[4]).append(" ASC) AS Rank_B FROM ").append(tableName);
				
				strBuild.append(") B ON ");
				
				for(int i=0; i < pkCols.length; ++i)					// ON A.pk1 = B.pk1 AND A.pk2 = B.pk2...
				{
					if(i!= 0)
						strBuild.append(" and ");
					strBuild.append("COALESCE(A.").append(pkCols[i]).append(", \'\') = COALESCE(B.").append(pkCols[i]).append(", \'\')");
				}
				
				//query = strBuild.append(" WHERE A.").append(tblToHistoryCount.get(tableName)[4]).append(" < B.").append(tblToHistoryCount.get(tableName)[4]).append(" AND A.").append(tblToHistoryCount.get(tableName)[5]).append(" <> B.").append(tblToHistoryCount.get(tableName)[4]).append(" - TIMESTAMP '1' ) AND B.Rank_B = A.Rank_A + 1;").toString();	// WHERE A.START_DATE < B.START_DATE AND A.END_DATE <> B.START_DATE -1 AND B.Rank_B = A.Rank_A+1;
				strBuild.append(" WHERE A.").append(tblToHistoryCount.get(tableName)[4]).append(" < B.").append(tblToHistoryCount.get(tableName)[4]).append(" AND SEC > 1 AND B.Rank_B = A.Rank_A + 1");  // WHERE A.START_DATE < B.START_DATE AND A.END_DATE <> B.START_DATE -1 AND B.Rank_B = A.Rank_A+1;
				
				strBuild.append(" GROUP BY ");
				
				for(int i=1; i<=cols.size()+3; ++i)		// GROUP BY 1,2,3...
				{
					if(i != 1)
						strBuild.append(", ");
					strBuild.append(i);
				}
				
				query = strBuild.append(";").toString();	
				
				//query = "Select CURRENT_TIMESTAMP(0) - INTERVAL '1' DAY;";
				
				//System.out.println(query);
				break;
		}
		
		//System.out.println(query);
		JobTypeParser.getHistorytestlogger().info("In function \'getHistoryCaseQuery\', returning \'query\' = " + query);
		return query;
	}
	/**
	 * creates a HashMap (mapping tables to their Primary Keys, PK duplicate count and full row duplicate count)
	 * and populates the HashMap
	 */
	
	public HashMap<String, String[]> populateTblToHistoryMap()
	{
		HashMap<String, String[]> tblToHistoryCount = new HashMap<String, String[]>();

		int tblCount = -1;	// keeps track of which table is being processed, in order to retrieve the correct dup_type from HistoryTestDetail
		
		for(int i=0; i<JobTypeParser.getHistoryVerification().size(); ++i)
		{	
			String tableName = new StringBuilder().append(JobTypeParser.getHistoryVerification().get(i).getDbName()).append(".").append(JobTypeParser.getHistoryVerification().get(i).getTableName()).toString();
			
			// populating HashMap with (Key)[table name], (Value)[PK column names, historyOverlapCount, historyGapCount, historyReverseCount, startDate column, endDate column]
			if(JobTypeParser.getHistoryVerification().get(i).getPkColumn().equalsIgnoreCase("Y"))		// if marked "Y", add the fully-qualified table name, column name, historyOverlapCount, historyGapCount and historyReverseCount to HashMap
			{	
				if(tblToHistoryCount.containsKey(tableName) && !tblToHistoryCount.get(tableName)[0].contains((JobTypeParser.getHistoryVerification().get(i).getColumnName()).toString()) )		// if tblToHistoryCount already contains pk column(s), append this column to previous one, separated by a comma
				{
					String oldValue = tblToHistoryCount.get(tableName)[0];
					tblToHistoryCount.get(tableName)[0] = new StringBuilder().append(oldValue).append(",").append(JobTypeParser.getHistoryVerification().get(i).getColumnName()).toString();
				}
				
				else											// otherwise simply add the incoming data into the HashMap									
				{
					++tblCount;
					tblToHistoryCount.put(tableName, new String [] {JobTypeParser.getHistoryVerification().get(i).getColumnName(), JobTypeParser.getHistoryTestDetail().get(tblCount).getOverlap(), JobTypeParser.getHistoryTestDetail().get(tblCount).getGap(), JobTypeParser.getHistoryTestDetail().get(tblCount).getReverse(), "NA", "NA"});
				}
			}
			
			else if((!JobTypeParser.getHistoryVerification().get(i).getPkColumn().equalsIgnoreCase("Y")) && (!JobTypeParser.getHistoryVerification().get(i).getPkColumn().equalsIgnoreCase("")) && (!JobTypeParser.getHistoryVerification().get(i).getPkColumn().isEmpty()))
			{
				if(tblToHistoryCount.containsKey(tableName) && !tblToHistoryCount.get(tableName)[0].contains((JobTypeParser.getHistoryVerification().get(i).getColumnName()).toString()) )		// if tblToHistoryCount already contains pk column(s), append NA to previous one, separated by a comma
				{
					String oldValue = tblToHistoryCount.get(tableName)[0];
					tblToHistoryCount.get(tableName)[0] = new StringBuilder().append(oldValue).append(",").append("NA").toString();
				}
				
				else											// otherwise simply add the incoming data into the HashMap									
				{
					++tblCount;
					tblToHistoryCount.put(tableName, new String [] {"NA", JobTypeParser.getHistoryTestDetail().get(tblCount).getOverlap(), JobTypeParser.getHistoryTestDetail().get(tblCount).getGap(), JobTypeParser.getHistoryTestDetail().get(tblCount).getReverse(), "NA", "NA"});
				}
			}
			
			else if(JobTypeParser.getHistoryVerification().get(i).getStartTSColumn().equalsIgnoreCase("Y"))
			{
				tblToHistoryCount.get(tableName)[4] = JobTypeParser.getHistoryVerification().get(i).getColumnName();
			}
			
			else if(JobTypeParser.getHistoryVerification().get(i).getEndTSColumn().equalsIgnoreCase("Y"))
			{
				tblToHistoryCount.get(tableName)[5] = JobTypeParser.getHistoryVerification().get(i).getColumnName();
			}
		}
		
		JobTypeParser.getHistorytestlogger().info("In function \'populateTblToHistoryMap\', returning \'tblToHistoryCount\' = " + tblToHistoryCount);
		return tblToHistoryCount;
	}
	
	/** 
	 *	verifies the number of cases specified by 'caseType' parameter
	 */
	
	public void verifyHistoryCases(final String caseType, final HashMap<String, String[]> tblToHistoryCount, Connection conn)
	{
		JobTypeParser.getHistorytestlogger().info("In function \'verifyHistoryCases\', parameters: \'caseType\' = " + caseType + ", \'tblToHistoryCount\' = " + tblToHistoryCount);
		TeradataDataSource tdSource = new TeradataDataSource();
		long startTime = System.currentTimeMillis();
		/*int envColId = JobTypeParser.getReader().getColId("Env_Id", "History Verification", JobTypeParser.getWorkbook());
		int streamColId = JobTypeParser.getReader().getColId("Stream_Id", "History Verification", JobTypeParser.getWorkbook());
		int subStreamColId = JobTypeParser.getReader().getColId("Sub_Stream_Id", "History Verification", JobTypeParser.getWorkbook());*/
		
		String env = "";
		String caseTypeQuery = "";
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		int caseTypeIndex = 0;		// caseTypeIndex will hold the relevant index in the HashMap of the caseType (e.g. 0 for 'Overlap', 1 for 'Gap')
		
		switch (caseType)
		{
			case "Overlap":
				caseTypeIndex = 1;
				break;
			case "Gap":
				caseTypeIndex = 2;
				break;
			case "Reverse":
				caseTypeIndex = 3;
				break;
			default:
				JobTypeParser.getLogger().error("case type \'" + caseType + "\' not supported. Aborting...");
				return;
		}
		
		int i=0;
		
		String summaryStatus = "Unsuccessful";
		
		for(String table : tblToHistoryCount.keySet())
		{	
			boolean cont = false;
			
			JobTypeParser.getHistorytestlogger().debug("Tablename: " + table + ", CaseType: " + caseType);
			
			if(table.equals(".") || table.equals("") || table.isEmpty()/* || table.startsWith(".") || table.endsWith(".")*/)	// check if database names can begin with "."
			{
				try {
					throw new MissingInformationException(JobTypeParser.class, new Exception(), "Table name and/or database name not found in input file. Please check columns \'DB_Name\' and \'Table_Name\' in sheet \'History Verification\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				} catch (MissingInformationException e) {
				}
				continue;
			}
			env = JobTypeParser.getHistoryVerification().get(i).getEnvId();
			
			if(tblToHistoryCount.get(table)[caseTypeIndex].equalsIgnoreCase("Y"))
			{
				/*JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------");
				JobTypeParser.getLogger().info("|				Running Module \'HISTORY " + caseType.toUpperCase() + " VERIFICATION\'		|");
				JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------");*/
				
				JobTypeParser.getHistorytestlogger().debug("Connection made");
				
				if(!tdSource.objectExists(table, conn))
				{
					try {
						throw new ObjectNotFoundException(DuplicateRecordTestAutomator.class, new Exception(), "Error: Object \'" + table + "\' does not exist - Module \'History " + caseType + " Verification\' cannot be executed");
					} catch (ObjectNotFoundException e) {
					}
					cont = true;
					continue;
				}
				
				for(String col : tblToHistoryCount.get(table)[0].split(","))	// check for column existence
				{
					if(!tdSource.columnExists(table.split("\\.")[0], table.split("\\.")[1], col, conn))
					{
						try {
							throw new ObjectNotFoundException(DuplicateRecordTestAutomator.class, new Exception(), "Error: Column \'" + col + "\' does not exist in object \'" + table + "\' - Module \'History " + caseType + " Verification\' cannot be executed");
						} catch (ObjectNotFoundException e) {
						}
						cont = true;
						continue;
					}
				}
				
				if(cont)
				{
					cont = false;
					continue;
				}
				
				summaryStatus = "Unsuccessful";
				
				String testCaseStatus = "";
				int testCaseCount = 0;
				
				caseTypeQuery = getHistoryCaseQuery(caseType, table, tblToHistoryCount);		// get relevant query for current Case Type (Overlap, Gap or Reverse)
				
				JobTypeParser.getHistorytestlogger().info("Query generated for History " + caseType + ": " + caseTypeQuery);
				
				if(caseTypeQuery.equals(""))						// if query generation encountered an error and returned an empty string, move to the next table name
					continue;
				
				else if(caseTypeQuery.startsWith("Invalid"))
				{
					try {
						throw new InvalidInformationException(JobTypeParser.class, new Exception(), caseTypeQuery + ". Module \'History " + caseType + " Verification\' cannot be executed for object \'" + table + "\'.");
					} catch (InvalidInformationException e1) {
					}
					continue;
				}
				
				try {
					ps = conn.prepareStatement("SELECT COUNT(*) FROM (" + caseTypeQuery.substring(0, (caseTypeQuery.length()-1)) + ") X;");		// execute the query generated for this specific history case type
					rs = ps.executeQuery();
					
					if(rs.next())
						testCaseCount = rs.getInt(1);
					
					testCaseStatus = (testCaseCount > 0) ? "Failed" : "Passed";		// test Case Status depends on whether or not the ResultSet returned any rows
					
					String insertionQuery = new StringBuilder("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Hh_Rslt (stream_id, sub_stream_id, test_cycle_id, test_type_cd, test_status, table_name, history_type_count, script_text, business_date, user_id, env_id, execution_timestamp) values (").append(JobTypeParser.getHistoryVerification().get(i).getStreamId()).append(", ").append(JobTypeParser.getHistoryVerification().get(i).getSubStreamId()).append(", \'").append(env).append("_").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(ConfigurationManager.getInstance().getAppConfig().getHistoryCase(caseType.toLowerCase())).append("\', \'").append(testCaseStatus).append("\', \'").append(table).append("\', ").append(testCaseCount).append(", '").append(caseTypeQuery.replace("'","''")).append("', \'").append(ConfigurationManager.getInstance().getUserConfig().getBusinessDate()).append("\', \'").append(ConfigurationManager.getInstance().getAppConfig().getUserId()).append("\', \'").append(env).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\')").toString();
					
					JobTypeParser.addToBatch(JobTypeParser.getHistoryBatchStatements(), insertionQuery);
					
					summaryStatus = "Successful";
					++numOfHHTables;
				} catch (SQLException e) {
					//e.printStackTrace();
					if(e.getSQLState().equals("HY000"))
					{
						try {
							throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".summary_tbl\' or object \'" + ApplicationDatabaseStructure.getInstance().getDbName() + ".Hh_Rslt\'");
						} catch (AccessViolationException e1) {
						}
					}
					else
					{
						QueryGenerator.logSQLErrors(e, "Hh_Rslt");
						summaryStatus = "Unsuccessful";
					}
				} catch(Exception e) {
					JobTypeParser.getHistorytestlogger().error("Exception encountered in Module History Verification");
					JobTypeParser.getHistorytestlogger().error("Exception is: ", e);
				}
			}
			
			else
			{				
				if(!tblToHistoryCount.get(table)[caseTypeIndex].equalsIgnoreCase("N"))		// if the command entered in Metadata sheet is 'N', do not throw invalid information exception
				{
					try {
						throw new InvalidInformationException(JobTypeParser.class, new Exception(), "Invalid command \'" + tblToHistoryCount.get(table)[caseTypeIndex] + "\' entered at row \'" + (i+2) + "\' in sheet \'History Verification\'. Module \'History " + caseType + " Verification\' cannot be executed for table \'" + table + "\'.");
					} catch (InvalidInformationException e1) {
					}
				}
				
				summaryStatus = "Not Run";
			}
			
			try {		
				QueryGenerator.insertIntoSummary(i, ConfigurationManager.getInstance().getAppConfig().getHistoryCase(caseType.toLowerCase()), caseType, summaryStatus, table, "NA", JobTypeParser.getHistoryVerification().get(i).getEnvId());
				
				if((i == (tblToHistoryCount.size()-1) ) && numOfHHTables != 0)
				{
					//JobTypeParser.getLogger().info("Module \'History " + caseType + " Verification\' completed successfully for " + numOfHHTables + " tables");
					long endTime = System.currentTimeMillis();
					
					Validator.printModuleCompletionPrompt("History " + caseType + " Verification", startTime, endTime, summaryStatus, numOfHHTables);
					numOfHHTables = 0;
				}
				
			} catch(SQLException e) {
				JobTypeParser.getHistorytestlogger().debug("Duplicate primary key in summary_tbl");
			} catch(Exception e) {
				JobTypeParser.getHistorytestlogger().error("Exception encountered in Module History Verification");
				JobTypeParser.getHistorytestlogger().error("Exception is: ", e);
			} 
			++i;	
		}
		
		try {
			JobTypeParser.executeBatchStatements(JobTypeParser.getHistoryBatchStatements());
		} catch (SQLException e) {
			JobTypeParser.getHistorytestlogger().error("Exception encountered while executing History batch inserts");
			JobTypeParser.getHistorytestlogger().error("Exception is: ", e);
		} catch(Exception e) {
			JobTypeParser.getHistorytestlogger().error("Exception encountered in Module History Verification");
			JobTypeParser.getHistorytestlogger().error("Exception is: ", e);
		} 
	}
	
	/**
	 * Overall wrapper for running History Verification Tests
	 */
	
	@Override
	public Void call() throws Exception 
	{
		HashMap<String, String[]> tblToHistoryCount = populateTblToHistoryMap();
		
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'HISTORY VERIFICATION\'			|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");
		
		Connection conn = null;
		
		try {
			JobTypeParser.getHistorytestlogger().debug("getting connection");
			conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
			JobTypeParser.getHistorytestlogger().debug("got connection");
			
			for(String caseType : historyCaseTypes)
				verifyHistoryCases(caseType, tblToHistoryCount, conn);
		} catch (Exception e){
			JobTypeParser.getHistorytestlogger().error("Exception in History Test: " + e);
		} finally {
			if(conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					JobTypeParser.getHistorytestlogger().error("Connection Closing encountered an exception");
				}
		}
		//System.out.println("Exiting from history test");
		return null;
	}
}
