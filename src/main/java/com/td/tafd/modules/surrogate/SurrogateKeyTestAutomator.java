/**
 * 
 */
package com.td.tafd.modules.surrogate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.SurrogateKey;

/**
 * @author kt186036
 *
 */
public class SurrogateKeyTestAutomator implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfSKTables;
	
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
	
	public SurrogateKeyTestAutomator()
	{
		tdSource = new TeradataDataSource();
		numOfSKTables = 0;
	}
	
	/**
	 * Generates and returns the Surrogate Key Verification query
	 * @param surrKey - SurrogateKey class object containing relevant information
	 * @return Surrogate Key Verification query string
	 */
	
	public String getSurrogateKeyVerificationQuery(SurrogateKey surrKey)
	{
		String surrKeyTbl = surrKey.getSurrKeyDbName() + "." + surrKey.getSurrKeyTblName();
		String sourceTbl = surrKey.getSourceDbName() + "." + surrKey.getSourceTblName();
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("SELECT").append("\n")
				.append("(").append("\n")
				.append("SELECT COUNT (DISTINCT ").append(surrKey.getSurrogateKeyCol()).append(") FROM ").append(surrKeyTbl).append("\n")
				.append(")")
				.append(" AS SURR_TOTAL_COUNT ").append("\n")					// 1.
				.append(",").append("\n")
				.append("(").append("\n")
				.append("SELECT COUNT (*) FROM \n(\nSELECT DISTINCT ").append(surrKey.getNaturalKeyCols()).append(" FROM ").append(sourceTbl).append("\n")
				.append(") A )")
				.append("AS SRC_TOTAL_COUNT").append("\n")						// 2.
				.append(",").append("\n")
				.append("(").append("\n")
				.append("SELECT COUNT(*) FROM ").append(sourceTbl).append("\n")
				.append("INNER JOIN").append("\n")
				.append(surrKeyTbl).append(" B").append("\n")
				.append("ON").append("\n")
				.append("(").append(surrKey.getNaturalKeyFunction()).append(")").append(" = B.").append(surrKey.getConcatenatedNaturalKeyCol()).append("\n")		//COMPARING THE COLUMNS FROM SRC AND SURROGATE TABLES BASED ON CONCATENATION POLICY ENTERED BY USER
				.append(") AS MATCHING_NATURAL_KEYS").append("\n")				// 3. NATURAL KEYS PRESENT IN BOTH SOURCE AND SURROGATE TABLE
				.append(",").append("\n")
				.append("(").append("\n")
				.append("SURR_TOTAL_COUNT - MATCHING_NATURAL_KEYS").append("\n")
				.append(") AS SURR_KEYS_WITHOUT_SOURCE_KEYS").append("\n")		// 4.
				.append(",").append("\n")
				.append("(").append("\n")
				.append("SRC_TOTAL_COUNT - MATCHING_NATURAL_KEYS").append("\n")
				.append(") AS SRC_KEYS_WITHOUT_SURR_KEYS").append("\n")			// 5.
				.append(",").append("\n")
				.append("(").append("\n")
				.append("SELECT COUNT (*) FROM (")
				.append("SELECT DISTINCT A.").append(surrKey.getSurrogateKeyCol()).append(", A.").append(surrKey.getConcatenatedNaturalKeyCol()).append(" FROM ").append(surrKeyTbl).append(" A").append("\n")
				.append("INNER JOIN ").append(surrKeyTbl).append(" B").append("\n")
				.append("ON").append("\n")
				.append("A.").append(surrKey.getSurrogateKeyCol()).append(" = B.").append(surrKey.getSurrogateKeyCol()).append("\n")
				.append("WHERE").append("\n")
				.append("A.").append(surrKey.getConcatenatedNaturalKeyCol()).append(" <> B.").append(surrKey.getConcatenatedNaturalKeyCol()).append("\n")
				.append(") X) AS MULTIPLE_SRC_KEYS").append("\n")					// 6.
				.append(",").append("\n")
				.append("(").append("\n")
				.append("SELECT COUNT (*) FROM (")
				.append("SELECT DISTINCT A.").append(surrKey.getSurrogateKeyCol()).append(", A.").append(surrKey.getConcatenatedNaturalKeyCol()).append(" FROM ").append(surrKeyTbl).append(" A").append("\n")
				.append("INNER JOIN ").append(surrKeyTbl).append(" B").append("\n")
				.append("ON").append("\n")
				.append("A.").append(surrKey.getConcatenatedNaturalKeyCol()).append(" = B.").append(surrKey.getConcatenatedNaturalKeyCol()).append("\n")
				.append("WHERE").append("\n")
				.append("A.").append(surrKey.getSurrogateKeyCol()).append(" <> B.").append(surrKey.getSurrogateKeyCol()).append("\n")
				.append(") X) AS MULTIPLE_SURR_KEYS;");								// 7.
		
		//System.out.println(builder.toString());
		return builder.toString();
	}
	
	public String getInsertionQuery(ResultSet rs, SurrogateKey surrogateKeyInfo, String scriptText)
	{
		StringBuilder builder = new StringBuilder();
		
		int multipleSourceKeysCount = 0;
		int multipleSurrKeysCount = 0;
		int sourceKeysWithoutSurrKeysCount = 0;
		int surrKeysWithoutSourceKeysCount = 0;
		
		try {
			rs.next();
			multipleSourceKeysCount = rs.getInt(6);
			multipleSurrKeysCount = rs.getInt(7);
			
			sourceKeysWithoutSurrKeysCount = rs.getInt(5);
			sourceKeysWithoutSurrKeysCount = (sourceKeysWithoutSurrKeysCount < 0) ? 0 : sourceKeysWithoutSurrKeysCount;
			
			surrKeysWithoutSourceKeysCount = rs.getInt(4);
			surrKeysWithoutSourceKeysCount = (surrKeysWithoutSourceKeysCount < 0) ? 0 : surrKeysWithoutSourceKeysCount;
			
		} catch (SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getSurrogatekeytestlogger().error("Exception in SurrogateKeyTestAutomator: " + e);
		} finally {
			try {
				if(rs != null)
					rs.close();
			} catch(SQLException e) {
				JobTypeParser.getSurrogatekeytestlogger().error("Exception in SurrogateKeyTestAutomator: " + e);
			}
		}
		
		String status = (multipleSourceKeysCount == 0 && multipleSurrKeysCount == 0 && sourceKeysWithoutSurrKeysCount == 0 && surrKeysWithoutSourceKeysCount== 0) ? "Passed" : "Failed";
		
		builder.append("INSERT INTO ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Surrogate_Key_Rslt (stream_id, sub_stream_id, test_cycle_id, test_type_cd, test_status, src_db_name, src_tbl_name, surr_key_db_name, surr_key_tbl_name, multiple_src_keys, multiple_surr_keys, src_keys_without_surr, surr_keys_without_src, script_text, business_date, user_id, env_id, execution_timestamp) values (").append(surrogateKeyInfo.getStreamId()).append(", ").append(surrogateKeyInfo.getSubStreamId()).append(", \'").append(surrogateKeyInfo.getEnvId()).append("_").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', ").append(ConfigurationManager.getInstance().getAppConfig().getSurrogateKeyTest()).append(", \'").append(status).append("\', \'").append(surrogateKeyInfo.getSourceDbName()).append("\', \'").append(surrogateKeyInfo.getSourceTblName()).append("\', \'").append(surrogateKeyInfo.getSurrKeyDbName()).append("\', \'").append(surrogateKeyInfo.getSurrKeyTblName()).append("\', ").append(multipleSourceKeysCount).append(", ").append(multipleSurrKeysCount).append(", ").append(sourceKeysWithoutSurrKeysCount).append(", ").append(surrKeysWithoutSourceKeysCount).append(", \'").append(scriptText).append("\', \'").append(ConfigurationManager.getInstance().getUserConfig().getBusinessDate()).append("\', ").append(1).append(", \'").append(surrogateKeyInfo.getEnvId()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\');");
		
		//System.out.println(builder.toString());
		
		return builder.toString();
	}
	
	public Void call()
	{
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
				"\t\t\t    |				Running Module \'SURROGATE KEY VERIFICATION\'			|\n" + 
				"\t\t\t    --------------------------------------------------------------------------------------");
		
		long startTime = System.currentTimeMillis();
		String summaryStatus = "Unsuccessful";
		
		Connection conn = null;
		
		try {
			conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
		} catch (SQLException e) {
			JobTypeParser.getSurrogatekeytestlogger().error("Connection getting exception: " + e);
			//e.printStackTrace();
		}
		
		// get input values from relevant object in JobTypeParser
		List<SurrogateKey> surrogateKeyInfoList = JobTypeParser.getSurrogateKeyList();
		
		int i=0;
		
		for(SurrogateKey surrogateKeyInfo : surrogateKeyInfoList)
		{	
			//summaryStatus = "Unsuccessful"; // reset summary status
			
			if(surrogateKeyInfo.getSourceTblName().equals("") || surrogateKeyInfo.getSourceTblName() == null)
				continue;
			
			//getSurrogateKeyVerificationQuery(surrogateKeyInfo);
			
			// ----- VALIDATION -----

			if(!tdSource.objectExists((surrogateKeyInfo.getSourceDbName() + "." + surrogateKeyInfo.getSourceTblName()), conn))	// validate source_table presence
			{
				JobTypeParser.getLogger().error("Source table \'" + (surrogateKeyInfo.getSourceDbName() + "." + surrogateKeyInfo.getSourceTblName()) + "\' does not exist. Module \'Surrogate Key Verification\' cannot be executed for this input");
				continue;
			}
			
			if(!tdSource.objectExists((surrogateKeyInfo.getSurrKeyDbName() + "." + surrogateKeyInfo.getSurrKeyTblName()), conn)) // validate surrogate_key_table presence
			{
				JobTypeParser.getLogger().error("Surrogate Key table \'" + (surrogateKeyInfo.getSurrKeyDbName() + "." + surrogateKeyInfo.getSurrKeyTblName()) + "\' does not exist. Module \'Surrogate Key Verification\' cannot be executed for this input");
				continue;
			}
			
			String [] naturalKeys = surrogateKeyInfo.getNaturalKeyCols().split(",");	
			boolean allKeysPresent = true;
			
			for(String naturalKey : naturalKeys)	// validate presence of all natural keys in source table
			{
				if(!tdSource.columnExists(surrogateKeyInfo.getSourceDbName(), surrogateKeyInfo.getSourceTblName(), naturalKey, conn))
				{
					JobTypeParser.getLogger().error("Column \'" + naturalKey + "\' does not exist in Source Table \'" + (surrogateKeyInfo.getSourceDbName() + "." + surrogateKeyInfo.getSourceTblName()) + "\'. Module \'Surrogate Key Verification\' cannot be executed for this input");
					allKeysPresent = false;
					break;
				}
			}
			 
			if(!allKeysPresent)		// if any column does not exist, skip to next input
				continue;
			
			if(!tdSource.columnExists(surrogateKeyInfo.getSurrKeyDbName(), surrogateKeyInfo.getSurrKeyTblName(), surrogateKeyInfo.getSurrogateKeyCol(), conn))	// validate surrogate key presence in surrogate key table
			{
				JobTypeParser.getLogger().error("Column \'" + surrogateKeyInfo.getSurrogateKeyCol() + "\' does not exist in Surrogate Key Table \'" + (surrogateKeyInfo.getSurrKeyDbName() + "." + surrogateKeyInfo.getSurrKeyTblName()) + "\'. Module \'Surrogate Key Verification\' cannot be executed for this input");
				continue;
			}
			
			if(!surrogateKeyInfo.getConcatenatedNaturalKeyCol().equals("") && !(surrogateKeyInfo.getConcatenatedNaturalKeyCol() == null))	// validate concatenated natural key column (if mentioned in input MTD sheet) presence in surrogate key table
			{
				if(!tdSource.columnExists(surrogateKeyInfo.getSurrKeyDbName(), surrogateKeyInfo.getSurrKeyTblName(), surrogateKeyInfo.getConcatenatedNaturalKeyCol(), conn))
				{
					JobTypeParser.getLogger().error("Column \'" + surrogateKeyInfo.getConcatenatedNaturalKeyCol() + "\' does not exist in Surrogate Key Table \'" + (surrogateKeyInfo.getSurrKeyDbName() + "." + surrogateKeyInfo.getSurrKeyTblName()) + "\'. Module \'Surrogate Key Verification\' cannot be executed for this input");
					continue;
				}
			}
			
			// ----- CORE MODULE WORKING -----
			
			try { 	
				String query = getSurrogateKeyVerificationQuery(surrogateKeyInfo);	// build surrogate key verification query

				PreparedStatement stmt = conn.prepareStatement(query);	// execute query
				ResultSet rs = stmt.executeQuery();

				// ----- RECORDING THE RESULTS INTO DATABASE (BOTH RESULT TABLE AND SUMMARY TABLE) -----
				
				String insertionQuery = getInsertionQuery(rs, surrogateKeyInfo, query.replace("\'", "\'\'"));	// using ResultSet of query execution, build an insertion query
				
				JobTypeParser.addToBatch(JobTypeParser.getSurrogateKeyTestBatchStatements(), insertionQuery);
				
				summaryStatus = "Successful";	// set summaryStatus
				
				++numOfSKTables;		// increment number of Surrogate
				
				QueryGenerator.insertIntoSummary(i, ConfigurationManager.getInstance().getAppConfig().getSurrogateKeyTest(), "", summaryStatus, (surrogateKeyInfo.getSourceDbName() + "." + surrogateKeyInfo.getSourceTblName()), (surrogateKeyInfo.getSurrKeyDbName() + "." + surrogateKeyInfo.getSurrKeyTblName()), surrogateKeyInfo.getEnvId());	// add to summary table batch insertion statements

			} catch(SQLException e) {
				//e.printStackTrace();
				JobTypeParser.getSurrogatekeytestlogger().error("Exception in SurrogateKeyTestAutomator: " + e);
			}
			
			++i;
		}
		
		try {
			JobTypeParser.executeBatchStatements(JobTypeParser.getSurrogateKeyTestBatchStatements());	// execute the batch of surrogate Key Verification Test result table insertion statements
		} catch (SQLException e) {
			//e.printStackTrace();
			JobTypeParser.getSurrogatekeytestlogger().error("Exception in SurrogateKeyTestAutomator: " + e);
		} finally {
			if(conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					JobTypeParser.getSurrogatekeytestlogger().error("Exception in SurrogateKeyTestAutomator: " + e);
				}
		}
		
		long endTime = System.currentTimeMillis();
		Validator.printModuleCompletionPrompt("Surrogate Key Verification", startTime, endTime, summaryStatus, numOfSKTables);
	
		return null;
	}
}
