
package com.td.tafd.db;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Sheet;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.constants.ConstantsInterface;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.parsers.excel.ExcelReader;
import com.td.tafd.vo.ColumnMapping;
import com.td.tafd.vo.ConfigParams;
import com.td.tafd.vo.DuplicateDataVerification;
import com.td.tafd.vo.HistoryTestDetail;
import com.td.tafd.vo.HistoryVerification;
import com.td.tafd.vo.MinusTest;
import com.td.tafd.vo.MinusTestColumnMapping;
import com.td.tafd.vo.PreliminaryChecks;
import com.td.tafd.vo.RIVerification;
import com.td.tafd.vo.ReleasePackageInfo;
import com.td.tafd.vo.SurrogateKey;
import com.td.tafd.vo.TableAndDuplicateTypeInfo;
import com.td.tafd.vo.TechnicalColumnInfo;
import com.td.tafd.vo.TestScriptExecutionInfo;

public class ApplicationDatabaseStructure implements ConstantsInterface
{
	private static ApplicationDatabaseStructure appDb = null;
	
	private ApplicationDatabaseStructure() {		
	}
	
	/** 
	 * Returns the instance of ApplicationDatabaseStructure class, assigning it before returning if the object is null
	 */
	
	public static ApplicationDatabaseStructure getInstance(){
		if(appDb == null) {
			appDb = new ApplicationDatabaseStructure();
		}
		
		return appDb;
	}
	
	public boolean databaseExists(String dbName) {
		JobTypeParser.getApplicationlogger().info("In function \'databaseExists\', parameter: \'dbName\' = " + dbName);
		boolean exists = false;
		
		try {	
			try {
				Class.forName("com.teradata.jdbc.TeraDriver");
			} catch (ClassNotFoundException e) {
			}
			String url = "jdbc:teradata://" + ConfigurationManager.getInstance().getUserConfig().getHostname();
			Connection conn = DriverManager.getConnection(url, ConfigurationManager.getInstance().getUserConfig().getUsername(), ConfigurationManager.getInstance().getUserConfig().getPassword());
			PreparedStatement ps = conn.prepareStatement("select databasename from dbc.databasesv where databasename = \'" + dbName + "\';");
			ResultSet rs = ps.executeQuery();
			exists = rs.next();
			rs.close();
			conn.close();
		} catch(SQLException e) {
			JobTypeParser.getApplicationlogger().debug("Exception in ApplicationDatabaseStructure.databaseExists. Exception: " + e);
			if(e.getSQLState().equals("HY000"))
			{
				try {
					throw new AccessViolationException(JobTypeParser.class, new Exception(), "The user \'" + ConfigurationManager.getInstance().getUserConfig().getUsername() + "\' does not have \'insert\' and/or \'update\' rights to database \'" + ApplicationDatabaseStructure.getInstance().getDbName() + "\'");
				} catch (AccessViolationException e1) {
				}
			}
			
			JobTypeParser.getApplicationlogger().debug("Checking for database existence encountered an error");
		}
		
		JobTypeParser.getApplicationlogger().info("In function \'databaseExists\', Returning " + exists);
		return exists;
	}
	
	public String getDbName()			// function getDbName retrieves the database name from the user's input in config.properties
	{
		return ConfigurationManager.getInstance().getUserConfig().getDatabaseName();
	}

	public void setDelimeterMap(Sheet sheet, int i, int delimeter_cd_col_id, int delimeter_type_col_id)
	{
		if(sheet.getRow(i+1).getCell(delimeter_cd_col_id).getNumericCellValue() != 5)
			delimitersMap.put(sheet.getRow(i+1).getCell(delimeter_cd_col_id).getNumericCellValue(), "\\"+sheet.getRow(i+1).getCell(delimeter_type_col_id).getStringCellValue());
		else
			delimitersMap.put(sheet.getRow(i+1).getCell(delimeter_cd_col_id).getNumericCellValue(), "\\\t");
	}
	
	public static boolean isOSWindows()
	{
		return (System.getProperty("os.name").startsWith("Windows"));
	}
	
	/**
	 *  Sets up the required output tables if they do not exist
	 */
	
	public void application_ddl()
	{	
		JobTypeParser.getLogger().info("======================================================================================");
		JobTypeParser.getLogger().info("=                                                                          		=");
		JobTypeParser.getLogger().info("=			Setting up Result and lookup Tables				=");
		JobTypeParser.getLogger().info("=	If this is the first time the tool is being executed, this may take some time	=");
		JobTypeParser.getLogger().info("=                                                                          		=");
		JobTypeParser.getLogger().info("======================================================================================");
		
		StringBuffer bteqBuffer = new StringBuffer();
		
		// logon and database commands
		
		bteqBuffer.append(".logon ").append(ConfigurationManager.getInstance().getUserConfig().getHostname()).append("/").append(ConfigurationManager.getInstance().getUserConfig().getUsername()).append(",").append(ConfigurationManager.getInstance().getUserConfig().getPassword()).append(";\nDatabase ").append(ApplicationDatabaseStructure.getInstance().getDbName()).append(";\n\n");
		
		// check if reset test cycle has been marked true. If so, drop previous summary table
		
		if((ConfigurationManager.getInstance().getUserConfig().getResetTestCycle()).trim().toUpperCase().equals("T"))	
		{	
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Summary_Tbl;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Exec_Status_Check_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Obj_Level_Row_Count_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Script_Level_Row_Count_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Row_Count_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Distinct_Value_Count_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Sum_Avg_Recon_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Min_Max_Recon_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Ri_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Null_Value_Count_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Pk_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Hh_Rslt;").append("\n\n");
			bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Minus_Test_Rslt;").append("\n\n");
		}
		
		// result table DDL commands
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Exec_Status_Check_Rslt(test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status varchar(10), process_type varchar(10), process_name varchar(20), Process_Exec_Start_TS timestamp(6), Process_Exec_End_TS timestamp(6), Process_Exec_Status varchar(10), Records_Inserted varchar(10), Records_Updated varchar(10), Records_Deleted varchar(10), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date varchar(40), user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");	
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Obj_Level_Row_Count_Rslt(test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, object_name varchar(30), db_name varchar(30), object_row_count integer, business_date varchar(40), user_id integer, env_id varchar(20), execution_timestamp timestamp(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Script_Level_Row_Count_Rslt(test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status varchar(10), process_type varchar(10), process_name varchar(20), Process_Exec_Start_TS timestamp(6), Process_Exec_End_TS timestamp(6), Process_Exec_Status varchar(10), Records_Inserted varchar(10), Records_Updated varchar(10), Records_Deleted varchar(10), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date varchar(40), user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");	
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Row_Count_Rslt ,NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_total_count varchar(50), target_total_count varchar(50), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Distinct_Value_Count_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_distinct_count varchar(50), target_distinct_count varchar(50), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Sum_Avg_Recon_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_sum varchar(150), target_sum varchar(150), source_avg varchar(150), target_avg varchar(150), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Min_Max_Recon_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_max varchar(150), target_max varchar(150), source_min varchar(150), target_min varchar(150), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Ri_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, no_of_violations INTEGER, parent_table_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, child_table_name VARCHAR(250), parent_db VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, child_db VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, primary_key VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, foreign_key VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Summary_Tbl (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), test_type_cd varchar(10), test_cycle_id varchar(50), source varchar(50), target varchar(50), metadata_file_input varchar(3), application_output varchar(1), status varchar(15), PRIMARY KEY(test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Null_Value_Count_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_null_count varchar(50), target_null_count varchar(50), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Pk_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10), full_row_status VARCHAR(10), primary_key_status VARCHAR(10), table_name VARCHAR(80), pk_columns VARCHAR(100), full_row_count VARCHAR(20), primary_key_count VARCHAR(20), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Hh_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd varchar(10), test_status VARCHAR(10), table_name varchar(80), history_type_count integer, script_text VARCHAR(1300) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Release_Lookup (release_id varchar(200), test_cycle_id varchar(200));").append("\n\n");	
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Minus_Test_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, db_name_a VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, tbl_name_a VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, db_name_b VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, tbl_name_b VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, a_minus_b VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, b_minus_a VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, row_count_a integer, row_count_b integer, script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Surrogate_Key_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC,  src_db_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, src_tbl_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, surr_key_db_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, surr_key_tbl_name VARCHAR(50) CHARACTER SET LATIN NOT CASESPECIFIC, multiple_src_keys integer, multiple_surr_keys integer, src_keys_without_surr integer, surr_keys_without_src integer, script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		
		// tables for Test Script Execution results of Row Count and Metrics Collection
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".TRN_TST_Row_Count_Rslt ,NO FALLBACK, NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_total_count varchar(50), target_total_count varchar(50), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".TRN_TST_Distinct_Value_Count_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_distinct_count varchar(50), target_distinct_count varchar(50), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".TRN_TST_Sum_Avg_Recon_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_sum varchar(150), target_sum varchar(150), source_avg varchar(150), target_avg varchar(150), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".TRN_TST_Min_Max_Recon_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_max varchar(150), target_max varchar(150), source_min varchar(150), target_min varchar(150), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		bteqBuffer.append("CREATE MULTISET TABLE " + ApplicationDatabaseStructure.getInstance().getDbName() + ".TRN_TST_Null_Value_Count_Rslt ,NO FALLBACK , NO BEFORE JOURNAL, NO AFTER JOURNAL, CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO (test_id integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1 MINVALUE 1 NO CYCLE), stream_id integer, sub_stream_id integer, test_cycle_id varchar(20), test_type_cd integer, test_status VARCHAR(10) CHARACTER SET LATIN NOT CASESPECIFIC, source_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_type_cd integer, target_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_type_cd integer, source_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_path VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, target_column_name VARCHAR(250) CHARACTER SET LATIN NOT CASESPECIFIC, source_null_count varchar(50), target_null_count varchar(50), script_text VARCHAR(1500) CHARACTER SET LATIN NOT CASESPECIFIC, business_date VARCHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC, user_id integer, env_id varchar(20), execution_timestamp TIMESTAMP(6), PRIMARY KEY (test_id));").append("\n\n");
		
		// drop lookup tables commands
		bteqBuffer.append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Source_Type_Lookup;").append("\n\n").append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Target_Type_Lookup;").append("\n\n").append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Env_Lookup;").append("\n\n").append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".File_Delimeter_Lookup;").append("\n\n").append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Stream_Lookup;").append("\n\n").append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Sub_Stream_Lookup;").append("\n\n").append("drop table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Module_Lookup;").append("\n\n");	// drop lookup tables commands
		
		bteqBuffer.append(createMetadataTables());
		
		try {	
			bteqBuffer.append("\n\n\n").append(lookupTableSetup(JobTypeParser.getReader())).append("\n.LOGOFF;\n.quit");	// lookup tables creation and insertion commands
			
			File bteqFile = new File(System.getProperty("user.dir").toString() + JobTypeParser.getFileSeparator() + "application_ddl_setup.btq");
			bteqFile.createNewFile();
			
			PrintWriter bteqWriter = new PrintWriter(bteqFile, "UTF-8");
			
			bteqWriter.println(bteqBuffer.toString());
			
			bteqWriter.close();
			
			executeBteqScript(bteqFile);		// execute the BTEQ script
			
			JobTypeParser.getLogger().info("======================================================================================");
			JobTypeParser.getLogger().info("=                                                                          		=");
			JobTypeParser.getLogger().info("=				Result and lookup Table Setup Completed			=");
			JobTypeParser.getLogger().info("=                                                                          		=");
			JobTypeParser.getLogger().info("======================================================================================");
			
		} catch (IOException e) {
			JobTypeParser.getApplicationlogger().error("Exception in application_ddl(): " + e);
		}
	}
	
	/* -------------------------------------------------------------------------------------
	 * | Function 'lookup_table_setup' creates structures of, and populates, lookup tables |
	 * ------------------------------------------------------------------------------------- */
	
	public String lookupTableSetup(ExcelReader reader)
	{		
		StringBuffer bteqBuffer = new StringBuffer();

		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Source_Type_Lookup (source_type_cd integer, source_type_name varchar(30), source_type_desc varchar(40));").append("\n\n");
		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Target_Type_Lookup (target_type_cd integer, target_type_name varchar(30), target_type_desc varchar(40));").append("\n\n");
		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Env_Lookup (environment_id varchar(20), environment_name varchar(30), environment_desc varchar(40));").append("\n\n");
		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".File_Delimeter_Lookup (file_delimeter_type_cd integer, file_delimeter varchar(30), file_delimeter_desc varchar(40));").append("\n\n");
		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Stream_Lookup (stream_id integer, stream_name varchar(30), stream_desc varchar(40));").append("\n\n");
		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Sub_Stream_Lookup (sub_stream_id integer, sub_stream_name varchar(30), sub_stream_desc varchar(40));").append("\n\n");
		bteqBuffer.append("create set table " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Module_Lookup (module_code integer, module_name varchar(80));").append("\n\n");

		Sheet lookup_sheet = JobTypeParser.getWorkbook().getSheet("Lookup Values");

		int source_cd_col_id = reader.getColId("Source_Type_Cd", "Lookup Values", JobTypeParser.getWorkbook());		// column id's of columns in "Lookup Values" sheet
		int source_type_col_id = reader.getColId("Source_Type_Name", "Lookup Values", JobTypeParser.getWorkbook());
		int source_desc_col_id = reader.getColId("Source_Type_Desc", "Lookup Values", JobTypeParser.getWorkbook());

		if(source_cd_col_id == -1 || source_type_col_id == -1 || source_desc_col_id == -1)		// in case any of the required columns have been deleted, log an error explaining the situation
			JobTypeParser.getLogger().error("Error: \"Source_Type\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{
				if(lookup_sheet.getRow(i+1).getCell(source_cd_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(source_cd_col_id).getCellTypeEnum().equals(CellType.BLANK))		// check for, and ignore, blank entries in Excel File
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Source_Type_Lookup values (" + lookup_sheet.getRow(i+1).getCell(source_cd_col_id).getNumericCellValue() + ", \'" + lookup_sheet.getRow(i+1).getCell(source_type_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(source_desc_col_id).getStringCellValue() + "\');\n");		// insert values into relevant lookup table
			}
		}

		int target_cd_col_id = reader.getColId("Target_Type_Cd", "Lookup Values", JobTypeParser.getWorkbook());
		int target_type_col_id = reader.getColId("Target_Type_Name", "Lookup Values", JobTypeParser.getWorkbook());
		int target_desc_col_id = reader.getColId("Target_Type_Desc", "Lookup Values", JobTypeParser.getWorkbook());

		if(target_cd_col_id == -1 || target_type_col_id == -1 || target_desc_col_id == -1)
			JobTypeParser.getLogger().error("Error: \"Target_Type\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{	
				if(lookup_sheet.getRow(i+1).getCell(target_cd_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(target_cd_col_id).getCellTypeEnum().equals(CellType.BLANK))
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Target_Type_Lookup values (" + lookup_sheet.getRow(i+1).getCell(target_cd_col_id).getNumericCellValue() + ", \'" + lookup_sheet.getRow(i+1).getCell(target_type_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(target_desc_col_id).getStringCellValue() + "\');\n");
			}
		}	

		int env_col_id = reader.getColId("Environment_Id", "Lookup Values", JobTypeParser.getWorkbook());
		int env_name_col_id = reader.getColId("Environment_Name", "Lookup Values", JobTypeParser.getWorkbook());
		int env_desc_col_id = reader.getColId("Environment_Desc", "Lookup Values", JobTypeParser.getWorkbook());

		if(env_col_id == -1 || env_name_col_id == -1 || env_desc_col_id == -1)
			JobTypeParser.getLogger().error("Error: \"Environment\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{	
				if(lookup_sheet.getRow(i+1).getCell(env_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(env_col_id).getCellTypeEnum().equals(CellType.BLANK))
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Env_Lookup values (\'" + lookup_sheet.getRow(i+1).getCell(env_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(env_name_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(env_desc_col_id).getStringCellValue() + "\');\n");
			}
		}

		int delimeter_cd_col_id = reader.getColId("File_Delimiter_Type_Cd", "Lookup Values", JobTypeParser.getWorkbook());
		int delimeter_type_col_id = reader.getColId("File_Delimiter", "Lookup Values", JobTypeParser.getWorkbook());
		int delimeter_desc_col_id = reader.getColId("File_Delimiter_Description", "Lookup Values", JobTypeParser.getWorkbook());

		if(delimeter_cd_col_id == -1 || delimeter_type_col_id == -1 || delimeter_desc_col_id == -1)
			JobTypeParser.getLogger().error("Error: \"File_Delimeter\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{
				if(lookup_sheet.getRow(i+1).getCell(delimeter_cd_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(delimeter_cd_col_id).getCellTypeEnum().equals(CellType.BLANK))
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".File_Delimeter_Lookup values (" + lookup_sheet.getRow(i+1).getCell(delimeter_cd_col_id).getNumericCellValue() + ", \'" + lookup_sheet.getRow(i+1).getCell(delimeter_type_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(delimeter_desc_col_id).getStringCellValue() + "\');\n");

				setDelimeterMap(lookup_sheet, i, delimeter_cd_col_id, delimeter_type_col_id);
			}
		}

		int stream_col_id = reader.getColId("Stream_Id", "Lookup Values", JobTypeParser.getWorkbook());
		int stream_name_col_id = reader.getColId("Stream_Name", "Lookup Values", JobTypeParser.getWorkbook());
		int stream_desc_col_id = reader.getColId("Stream_Desc", "Lookup Values", JobTypeParser.getWorkbook());

		if(stream_col_id == -1 || stream_name_col_id == -1 || stream_desc_col_id == -1)
			JobTypeParser.getLogger().error("Error: \"Stream\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{	
				if(lookup_sheet.getRow(i+1).getCell(stream_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(stream_col_id).getCellTypeEnum().equals(CellType.BLANK))
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Stream_Lookup values (" + lookup_sheet.getRow(i+1).getCell(stream_col_id).getNumericCellValue() + ", \'" + lookup_sheet.getRow(i+1).getCell(stream_name_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(stream_desc_col_id).getStringCellValue() + "\');\n");
			}
		}

		int sub_stream_col_id = reader.getColId("Sub_Stream_Id", "Lookup Values", JobTypeParser.getWorkbook());
		int sub_stream_name_col_id = reader.getColId("Sub_Stream_Name", "Lookup Values", JobTypeParser.getWorkbook());
		int sub_stream_desc_col_id = reader.getColId("Sub_Stream_Desc", "Lookup Values", JobTypeParser.getWorkbook());

		if(sub_stream_col_id == -1 || sub_stream_name_col_id == -1 || sub_stream_desc_col_id == -1)
			JobTypeParser.getLogger().error("Error: \"Sub_Stream\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{	
				if(lookup_sheet.getRow(i+1).getCell(sub_stream_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(sub_stream_col_id).getCellTypeEnum().equals(CellType.BLANK))
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Sub_Stream_Lookup values (" + lookup_sheet.getRow(i+1).getCell(sub_stream_col_id).getNumericCellValue() + ", \'" + lookup_sheet.getRow(i+1).getCell(sub_stream_name_col_id).getStringCellValue() + "\', \'" + lookup_sheet.getRow(i+1).getCell(sub_stream_desc_col_id).getStringCellValue() + "\');\n");
			}
		}

		int module_code_col_id = reader.getColId("Module Code", "Lookup Values", JobTypeParser.getWorkbook());
		int module_name_col_id = reader.getColId("Module Name", "Lookup Values", JobTypeParser.getWorkbook());

		if(module_code_col_id == -1 || module_name_col_id == -1)
			JobTypeParser.getLogger().error("Error: \"Module\" Lookup information incomplete - lookup table cannot be generated. Please check Lookup information in file \"Testing Automation Framework Metadata.xlsx\"");
		else
		{
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{	
				if(lookup_sheet.getRow(i+1).getCell(module_code_col_id) == null)
					continue;

				if(lookup_sheet.getRow(i+1).getCell(module_name_col_id).getCellTypeEnum().equals(CellType.BLANK))
					continue;

				bteqBuffer.append("insert into " + ApplicationDatabaseStructure.getInstance().getDbName() + ".Module_Lookup values (" + lookup_sheet.getRow(i+1).getCell(module_code_col_id).getNumericCellValue() + ", \'" + lookup_sheet.getRow(i+1).getCell(module_name_col_id).getStringCellValue() + "\');\n");
			}
		}

		return bteqBuffer.toString();

	}

	public static void executeBteqScript(File scriptFile)
	{
		File file = null;
		boolean osIsWindows = isOSWindows();
		
		try {
			
			if(osIsWindows)
				file = new File(System.getProperty("user.dir").toString() + "/temp.bat");	// if OS is Windows, create temporary batch file
			else
				file = new File(System.getProperty("user.dir").toString() + "/temp.sh");	// otherwise, create temporary shell script
			
			file.createNewFile();

			String log_folder = System.getProperty("user.dir").toString() + "/output log/";

			PrintWriter writer = new PrintWriter(file, "UTF-8");			// create PrintWriter for writing to newly created batch file
			
			int returnCode = 0;
			
			if(osIsWindows)	// If operating system is windows, execution of application_ddl.bteq will be through batch file
			{
				writer.println("bteq < \"" + scriptFile + "\" > \"" + log_folder + scriptFile.getName() + ".txt\" 2>&1");
				writer.println("exit");				// 'exit' causes terminal window to close after temporary batch file's execution
				writer.close();
				
				Process p = null;

				p = Runtime.getRuntime().exec("cmd /c start \"\" /b /wait \"" + System.getProperty("user.dir").toString() + "/temp.bat\"");	// Running the file to execute commands written to it
				returnCode = p.waitFor();
			}
			
			else	// Otherwise, execution of application_ddl.bteq will be through shell script
			{
				writer.println("#!/bin/bash");
				writer.println("bteq < \"" + scriptFile + "\" > \"" + log_folder + scriptFile.getName() + ".txt\" 2>&1");
				writer.close();

				final ProcessBuilder pb = new ProcessBuilder("/bin/sh", "temp.sh");
				pb.directory(new File(System.getProperty("user.dir").toString()));
				// redirect stdout, stderr, etc
				final Process p = pb.start();
				returnCode = p.waitFor();
			}

			if(returnCode != 0)			// checking for unsuccessful execution
			{
				JobTypeParser.getApplicationlogger().debug("Unsuccessful execution of Application database setup bteq");
			}

			//System.out.println("Deleting file on path: " + file.getPath());
			
			if(file != null)
				file.delete();					// delete the temporary batch file, if it exists
		} catch (FileNotFoundException e) {
			JobTypeParser.getApplicationlogger().error("Exception in executeBteqScript(): " + e);
		} catch (IOException e) {
			JobTypeParser.getApplicationlogger().error("Exception in executeBteqScript(): " + e);
		} catch (InterruptedException e) {
			JobTypeParser.getApplicationlogger().error("Exception in executeBteqScript(): " + e);
		} /*finally {
			if(file != null)
				file.delete();					// delete the temporary batch file, if it exists
		}*/
	}

	public String createMetadataTables()
	{
		StringBuffer bteqBuffer = new StringBuffer();
	
		// table creation statements
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Config_Param (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), config_parameter varchar(50), parameter_value varchar(50), category varchar(50));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Row_Count_And_Metrics (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), source_path varchar(20), source_name varchar(20), source_type_cd integer, target_path varchar(20), target_name varchar(20), target_type_cd integer, file_delimeter_type_cd varchar(4), row_count varchar(1), distinct_count varchar(1), sum_avg varchar(1), min_max varchar(1), null_count varchar(1), mapping_specification varchar(4));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Column_Mapping (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), source_path varchar(50), source_name varchar(50), target_path varchar(50), target_name varchar(50), source_column varchar(50), target_column varchar(50), is_numeric varchar(2), auto_detect_datatype varchar(2));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Release_Package_Info (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), env_id varchar(20), db_name varchar(50), object_name varchar(50), object_type varchar(10), status varchar(10), process_name varchar(50), process_type varchar(10));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Dup_Verification (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), db_name varchar(50), table_name varchar(50), pk_column_name varchar(50));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Table_And_Dup_Info (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), db_name varchar(50), table_name varchar(50), dup_type varchar(4), open_records varchar(2), end_date_col varchar(30));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Ri_Verification (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), parent_db_name varchar(50), parent_table_name varchar(50), pk_columns varchar(50), child_db_name varchar(50), child_table_name varchar(50), fk_columns varchar(50));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Hh_Verification (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), db_name varchar(50), table_name varchar(50), column_name varchar(50), pk_column varchar(1), start_ts_column varchar(1), end_ts_column varchar(1));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Hh_Test_Detail (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), db_name varchar(50), table_name varchar(50), reverse varchar(1), overlap varchar(1), gap varchar(1), test_detail varchar(10));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Exception_List (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), exception_column varchar(50));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Minus_Test (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), db_name_a varchar(50), table_name_a varchar(50), db_name_b varchar(50), table_name_b varchar(50), a_minus_b varchar(1), b_minus_a varchar(1), exclude_technical_columns varchar(1), mapping_specification varchar(4));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Minus_Column_Mapping (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), db_name_a varchar(50), table_name_a varchar(50), db_name_b varchar(50), table_name_b varchar(50), column_name_a varchar(50), column_name_b varchar(50));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Surrogate_Key_Test (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), src_db_name varchar(50), src_tbl_name varchar(50), surr_key_db_name varchar(50), surr_key_tbl_name varchar(50), natural_key_cols varchar(100), surr_key_col varchar(50), concat_natural_key_col varchar(50), natural_key_function varchar(1000));\n\n");
		
		bteqBuffer.append("create multiset table ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Test_Script_Execution (test_cycle_id varchar(20), insertion_timestamp TIMESTAMP(6), stream_id varchar(20), sub_stream_id varchar(20), env_id varchar(20), test_script_path varchar(100), test_resultset_db varchar(50), test_resultset_tbl varchar(50), int_layer_db varchar(50), int_layer_tbl varchar(50), int_layer_script varchar(100));\n\n");
		
		bteqBuffer.append(populateMetadataTables());
		
		return bteqBuffer.toString();
	}
	
	public String populateMetadataTables()
	{	
		StringBuffer insertStmts = new StringBuffer();
		
		// insertion statements
		
		for(ConfigParams cp : JobTypeParser.getConfigParameters())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Config_Param values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(cp.getConfigParameter()).append("\', \'").append(cp.getValue()).append("\', \'\');");
		}
		
		insertStmts.append("\n\n");
		
		for(PreliminaryChecks pc : JobTypeParser.getRowCountAndMetrics())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Row_Count_And_Metrics values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(pc.getStreamId()).append("\', \'").append(pc.getSubStreamId()).append("\', \'").append(pc.getEnvId()).append("\', \'").append(pc.getSourcePath()).append("\', \'").append(pc.getSourceName()).append("\', ").append(pc.getSourceTypeCd()).append(", \'").append(pc.getTargetPath()).append("\', \'").append(pc.getTargetName()).append("\', ").append(pc.getTargetTypeCd()).append(", \'").append(pc.getFileDelimiterTypeCd()).append("\', \'").append(pc.getRowCount()).append("\', \'").append(pc.getDistinctValueCount()).append("\', \'").append(pc.getSumAvgValue()).append("\', \'").append(pc.getMinMaxValue()).append("\', \'").append(pc.getNullCount()).append("\', \'").append(pc.getMappingSpecifications()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(ReleasePackageInfo rpi : JobTypeParser.getReleasePackageInfo())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Release_Package_Info values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(rpi.getEnvID()).append("\', \'").append(rpi.getDbName()).append("\', \'").append(rpi.getObjectName()).append("\', \'").append(rpi.getObjectType()).append("\', \'").append(rpi.getStatus()).append("\', \'").append(rpi.getProcessName()).append("\', \'").append(rpi.getProcessType()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(DuplicateDataVerification dv : JobTypeParser.getDuplicateData())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Dup_Verification values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(dv.getStreamId()).append("\', \'").append(dv.getSubStreamId()).append("\', \'").append(dv.getEnvId()).append("\', \'").append(dv.getDbName()).append("\', \'").append(dv.getTableName()).append("\', \'").append(dv.getColumnName()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(TableAndDuplicateTypeInfo td : JobTypeParser.getTableAndDupTypeInfo())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Table_And_Dup_Info values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(td.getDbName()).append("\', \'").append(td.getTableName()).append("\', \'").append(td.getDupType()).append("\', \'").append(td.getOpenRecords()).append("\', \'").append(td.getEndDateColumn()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(RIVerification ri : JobTypeParser.getRiVerification())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Ri_Verification values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(ri.getStreamId()).append("\', \'").append(ri.getSubStreamId()).append("\', \'").append(ri.getEnvId()).append("\', \'").append(ri.getParentDbName()).append("\', \'").append(ri.getParentTableName()).append("\', \'").append(ri.getPkColumns()).append("\', \'").append(ri.getChildDbName()).append("\', \'").append(ri.getChildTableName()).append("\', \'").append(ri.getFkColumns()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(HistoryVerification hv : JobTypeParser.getHistoryVerification())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Hh_Verification values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(hv.getStreamId()).append("\', \'").append(hv.getSubStreamId()).append("\', \'").append(hv.getEnvId()).append("\', \'").append(hv.getDbName()).append("\', \'").append(hv.getTableName()).append("\', \'").append(hv.getColumnName()).append("\', \'").append(hv.getPkColumn()).append("\', \'").append(hv.getStartTSColumn()).append("\', \'").append(hv.getEndTSColumn()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(HistoryTestDetail hd : JobTypeParser.getHistoryTestDetail())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Hh_Test_Detail values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(hd.getDbName()).append("\', \'").append(hd.getTableName()).append("\', \'").append(hd.getReverse()).append("\', \'").append(hd.getOverlap()).append("\', \'").append(hd.getGap()).append("\', \'").append(hd.getTestDetail()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(TechnicalColumnInfo tci : JobTypeParser.getExceptionList())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Exception_List values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(tci.getColumnName()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(ColumnMapping cm : JobTypeParser.getColumnMapping())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Column_Mapping values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(cm.getSourceDb()).append("\', \'").append(cm.getSourceName()).append("\', \'").append(cm.getTargetDb()).append("\', \'").append(cm.getTargetName()).append("\', \'").append(cm.getSourceColumn()).append("\', \'").append(cm.getTargetColumn()).append("\', \'").append(cm.getIsNumeric()).append("\', \'").append(cm.getAutoDetectDatatype()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(MinusTest mt : JobTypeParser.getMinusTestList())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Minus_Test values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(mt.getStreamId()).append("\', \'").append(mt.getSubStreamId()).append("\', \'").append(mt.getEnvId()).append("\', \'").append(mt.getDbNameA()).append("\', \'").append(mt.getTblNameA()).append("\', \'").append(mt.getDbNameB()).append("\', \'").append(mt.getTblNameB()).append("\', \'").append(mt.getaMinusB()).append("\', \'").append(mt.getbMinusA()).append("\', \'").append(mt.getExcludeTechnicalCols()).append("\', \'").append(mt.getMappingSpecification()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		for(MinusTestColumnMapping mtm : JobTypeParser.getMinusTestColumnMapping())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Minus_Column_Mapping values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(mtm.getDbNameA()).append("\', \'").append(mtm.getTblNameA()).append("\', \'").append(mtm.getDbNameB()).append("\', \'").append(mtm.getTblNameB()).append("\', \'").append(mtm.getColumnA()).append("\', \'").append(mtm.getColumnB()).append("\');");
		}
		
		for(SurrogateKey sk : JobTypeParser.getSurrogateKeyList())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Surrogate_Key_Test values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(sk.getStreamId()).append("\', \'").append(sk.getSubStreamId()).append("\', \'").append(sk.getEnvId()).append("\', \'").append(sk.getSourceDbName()).append("\', \'").append(sk.getSourceTblName()).append("\', \'").append(sk.getSurrKeyDbName()).append("\', \'").append(sk.getSurrKeyTblName()).append("\', \'").append(sk.getNaturalKeyCols()).append("\', \'").append(sk.getSurrogateKeyCol()).append("\', \'").append(sk.getNaturalKeyFunction()).append("\', \'").append(sk.getConcatenatedNaturalKeyCol()).append("\');");
		}
		
		for(TestScriptExecutionInfo tsi : JobTypeParser.getTestScriptInfoList())
		{
			insertStmts.append("insert into ").append(ConfigurationManager.getInstance().getUserConfig().getDatabaseName()).append(".Inp_Test_Script_Execution values (\'").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(tsi.getStreamId()).append("\', \'").append(tsi.getSubStreamId()).append("\', \'").append(tsi.getEnvId()).append("\', \'").append(tsi.getTestScript()).append("\', \'").append(tsi.getTestResultsetDb()).append("\', \'").append(tsi.getTestResultsetTbl()).append("\', \'").append(tsi.getIntegratedLayerDb()).append("\', \'").append(tsi.getIntegratedLayerTbl()).append("\', \'").append(tsi.getIntegratedLayerScript()).append("\');");
		}
		
		insertStmts.append("\n\n");
		
		return insertStmts.toString();
	}
}
