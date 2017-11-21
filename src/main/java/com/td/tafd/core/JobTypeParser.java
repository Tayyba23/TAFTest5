/*
 * @(#)JobTypeParser.java 1.0 03/30/2017
 * Copyright (c) 2016-2017
 */

package com.td.tafd.core;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.DefaultFileSystem;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.constants.ConstantsInterface;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.exceptions.InvalidInformationException;
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.exceptions.ObjectNotFoundException;
/*import com.td.tafd.licensevalidation.LicenseController;
import com.td.tafd.licensevalidation.LicenseView;*/
import com.td.tafd.modules.dd.DuplicateRecordTestAutomator;
import com.td.tafd.modules.di.DataIntegrityAutomation;
import com.td.tafd.modules.di.DistinctCountTestAutomator;
import com.td.tafd.modules.di.FileDataRetriever;
import com.td.tafd.modules.di.MinMaxTestAutomator;
import com.td.tafd.modules.di.NullCountTestAutomator;
import com.td.tafd.modules.di.RowCountTestAutomator;
import com.td.tafd.modules.di.SumAvgTestAutomator;
import com.td.tafd.modules.dl.DataLoadAutomation;
import com.td.tafd.modules.dl.ExecutionStatusTestAutomator;
import com.td.tafd.modules.dl.ObjectLevelRowCountTestAutomator;
import com.td.tafd.modules.dl.ScriptLevelRowCountTestAutomator;
import com.td.tafd.modules.history.HistoryTestAutomator;
import com.td.tafd.modules.minus.MinusTestAutomator;
import com.td.tafd.modules.rca.RootCauseAnalysis;
import com.td.tafd.modules.ri.RITestAutomator;
import com.td.tafd.modules.surrogate.SurrogateKeyTestAutomator;
import com.td.tafd.parsers.excel.ExcelReader;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.ColumnAndDatatypeInfo;
import com.td.tafd.vo.ColumnMapping;
import com.td.tafd.vo.ConfigParams;
import com.td.tafd.vo.DistinctColumnInfo;
import com.td.tafd.vo.DuplicateDataVerification;
import com.td.tafd.vo.DuplicateRecordData;
import com.td.tafd.vo.ExecutionStatusCheck;
import com.td.tafd.vo.HistoryTestDetail;
import com.td.tafd.vo.HistoryVerification;
import com.td.tafd.vo.LookupValues;
import com.td.tafd.vo.MinusTest;
import com.td.tafd.vo.MinusTestColumnMapping;
import com.td.tafd.vo.PreliminaryChecks;
import com.td.tafd.vo.RIVerification;
import com.td.tafd.vo.ReleasePackageInfo;
import com.td.tafd.vo.SurrogateKey;
import com.td.tafd.vo.TableAndDuplicateTypeInfo;
import com.td.tafd.vo.TechnicalColumnInfo;
import com.td.tafd.vo.TestScriptExecutionInfo;
//import com.td.tma.licsvc.LicenseVO;

/**
 * Description
 * 
 * @author <a href="mailto:kanwal.tariq@teradata.com">Kanwal Tariq</a>
 * @version 1.0
 * @see <a href=”spec.html#section”>Java Spec</a>
 */

public class JobTypeParser implements ConstantsInterface {
	static {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hhmmss");
		System.setProperty("current.date", dateFormat.format(new Date()));
		System.setProperty("log_path", System.getProperty("user.dir")
				.toString() + "/log files/");
	}

	private static final Logger logger = Logger.getLogger("userLogger");
	private static final Logger applicationLogger = Logger.getLogger("appLogger");
	
	private static final Logger duplicateRecorderLogger = Logger.getLogger("duplicateRecordLogger");
	private static final Logger execCheckLogger = Logger.getLogger("ExecCheckLogger");
	private static final Logger objLevelCountLogger = Logger.getLogger("ObjLevelCountLogger");
	private static final Logger scriptLevelLogger = Logger.getLogger("ScriptLevelLogger");
	private static final Logger rowCountLogger = Logger.getLogger("RowCountLogger");
	private static final Logger distinctCountLogger = Logger.getLogger("DistinctCountLogger");
	private static final Logger sumAvgLogger = Logger.getLogger("SumAvgLogger");
	private static final Logger minMaxLogger = Logger.getLogger("MinMaxLogger");
	private static final Logger nullCountLogger = Logger.getLogger("NullCountLogger");
	private static final Logger RICheckLogger = Logger.getLogger("RICheckLogger");
	private static final Logger historyTestLogger = Logger.getLogger("HistoryTestLogger");
	private static final Logger minusTestLogger = Logger.getLogger("MinusTestLogger");
	private static final Logger surrogateKeyTestLogger = Logger.getLogger("SurrogateKeyLogger");

	private static ExcelReader reader; // ExcelReader instance
	private static Workbook wb; // Excel Workbook's programmatic instance

	private static String[] bteq_exts;
	private static String[] fld_exts;
	private static String[] mld_exts;
	private static String[] tpt_exts;

	private static Statement [] rowCountBatchStatements = null;
	private static Statement [] distinctCountBatchStatements = null;
	private static Statement [] sumAvgBatchStatements = null;
	private static Statement [] minMaxBatchStatements = null;
	private static Statement [] riBatchStatements = null;
	private static Statement [] nullValueBatchStatements = null;
	private static Statement [] duplicateBatchStatements = null;
	private static Statement [] historyBatchStatements = null;
	private static Statement [] minusTestBatchStatements = null;
	private static Statement [] surrogateKeyTestBatchStatements = null;
	private static Statement [] summaryBatchStatements = null;
	private static Statement [] scriptExecTestBatchStatements = null;
	
	int env_col_id;
	int stream_col_id;
	int sub_stream_col_id;

	private String environment;

	private static int numOfRecords;
	
	/**
	 * @return the numOfRecords
	 */
	public static int getNumOfRecords() {
		return numOfRecords;
	}
	
	private static List<ConfigParams> configParameters;
	private static List<ReleasePackageInfo> releasePackageInfo;
	private static List<ExecutionStatusCheck> executionStatusCheck;
	private static List<PreliminaryChecks> rowCountAndMetrics;
	private static List<DuplicateDataVerification> duplicateData;
	private static List<TableAndDuplicateTypeInfo> tableAndDupTypeInfo;
	private static List<RIVerification> riVerification;
	private static List<HistoryVerification> historyVerification;
	private static List<HistoryTestDetail> historyTestDetail;
	private static List<TechnicalColumnInfo> exceptionList;
	private static List<MinusTest> minusTestList;
	private static List<LookupValues> lookupValues;
	private static List<ColumnMapping> columnMapping;
	private static List<MinusTestColumnMapping> minusTestColumnMapping;
	private static List<SurrogateKey> surrogateKeyList;
	private static List<TestScriptExecutionInfo> testScriptInfoList;
	
	/**
	 * @return the testScriptInfoList
	 */
	public static List<TestScriptExecutionInfo> getTestScriptInfoList() {
		return testScriptInfoList;
	}
	/**
	 * @param testScriptInfoList the testScriptInfoList to set
	 */
	public static void setTestScriptInfoList(
			List<TestScriptExecutionInfo> testScriptInfoList) {
		JobTypeParser.testScriptInfoList = testScriptInfoList;
	}

	private static final String fs = File.separator;
	
	public static String getFileSeparator() {
		return fs;
	}
	/**
	 * @return the columnMapping
	 */
	public static List<ColumnMapping> getColumnMapping() {
		return columnMapping;
	}

	/**
	 * @param columnMapping the columnMapping to set
	 */
	public static void setColumnMapping(List<ColumnMapping> columnMapping) {
		JobTypeParser.columnMapping = columnMapping;
	}

	/**
	 * @return the configParameters
	 */
	public static List<ConfigParams> getConfigParameters() {
		return configParameters;
	}

	/**
	 * @param configParameters the configParameters to set
	 */
	public void setConfigParameters(List<ConfigParams> configPm) {
		configParameters = configPm;
	}

	/**
	 * @return the releasePackageInfo
	 */
	public static List<ReleasePackageInfo> getReleasePackageInfo() {
		return releasePackageInfo;
	}

	/**
	 * @param releasePackageInfo the releasePackageInfo to set
	 */
	public void setReleasePackageInfo(List<ReleasePackageInfo> releasePkgInfo) {
		releasePackageInfo = releasePkgInfo;
	}

	/**
	 * @return the executionStatusCheck
	 */
	public static List<ExecutionStatusCheck> getExecutionStatusCheck() {
		return executionStatusCheck;
	}

	/**
	 * @param executionStatusCheck the executionStatusCheck to set
	 */
	public void setExecutionStatusCheck(
			List<ExecutionStatusCheck> execStatusCheck) {
		executionStatusCheck = execStatusCheck;
	}

	/**
	 * @return the rowCountAndMetrics
	 */
	public static List<PreliminaryChecks> getRowCountAndMetrics() {
		return rowCountAndMetrics;
	}

	/**
	 * @param rowCountAndMetrics the rowCountAndMetrics to set
	 */
	public void setRowCountAndMetrics(List<PreliminaryChecks> rowCntAndMetrics) {
		rowCountAndMetrics = rowCntAndMetrics;
	}

	/**
	 * @return the duplicateData
	 */
	public static List<DuplicateDataVerification> getDuplicateData() {
		return duplicateData;
	}

	/**
	 * @param duplicateData the duplicateData to set
	 */
	public void setDuplicateData(List<DuplicateDataVerification> dupData) {
		duplicateData = dupData;
	}

	/**
	 * @return the riVerification
	 */
	public static List<RIVerification> getRiVerification() {
		return riVerification;
	}

	/**
	 * @param riVerification the riVerification to set
	 */
	public void setRiVerification(List<RIVerification> riVer) {
		riVerification = riVer;
	}

	/**
	 * @return the historyVerification
	 */
	public static List<HistoryVerification> getHistoryVerification() {
		return historyVerification;
	}

	/**
	 * @param historyVerification the historyVerification to set
	 */
	public void setHistoryVerification(List<HistoryVerification> historyVer) {
		historyVerification = historyVer;
	}

	/**
	 * @return the minusTestColumnMapping
	 */
	public static List<MinusTestColumnMapping> getMinusTestColumnMapping() {
		return minusTestColumnMapping;
	}

	/**
	 * @param minusTestColumnMapping the minusTestColumnMapping to set
	 */
	public static void setMinusTestColumnMapping(
			List<MinusTestColumnMapping> minusTestColumnMapping) {
		JobTypeParser.minusTestColumnMapping = minusTestColumnMapping;
	}

	/**
	 * @return the tableAndDupTypeInfo
	 */
	public static List<TableAndDuplicateTypeInfo> getTableAndDupTypeInfo() {
		return tableAndDupTypeInfo;
	}

	/**
	 * @param tableAndDupTypeInfo the tableAndDupTypeInfo to set
	 */
	public static void setTableAndDupTypeInfo(
			List<TableAndDuplicateTypeInfo> tableAndDupTypeInfo) {
		JobTypeParser.tableAndDupTypeInfo = tableAndDupTypeInfo;
	}

	/**
	 * @return the historyTestDetail
	 */
	public static List<HistoryTestDetail> getHistoryTestDetail() {
		return historyTestDetail;
	}

	/**
	 * @param historyTestDetail the historyTestDetail to set
	 */
	public static void setHistoryTestDetail(List<HistoryTestDetail> historyTestDetail) {
		JobTypeParser.historyTestDetail = historyTestDetail;
	}

	/**
	 * @return the surrogateKeyList
	 */
	public static List<SurrogateKey> getSurrogateKeyList() {
		return surrogateKeyList;
	}

	/**
	 * @param surrogateKeyList the surrogateKeyList to set
	 */
	public static void setSurrogateKeyList(List<SurrogateKey> surrogateKeyList) {
		JobTypeParser.surrogateKeyList = surrogateKeyList;
	}
	
	/**
	 * @return the exceptionList
	 */
	public static List<TechnicalColumnInfo> getExceptionList() {
		return exceptionList;
	}

	/**
	 * @param exceptionList the exceptionList to set
	 */
	public void setExceptionList(List<TechnicalColumnInfo> exceptionLst) {
		exceptionList = exceptionLst;
	}

	/**
	 * @return the lookupValues
	 */
	public static List<LookupValues> getLookupValues() {
		return lookupValues;
	}

	/**
	 * @param lookupValues the lookupValues to set
	 */
	public void setLookupValues(List<LookupValues> lookupVals) {
		lookupValues = lookupVals;
	}
	
	public static Statement [] getSummaryBatchStatements() {
		return summaryBatchStatements;
	}

	public static Statement [] getRowCountBatchStatements() {
		return rowCountBatchStatements;
	}

	public static Statement [] getDistinctCountBatchStatements() {
		return distinctCountBatchStatements;
	}

	public static Statement [] getSumAvgBatchStatements() {
		return sumAvgBatchStatements;
	}

	public static Statement [] getMinMaxBatchStatements() {
		return minMaxBatchStatements;
	}

	public static Statement [] getRiBatchStatements() {
		return riBatchStatements;
	}

	public static Statement [] getNullValueBatchStatements() {
		return nullValueBatchStatements;
	}

	public static Statement [] getDuplicateBatchStatements() {
		return duplicateBatchStatements;
	}

	public static Statement [] getHistoryBatchStatements() {
		return historyBatchStatements;
	}
	
	/**
	 * @return the minusTestBatchStatements
	 */
	public static Statement[] getMinusTestBatchStatements() {
		return minusTestBatchStatements;
	}

	/**
	 * @return the surrogateKeyTestBatchStatements
	 */
	public static Statement[] getSurrogateKeyTestBatchStatements() {
		return surrogateKeyTestBatchStatements;
	}

	/**
	 * @param surrogateKeyTestBatchStatements the surrogateKeyTestBatchStatements to set
	 */
	public static void setSurrogateKeyTestBatchStatements(
			Statement[] surrogateKeyTestBatchStatements) {
		JobTypeParser.surrogateKeyTestBatchStatements = surrogateKeyTestBatchStatements;
	}

	/**
	 * @param minusTestBatchStatements the minusTestBatchStatements to set
	 */
	public static void setMinusTestBatchStatements(
			Statement[] minusTestBatchStatements) {
		JobTypeParser.minusTestBatchStatements = minusTestBatchStatements;
	}
	
	/**
	 * @return the scriptExecTestBatchStatements
	 */
	public static Statement[] getScriptExecTestBatchStatements() {
		return scriptExecTestBatchStatements;
	}
	/**
	 * @param scriptExecTestBatchStatements the scriptExecTestBatchStatements to set
	 */
	public static void setScriptExecTestBatchStatements(
			Statement[] scriptExecTestBatchStatements) {
		JobTypeParser.scriptExecTestBatchStatements = scriptExecTestBatchStatements;
	}
	/**
	 * @return the minusList
	 */
	public static List<MinusTest> getMinusTestList() {
		return minusTestList;
	}

	/**
	 * @param minusList the minusList to set
	 */
	public static void setMinusTestList(List<MinusTest> minusList) {
		JobTypeParser.minusTestList = minusList;
	}

	public static void instantiateAllBatchStatements(Connection conn) throws SQLException {
		
		rowCountBatchStatements = new Statement[1];
		distinctCountBatchStatements = new Statement[1];
		sumAvgBatchStatements = new Statement[1];
		minMaxBatchStatements = new Statement[1];
		riBatchStatements = new Statement[1];
		nullValueBatchStatements = new Statement[1];
		duplicateBatchStatements = new Statement[1];
		historyBatchStatements = new Statement[1];
		minusTestBatchStatements = new Statement[1];
		surrogateKeyTestBatchStatements = new Statement[1];
		summaryBatchStatements = new Statement[1];
		scriptExecTestBatchStatements = new Statement[1];
		
		instantiateBatchStatement(rowCountBatchStatements, conn);
		instantiateBatchStatement(distinctCountBatchStatements, conn);
		instantiateBatchStatement(sumAvgBatchStatements, conn);
		instantiateBatchStatement(minMaxBatchStatements, conn);
		instantiateBatchStatement(riBatchStatements, conn);
		instantiateBatchStatement(nullValueBatchStatements, conn);
		instantiateBatchStatement(duplicateBatchStatements, conn);
		instantiateBatchStatement(historyBatchStatements, conn);
		instantiateBatchStatement(minusTestBatchStatements, conn);
		instantiateBatchStatement(surrogateKeyTestBatchStatements, conn);
		instantiateBatchStatement(summaryBatchStatements, conn);
		instantiateBatchStatement(scriptExecTestBatchStatements, conn);
	}
	
	public static void closeAllBatchStatements() throws SQLException {
		if(rowCountBatchStatements[0] != null)
			rowCountBatchStatements[0].close();
		if(distinctCountBatchStatements[0] != null)
			distinctCountBatchStatements[0].close();
		if(sumAvgBatchStatements[0] != null)
			sumAvgBatchStatements[0].close();
		if(minMaxBatchStatements[0] != null)
			minMaxBatchStatements[0].close();
		if(riBatchStatements[0] != null)
			riBatchStatements[0].close();
		if(nullValueBatchStatements[0] != null)
			nullValueBatchStatements[0].close();
		if(duplicateBatchStatements[0] != null)
			duplicateBatchStatements[0].close();
		if(historyBatchStatements[0] != null)
			historyBatchStatements[0].close();
		if(minusTestBatchStatements[0] != null)
			minusTestBatchStatements[0].close();
		if(surrogateKeyTestBatchStatements[0] != null)
			surrogateKeyTestBatchStatements[0].close();
		if(summaryBatchStatements[0] != null)
			summaryBatchStatements[0].close();
		if(scriptExecTestBatchStatements[0] != null)
			scriptExecTestBatchStatements[0].close();
	}
	
	public static void instantiateBatchStatement(Statement [] batchStatements, Connection conn) throws SQLException {
		batchStatements[0] = conn.createStatement();
	}
	
	public static void addToBatch(Statement [] batchStatements, String sqlStatement) throws SQLException {
		//System.out.println("batchStatements[0]: " + batchStatements[0]);
		batchStatements[0].addBatch(sqlStatement);
	}
	
	public static void executeBatchStatements(Statement [] batchStatements) throws SQLException {
		batchStatements[0].executeBatch();
	}
	
	private static HashMap<String, ColumnAndDatatypeInfo> tableToColumnsMap; // contains columns of every distinct table in the entire workbook 

	public static HashMap<String, ColumnAndDatatypeInfo> getTableToColumnsMap() {
		return tableToColumnsMap;
	}

	// constructor instantiates class's variables
	public JobTypeParser() {
		Logger.getLogger(PropertiesConfiguration.class).setLevel(
				org.apache.log4j.Level.OFF);
		Logger.getLogger(ConfigurationUtils.class).setLevel(
				org.apache.log4j.Level.OFF);
		Logger.getLogger(DefaultFileSystem.class).setLevel(
				org.apache.log4j.Level.OFF);

		applicationLogger.info("\t\t-----------------------------------------------------------------------------"); // a separator to identify logs of each test cycle execution from the previous one
																											 
		applicationLogger.info("\t\t\t\t"
				+ new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss")
						.format(new Date()));
		applicationLogger
				.info("\t\t-----------------------------------------------------------------------------");

		env_col_id = -1;
		stream_col_id = -1;
		sub_stream_col_id = -1;

		logger.info("======================================================================================");
		logger.info("=                                                                          		=");
		logger.info("=			Testing Automation Framework Execution Started			=");
		logger.info("=			  Do not close this window unless prompted			=");
		logger.info("=                                                                          		=");
		logger.info("======================================================================================");

		tableToColumnsMap = new HashMap<String, ColumnAndDatatypeInfo>();

		set_exts(); // call to setExts, which sets up extra configurations for
					// the main application's execution
	}

	public static Logger getLogger() {
		return logger;
	}

	public static Logger getApplicationlogger() {
		return applicationLogger;
	}
	
	public static Logger getDuplicateRecordLogger() {
		return duplicateRecorderLogger;
	}

	/**
	 * @return the execchecklogger
	 */
	public static Logger getExecchecklogger() {
		return execCheckLogger;
	}

	/**
	 * @return the objlevelcountlogger
	 */
	public static Logger getObjlevelcountlogger() {
		return objLevelCountLogger;
	}

	/**
	 * @return the scriptlevellogger
	 */
	public static Logger getScriptlevellogger() {
		return scriptLevelLogger;
	}

	/**
	 * @return the rowcountlogger
	 */
	public static Logger getRowcountlogger() {
		return rowCountLogger;
	}

	/**
	 * @return the distinctcountlogger
	 */
	public static Logger getDistinctcountlogger() {
		return distinctCountLogger;
	}

	/**
	 * @return the sumavglogger
	 */
	public static Logger getSumavglogger() {
		return sumAvgLogger;
	}

	/**
	 * @return the minmaxlogger
	 */
	public static Logger getMinmaxlogger() {
		return minMaxLogger;
	}

	/**
	 * @return the nullcountlogger
	 */
	public static Logger getNullcountlogger() {
		return nullCountLogger;
	}

	/**
	 * @return the richecklogger
	 */
	public static Logger getRichecklogger() {
		return RICheckLogger;
	}

	/**
	 * @return the historytestlogger
	 */
	public static Logger getHistorytestlogger() {
		return historyTestLogger;
	}

	/**
	 * @return the minustestlogger
	 */
	public static Logger getMinustestlogger() {
		return minusTestLogger;
	}
	
	/**
	 * @return the surrogatekeytestlogger
	 */
	public static Logger getSurrogatekeytestlogger() {
		return surrogateKeyTestLogger;
	}

	public static Workbook getWorkbook() {
		return wb;
	}

	public static ExcelReader getReader() {
		return reader;
	}

	public void setMetadataInfo()
	{
		setConfigParameters(reader.getConfigParams(wb.getSheet("Configuration Parameters")));
		setReleasePackageInfo(reader.getPackageInfo(wb.getSheet("Release Package Information")));
		setExecutionStatusCheck(reader.getExecutionStatusCheck(wb.getSheet("Execution Status Check")));
		setRowCountAndMetrics(reader.getPreliminaryChecks(wb.getSheet("Row Count & Metrics Collection")));
		setDuplicateData(reader.getDuplicateData(wb.getSheet("Duplicate Data Verification")));
		setTableAndDupTypeInfo(reader.getTableAndDuplicateTypeInfo(wb.getSheet("Duplicate Data Verification")));
		setRiVerification(reader.getRIList(wb.getSheet("RI Verification")));
		setHistoryVerification(reader.getHistoryDataList(wb.getSheet("History Verification")));
		setHistoryTestDetail(reader.getHistoryTestDetail(wb.getSheet("History Verification")));
		setExceptionList(reader.getTechnicalInfoList(wb.getSheet("Exception List")));
		setLookupValues(reader.getLookUpValues(wb.getSheet("Lookup Values")));
		setColumnMapping(reader.getColumnMapping(wb.getSheet("Row Count & Metrics Collection")));
		setMinusTestColumnMapping(reader.getMinusTestColumnMapping(wb.getSheet("Minus Test")));
		setMinusTestList(reader.getMinusTest(wb.getSheet("Minus Test")));
		setSurrogateKeyList(reader.getSurrogateKey(wb.getSheet("Surrogate Key Verification")));
		setTestScriptInfoList(reader.getTestScriptExecutionInfo(wb.getSheet("Test Script Execution")));
	}
	
	public void verifyMetadataInput()
	{
		applicationLogger.info(getConfigParameters());
		applicationLogger.info(getReleasePackageInfo());
		applicationLogger.info(getExecutionStatusCheck());
		applicationLogger.info(getRowCountAndMetrics());
		applicationLogger.info(getDuplicateData());
		applicationLogger.info(getRiVerification());
		applicationLogger.info(getHistoryVerification());
		applicationLogger.info(getMinusTestList());
		applicationLogger.info(getSurrogateKeyList());
		applicationLogger.info(getExceptionList());
		applicationLogger.info(getLookupValues());
		applicationLogger.info(getSurrogateKeyList());
		applicationLogger.info(getTestScriptInfoList());
	}
	
	/**
	 * Function releaseIdIsPresent returns true if the parameter 'releaseId' is found in Release_Lookup table
	 */
	
	public boolean releaseIdIsPresent(final String releaseId) {
		JobTypeParser.getApplicationlogger().info(
				"In function \'releaseIdIsPresent\', parameters: \'releaseId\' = "
						+ releaseId);
		boolean isPresent = false;

		Connection conn = null;
		try {
			String connurl = "jdbc:teradata://"
					+ ConfigurationManager.getInstance().getUserConfig()
							.getHostname();

			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(connurl, ConfigurationManager
					.getInstance().getUserConfig().getUsername(),
					ConfigurationManager.getInstance().getUserConfig()
							.getPassword());

			PreparedStatement ps1 = conn
					.prepareStatement("select tablename from dbc.tablesv where tablename = \'Release_Lookup\' and databasename = \'"
							+ ApplicationDatabaseStructure.getInstance()
									.getDbName() + "\';");
			ResultSet rs1 = ps1.executeQuery();

			if (!rs1.next())
				isPresent = false;
			else {
				String checkPresence = "select * from "
						+ ApplicationDatabaseStructure.getInstance()
								.getDbName()
						+ ".Release_Lookup where release_id = \'" + releaseId
						+ "\';";
				PreparedStatement ps = conn.prepareStatement(checkPresence);
				ResultSet rs = ps.executeQuery();

				if (rs.next())
					isPresent = true;

				rs.close();
				ps.close();
			}

			conn.close();

		} catch (SQLException e) {

			if (e.getSQLState().equals("HY000")) {
				try {
					throw new AccessViolationException(
							JobTypeParser.class,
							new Exception(),
							"The user \'"
									+ ConfigurationManager.getInstance()
											.getUserConfig().getUsername()
									+ "\' does not have \'select\' rights to object \'"
									+ ApplicationDatabaseStructure
											.getInstance().getDbName()
									+ ".Release_Lookup\'");
				} catch (AccessViolationException e1) {
				}
			}

			else {
				try {
					throw new InvalidInformationException(JobTypeParser.class,
							e, "");
				} catch (InvalidInformationException e1) {
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		JobTypeParser.getApplicationlogger().info(
				"In function \'releaseIdIsPresent\', returning \'" + isPresent
						+ "\'");
		return isPresent;
	}

	/**
	 * Returns true if the parameter 'envId' specified is found in Release_Lookup table
	 */

	public boolean envIdIsPresent(String envId) {
		JobTypeParser.getApplicationlogger().info(
				"In function \'envIdIsPresent\', parameters: \'envId\' = "
						+ envId);
		ArrayList<String> testCycles = new ArrayList<String>();

		boolean isPresent = false;

		Connection conn = null;
		try {
			String connurl = "jdbc:teradata://"
					+ ConfigurationManager.getInstance().getUserConfig()
							.getHostname();

			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(connurl, ConfigurationManager
					.getInstance().getUserConfig().getUsername(),
					ConfigurationManager.getInstance().getUserConfig()
							.getPassword());

			PreparedStatement ps1 = conn
					.prepareStatement("select tablename from dbc.tablesv where tablename = \'Release_Lookup\' and databasename = \'"
							+ ApplicationDatabaseStructure.getInstance()
									.getDbName() + "\';");
			ResultSet rs1 = ps1.executeQuery();

			if (!rs1.next())
				isPresent = false;
			else {
				String checkPresence = "select distinct(test_cycle_id) from "
						+ ApplicationDatabaseStructure.getInstance()
								.getDbName() + ".Release_Lookup;";
				PreparedStatement ps = conn.prepareStatement(checkPresence);
				ResultSet rs = ps.executeQuery();

				while (rs.next())
					testCycles.add(rs.getString(1));

				for (String testCycle : testCycles) {
					if (testCycle.toUpperCase().startsWith(envId.toUpperCase())) // as
																					// the
																					// test_cycle_id
																					// column
																					// follows
																					// the
																					// format
																					// of
																					// env_cycle_count
						isPresent = true;
				}
				rs.close();
				ps.close();
			}

			conn.close();

		} catch (SQLException e) {

			if (e.getSQLState().equals("HY000")) {
				try {
					throw new AccessViolationException(
							JobTypeParser.class,
							new Exception(),
							"The user \'"
									+ ConfigurationManager.getInstance()
											.getUserConfig().getUsername()
									+ "\' does not have \'select\' rights to object \'"
									+ ApplicationDatabaseStructure
											.getInstance().getDbName()
									+ ".Release_Lookup\'");
				} catch (AccessViolationException e1) {
				}
			}

			else {
				try {
					throw new InvalidInformationException(JobTypeParser.class,
							e, "");
				} catch (InvalidInformationException e1) {
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		JobTypeParser.getApplicationlogger().info(
				"In function \'envIdIsPresent\', returning \'" + isPresent
						+ "\'");
		return isPresent;
	}

	/**
	 * Function setTestCycle sets the testCycle according to the requirement
	 */

	public void setTestCycle() {
		ExcelReader rd = new ExcelReader();

		Workbook wbook;
		wbook = rd.getRequiredWorkbook(ConfigurationManager.getInstance()
				.getUserConfig().getInputFilePath(),
				ConfigurationManager.getInstance().getUserConfig()
						.getInputFilePath().endsWith("xls"));
		Sheet sheet = wbook.getSheet("Row Count & Metrics Collection");

		environment = sheet.getRow(1)
				.getCell(rd.getColId("Env_Id", "Row Count & Metrics Collection", wbook))
				.getStringCellValue();

		applicationLogger.info("Initial Environment fetched: " + environment);

		String currentRelease = ConfigurationManager.getInstance()
				.getUserConfig().getReleasePackageId().trim();
		if (!releaseIdIsPresent(currentRelease)) // if new release is to be run,
													// test cycle will be rest
													// to 1.1, and the minor
													// will be reset as well
		{
			if (!envIdIsPresent(environment)) // if a new environment has been
												// introduced in the new release
			{
				try {
					PropertiesConfiguration conf = new PropertiesConfiguration(
							System.getProperty("user.dir").toString()
									+ fs + "src" + fs + "main" + fs + "resources" + fs + "app_config.properties");
					// if(props.getProperty("reset_test_cycle").equalsIgnoreCase("T"))
					conf.setProperty("test_cycle_minor", "0");
					conf.setProperty(
							"test_cycle",
							"1."
									+ (Integer.parseInt(conf
											.getString("test_cycle_minor")) + 1));
					conf.setProperty("test_cycle_minor", (Integer.parseInt(conf
							.getString("test_cycle_minor")) + 1));
					conf.save();
				} catch (ConfigurationException e) {
					e.printStackTrace();
				}
			}

			else // if the same environment is being continued in a new release,
					// update the test cycle by incrementing 1 in the major
			{
				try {
					PropertiesConfiguration conf = new PropertiesConfiguration(
							System.getProperty("user.dir").toString()
									+ fs + "src" + fs + "main" + fs + "resources" + fs + "app_config.properties");
					conf.setProperty("test_cycle_minor", "0");
					conf.setProperty(
							"test_cycle",
							(((int) Double.parseDouble(conf
									.getString("test_cycle")) + 1) + "." + (Integer
									.parseInt(conf
											.getString("test_cycle_minor")) + 1)));
					conf.setProperty("test_cycle_minor", (Integer.parseInt(conf
							.getString("test_cycle_minor")) + 1));
					conf.save();
				} catch (ConfigurationException e) {
					// e.printStackTrace();
				}
			}
		}

		else // the same release is being executed, which is taken to mean the
				// environment has not changed either, as per the assumptions
				// agreed upon
		{
			try {
				PropertiesConfiguration conf = new PropertiesConfiguration(
						System.getProperty("user.dir").toString()
								+ fs + "src" + fs + "main" + fs + "resources" + fs + "app_config.properties");
				if (ConfigurationManager.getInstance().getUserConfig()
						.getResetTestCycle().trim().equalsIgnoreCase("T"))
					conf.setProperty("test_cycle_minor", "0");
				conf.setProperty(
						"test_cycle",
						((int) Double.parseDouble(conf.getString("test_cycle")))
								+ "."
								+ (Integer.parseInt(conf
										.getString("test_cycle_minor")) + 1));
				conf.setProperty(
						"test_cycle_minor",
						(Integer.parseInt(conf.getString("test_cycle_minor")) + 1));
				conf.save();
			} catch (ConfigurationException e) {
				e.printStackTrace();
			}
		}
	}

	public void enterIntoReleaseLookup(String env) {
		TeradataDataSource tdSource = new TeradataDataSource();
		PreparedStatement ps = null;
		Connection conn  = null;
		
		try {
			conn = tdSource.getConnection(ConfigurationManager
					.getInstance().getUserConfig().getHostname());
			String insertIntoReleaseLookup = "Insert into "
					+ ApplicationDatabaseStructure.getInstance().getDbName()
					+ ".Release_Lookup (release_id, test_cycle_id) values (\'"
					+ ConfigurationManager.getInstance().getUserConfig()
							.getReleasePackageId()
					+ "\', \'"
					+ environment
					+ "_"
					+ ConfigurationManager.getInstance().getAppConfig()
							.getTestCycle() + "\');";
			ps = conn
					.prepareStatement(insertIntoReleaseLookup);
			ps.execute();
			
		} catch (SQLException e) {
			if (e.getSQLState().equals("HY000")) {
				try {
					throw new AccessViolationException(
							JobTypeParser.class,
							new Exception(),
							"The user \'"
									+ ConfigurationManager.getInstance()
											.getUserConfig().getUsername()
									+ "\' does not have \'insert\' rights to object \'"
									+ ApplicationDatabaseStructure
											.getInstance().getDbName()
									+ ".Release_Lookup\'");
				} catch (AccessViolationException e1) {
				}
			}

			else {
				try {
					throw new InvalidInformationException(JobTypeParser.class,
							e, "Release_Lookup");
				} catch (InvalidInformationException e1) {
				}
			}
		} finally {
			try {
				if(ps != null)
					ps.close();
				if(conn != null)
					conn.close();
			} catch (SQLException e){
				applicationLogger.error(e);
			}
		}
	}

	/**
	 * Reorders columns of source and target so that they are aligned
	 * In case there is a mismatch, said mismatch is logged into log file and false is returned
	 */

	public static boolean re_order_cols(ArrayList<String> source, ArrayList<String> target, String source_name, String target_name) {
		
		boolean sourceColsLessThanTargetCols = (source.size() <= target.size());
		
		//System.out.println("sourceColsLessThanTargetCols: " + sourceColsLessThanTargetCols);
		
		ArrayList<String> temp = new ArrayList<String>();
		temp.addAll(target);

		target.clear();

		for (String t : temp)	// convert all the columns in the target list to lower case for case-insensitive checking
			target.add(t.toLowerCase());

		temp.clear(); // refreshing temp

		temp.addAll(source);

		source.clear();

		for (String s : temp)	// convert all the columns in the target list to lower case for case-insensitive checking
			source.add(s.toLowerCase());

		temp.clear();

		if(sourceColsLessThanTargetCols)	// if source columns are less in number than target columns, source columns will be considered as the reference point
											// and target will be checked to see if it contains the source's columns
		{
			for (String s : source) {
				if (target.contains(s.toLowerCase())) // if target contains current column, put it in temp in the order it appears in source
					temp.add(s.toLowerCase());

				else // for any column that does not appear in target and is present in source, log error into file and return false
				{
					logger.debug("Error: Source/Target Mismatch - Column \'" + s
							+ "\' of Source Object \'" + source_name
							+ "\' Not found in Target Object \'" + target_name
							+ "\'");
					return false;
				}
			}
			target.clear();
			target.addAll(temp);
		}

		else	// if target columns are less in number than source columns, target columns will be considered as the reference point,
				// and source will be checked to see if it contains the target's columns
		{		
			for (String t : target) {
				if (source.contains(t.toLowerCase())) // if source contains current column, put it in temp in the order it appears in target
					temp.add(t.toLowerCase());

				else // for any column that does not appear in target and is present in source, log error into file and return false
				{
					logger.debug("Error: Source/Target Mismatch - Column \'" + t
							+ "\' of Target Object \'" + target_name
							+ "\' Not found in Source Object \'" + source_name
							+ "\'");
					return false;
				}
			}
			source.clear();
			source.addAll(temp);
		}
		
		// System.out.println("Returning targets: " + target);

		return true;
	}

	public static boolean containsName(final List<DistinctColumnInfo> dciList, final String name)
	{
		for(DistinctColumnInfo dci : dciList)
		{
			if(dci.getName().toLowerCase().equals(name.toLowerCase()))
				return true;
		}
		
		return false;
	}
	
	/**
	 * Overloaded re_order_cols function for ArrayLists of DistinctColumnInfo
	 * Reorders columns of source and target so that they are aligned 
	 * In case there is a mismatch, said mismatch is logged into log file and false is returned
	 */

	public static boolean re_order_cols(List<DistinctColumnInfo> source, List<DistinctColumnInfo> target, String source_name, String target_name) {
		
		boolean sourceColsLessThanTargetCols = (source.size() <= target.size());
		
		List<DistinctColumnInfo> temp = new ArrayList<DistinctColumnInfo>();
		temp.addAll(target);

		target.clear();

		for (DistinctColumnInfo dci : temp)	
		{
			dci.setName(dci.getName().toLowerCase()); // convert all the column names in the target list to lower case for case-insensitive checking
			target.add(dci);
		}

		temp.clear(); // refreshing temp

		temp.addAll(source);

		source.clear();

		for (DistinctColumnInfo dci : temp)
		{
			dci.setName(dci.getName().toLowerCase()); // convert all the column names in the target list to lower case for case-insensitive checking
			source.add(dci);
		}

		temp.clear();

		if(sourceColsLessThanTargetCols)	// if source columns are less in number than target columns, source columns will be considered as the reference point
			// and target will be checked to see if it contains the source's columns
		{
			for (DistinctColumnInfo s : source) {
				s.setName(s.getName().toLowerCase());
				if (containsName(target, s.getName())) // if target contains current column, put it in temp in the order it appears in source
					temp.add(s);

				else // for any column that does not appear in target and is present in source, log error into file and return false
				{
					logger.debug("Error: Source/Target Mismatch - Column \'" + s.getName()
							+ "\' of Source Object \'" + source_name
							+ "\' Not found in Target Object \'" + target_name
							+ "\'");
					return false;
				}
			}

			target.clear();
			target.addAll(temp);
		}
		
		else	// if target columns are less in number than source columns, target columns will be considered as the reference point,
				// and source will be checked to see if it contains the target's columns
		{
			for (DistinctColumnInfo t : target) {
				t.setName(t.getName().toLowerCase());
				if (containsName(source, t.getName())) // if target contains current column, put it in temp in the order it appears in source
					temp.add(t);

				else // for any column that does not appear in target and is present in source, log error into file and return false
				{
					logger.debug("Error: Source/Target Mismatch - Column \'" + t.getName()
							+ "\' of Target Object \'" + target_name
							+ "\' Not found in Source Object \'" + source_name
							+ "\'");
					return false;
				}
			}

			source.clear();
			source.addAll(temp);
		}
		// System.out.println("Returning targets: " + target);

		return true;
	}
	
	public void cleanupTestId() {
		JobTypeParser.getApplicationlogger().info(
				"In function \'cleanupTestId\'");
		Connection conn = null;
		try {
			String connurl = "jdbc:teradata://"
					+ ConfigurationManager.getInstance().getUserConfig()
							.getHostname();

			Class.forName("com.teradata.jdbc.TeraDriver");
			conn = DriverManager.getConnection(connurl, ConfigurationManager
					.getInstance().getUserConfig().getUsername(),
					ConfigurationManager.getInstance().getUserConfig()
							.getPassword());

			String cleanupId = "delete from "
					+ ApplicationDatabaseStructure.getInstance().getDbName()
					+ ".summary_tbl where test_id LIKE \'_%\';";
			PreparedStatement ps = conn.prepareStatement(cleanupId);
			ps.execute();
			ps.close();
			conn.close();

		} catch (SQLException e) {
			JobTypeParser.getApplicationlogger().debug(
					"SQL Exception in function \'cleanupTestId\'");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		JobTypeParser.getApplicationlogger().info(
				"In function \'cleanupTestId\', returning after completion");
	}

	/**
	 * Reads all possible file extensions from the configuration file. These are then assigned to the enum 'extensions'
	 * Extra configuration settings required for the main execution of application are also handled,
	 * such as assigning values to objects of other classes and setting up other class variables
	 */

	private void set_exts() {
		logger.info("======================================================================================");
		logger.info("=                                                                          		=");
		logger.info("=		    Scanning Input Metadata Sheet for Mandatory Fields			=");
		logger.info("=                                                                          		=");
		logger.info("======================================================================================");

		ConfigurationManager.getInstance().getExceptionConfig()
				.setExceptionConfiguration(); // setup Exception Configuration
												// through
												// exception_config.properties
												// file
		ConfigurationManager.getInstance().getUserConfig()
				.setUserConfiguration(); // setup User Level Configurations from
											// config.properties

		String dbName = ConfigurationManager.getInstance().getUserConfig()
				.getDatabaseName();

		if (ConfigurationManager.getInstance().getUserConfig().getHostname()
				.equals("")
				|| ConfigurationManager.getInstance().getUserConfig()
						.getHostname().isEmpty()) {
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Hostname field is empty. Please enter the hostname for the target tables next to field \'Hostname\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (MissingInformationException e) {
			}
			System.exit(0);
			// MISSING INFORMATION
		}

		if (!Validator.getInstance().validateIP(
				ConfigurationManager.getInstance().getUserConfig()
						.getHostname())) // determine whether the entered
											// hostname is valid or not. If not,
											// log error and exit, as execution
											// cannot be carried out without
											// this information
		{
			try {
				throw new InvalidInformationException(
						JobTypeParser.class,
						new Exception(),
						"Invalid hostname - the entered IP address \'"
								+ ConfigurationManager.getInstance()
										.getUserConfig().getHostname()
								+ "\' could not be resolved to a valid hostname. Please check field \'Hostname\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\" and enter a valid hostname.");
			} catch (InvalidInformationException e1) {
			}
			// logger.debug("Invalid hostname - the entered IP address could not be resolved to a valid hostname. Please check config.properties file and enter a valid hostname");
			System.exit(0);
		}

		if (ConfigurationManager.getInstance().getUserConfig().getUsername()
				.equals("")
				|| ConfigurationManager.getInstance().getUserConfig()
						.getUsername().isEmpty()) {
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Username field is empty. Please enter the username (for accessing the target database) next to field \'Username\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (MissingInformationException e) {
			}
			System.exit(0);
		}

		if (ConfigurationManager.getInstance().getUserConfig().getPassword()
				.equals("")
				|| ConfigurationManager.getInstance().getUserConfig()
						.getPassword().isEmpty()) {
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Password field is empty. Please enter the password (for accessing the target database) next to field \'Password\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (MissingInformationException e) {
			}
			System.exit(0);
		}

		if (dbName.equals("") || dbName.isEmpty()) // Database Name field is
													// empty
		{
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Database Field is empty. Please enter a database name next to \'Database\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (MissingInformationException e) {
			}
			System.exit(0);
		}

		String f_path = ConfigurationManager.getInstance().getUserConfig()
				.getInputFilePath();

		if (f_path.equals("") || f_path.isEmpty()) // ensure the input metadata
													// file's path is written in
													// config.properties file.
													// If not present, log error
													// and exit execution as
													// execution cannot be
													// carried out without this
													// information
		{
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Please provide name of Input Parameter file next to \'Input File Path\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (MissingInformationException e) {
			}
			// logger.debug("Error: Missing Information - Please provide name of Input Parameter file next to \'input_file_path\' in file \"config.properties\"");
			System.exit(0);
		}

		if (!(new File(f_path).exists())) // ensure the input metadata file's
											// path in config.properties file is
											// valid. If not, log error and exit
											// execution
		{
			try {
				throw new InvalidInformationException(JobTypeParser.class,
						new Exception(),
						"Invalid file name - Input Parameter file \'" + f_path
								+ "\' does not exist.");
			} catch (InvalidInformationException e1) {
			}
			// logger.debug("Error: Invalid file name - Input Parameter file \'"
			// + f_path + "\' not found");
			System.exit(0);
		}

		if (!Validator.getInstance().validate_input_file(f_path)) // validate whether or not the input metadata file is stable - i.e. there is no missing information such as the absence of a required column or sheet. 
			System.exit(0);					// If there is missing data, exit as execution cannot be carried out without this information

		if (ConfigurationManager.getInstance().getUserConfig()
				.getBusinessDate().equals("")
				|| ConfigurationManager.getInstance().getUserConfig()
						.getBusinessDate().isEmpty()) // Business Date field is empty
		{
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Business date field is empty. Please enter the business date for the execution next to \'Business Date\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (MissingInformationException e) {
			}

			System.exit(0);
			// MISSING INFORMATION
		}

		if (!Validator.getInstance().isDateValid(
				ConfigurationManager.getInstance().getUserConfig()
						.getBusinessDate(), "dd/MM/yyyy")) {
			try {
				throw new InvalidInformationException(
						JobTypeParser.class,
						new Exception(),
						"Invalid Business date \'"
								+ ConfigurationManager.getInstance()
										.getUserConfig().getBusinessDate()
								+ "\' entered, or the format is incorrect. Please enter Business date in the format \'DD/MM/YYYY\' in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\"");
			} catch (InvalidInformationException e1) {
			}
			System.exit(0);
		}

		if (!ConfigurationManager.getInstance().getUserConfig()
				.getResetTestCycle().equals("T")
				&& !ConfigurationManager.getInstance().getUserConfig()
						.getResetTestCycle().equals("F")) {
			try {
				throw new InvalidInformationException(
						JobTypeParser.class,
						new Exception(),
						"Invalid Command \'"
								+ ConfigurationManager.getInstance()
										.getUserConfig().getResetTestCycle()
								+ "\' entered for field Reset Test Cycle in Sheet \"Configuration Parameters\" in file \""
								+ ConfigurationManager.getInstance()
										.getUserConfig().getInputFilePath()
								+ "\". Valid commands are \'T\' and \'F\' ");
			} catch (InvalidInformationException e1) {
			}
		}
		
		reader = new ExcelReader(); // ExcelReader class's instance, which contains all Excel File related functions
		
		wb = reader.getRequiredWorkbook(f_path, f_path.endsWith("xls")); // instantiating Excel Workbook's programmatic reference

		if(!Validator.getInstance().validateAllStreams())
		{
			try {
				//throw new InvalidInformationException(DataIntegrityAutomation.class, new Exception(), "Characters entered in columns \'Stream_Id\' and/or \'Sub_Stream_Id\' in row " + (i+2) + ", in sheet \'Row Count & Metrics Collection\' in Input Metadata File \'" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\' Module \'Row Count\' cannot be executed. Please enter Numeric codes.");
				throw new InvalidInformationException(DataIntegrityAutomation.class, new Exception(), "Characters entered in columns \'Stream_Id\' and/or \'Sub_Stream_Id\' in a sheet in Input Metadata File \'" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\' Module execution cannot be started. Please enter Numeric codes.");
			} catch (InvalidInformationException e) {
			}
			System.exit(0);
		}

		setMetadataInfo();
		
		for(ConfigParams cp: configParameters)
		{
			if(cp.getConfigParameter().trim().equalsIgnoreCase("password"))
			{
				ConfigurationManager.getInstance().getUserConfig().setPassword(cp.getValue());
			}
		}
		
		verifyMetadataInput();
		
		if (!ApplicationDatabaseStructure.getInstance().databaseExists(dbName)) {
			String errorMsg = "Invalid database name - Database \'" + dbName
					+ "\' does not exist.";
			if (dbName.contains("."))
				errorMsg = errorMsg
						+ " Please ensure that a fully qualified database name has not been entered.";

			try {
				throw new InvalidInformationException(JobTypeParser.class,
						new Exception(), errorMsg);
			} catch (InvalidInformationException e1) {
			}
			System.exit(0);
		}

		cleanupTestId(); // clean up any inconsistent Test Ids in the summary
							// table
		
		setTestCycle(); // set the test cycle for this execution

		ConfigurationManager.getInstance().getAppConfig()
				.setApplicationConfiguration();

		String[] temp_exts = ConfigurationManager.getInstance().getUserConfig()
				.getBteqFileExts().split(","); // splitting comma separated data
		bteq_exts = temp_exts;

		String[] temp_exts2 = ConfigurationManager.getInstance()
				.getUserConfig().getFastloadFileExts().split(","); // splitting
																	// comma
																	// separated
																	// data
		fld_exts = temp_exts2;

		String[] temp_exts3 = ConfigurationManager.getInstance()
				.getUserConfig().getMultiloadFileExts().split(","); // splitting
																	// comma
																	// separated
																	// data
		mld_exts = temp_exts3;

		String[] temp_exts4 = ConfigurationManager.getInstance()
				.getUserConfig().getTptFileExts().split(","); // splitting comma
																// separated
																// data
		tpt_exts = temp_exts4;
		
		reader.setSourcesAndTargets(f_path); // sources and targets set through
												// constructor in order for
												// individual components to be
												// run with a single point of
												// access

		if (reader.getSources().isEmpty() || reader.getTargets().isEmpty()) // ensure
																			// the
																			// input
																			// metadata
																			// file
																			// is
																			// not
																			// empty.
																			// If
																			// it
																			// is
																			// empty,
																			// log
																			// error
																			// and
																			// exit,
																			// as
																			// execution
																			// cannot
																			// be
																			// carried
																			// out
																			// without
																			// this
																			// information
		{
			try {
				throw new MissingInformationException(
						JobTypeParser.class,
						new Exception(),
						"Input Parameter file \'"
								+ f_path
								+ "\' is empty or is missing necessary information. Please check and run again");
			} catch (MissingInformationException e) {
			}
			// logger.debug("Input Parameter file \'" + f_path +
			// "\' is empty or is missing necessary information. Please check and run again");
			System.exit(0);
		}

		logger.info("======================================================================================");
		logger.info("=                                                                          		=");
		logger.info("=		Scan Completed - No Required sheets or columns found absent		=");
		logger.info("=                                                                          		=");
		logger.info("======================================================================================");

	}

	public enum extensions // the enum 'extensions' stores all the possible
							// extensions for each file type
	{
		BTEQ(bteq_exts), TPT(tpt_exts), FLD(fld_exts), MLD(mld_exts);

		private String[] value;

		private extensions(String[] val) // constructor
		{
			this.value = val;
		}

		public String[] getValue() {
			return this.value;
		}
	}

	// function printMap prints the HashMap

	public void printMap(HashMap<String, String[]> mm_col_map2) {
		Object[] keys = mm_col_map2.keySet().toArray(); // list of keys, i.e.
														// all possible job
														// types

		for (int i = 0; i < mm_col_map2.size(); ++i) {
			for (int j = 0; j < mm_col_map2.get(keys[i]).length; ++j)
				// if(Integer.parseInt(map.get(keys[i]).toString()) != 0)
				JobTypeParser.getApplicationlogger().debug(
						"\t\t\t    " + keys[i].toString() + " - "
								+ mm_col_map2.get(keys[i])[j]);
		}
	}

	/**
	 * Contains all possible numeric and certain other datatypes for reference when checking similarity of datatypes in tables and/or views
	 */

	public enum datatypes {
		numerics(new String[] { "F", "D", "I", "I1", "I2", "I8", "N" }), non_numerics(
				new String[] { "CF", "DA", "AT" });

		private String[] value;

		private datatypes(String[] val) // constructor
		{
			this.value = val;
		}

		public String[] getValue() {
			return this.value;
		}
	}

	public void retrieveMetricsOnFiles(String file, int idx)
	{
		//System.out.println("Source: " + source);
		
		//System.out.println("Starting to get columns and run get_numeric_data");
		
		String[] cols = null;
		try {
			cols = convertToStringArray(FileDataRetriever.getFileColumns(file, idx));
		} catch (IOException e) {
			//e.printStackTrace();
		}
		
		if(!file.endsWith(".xls") && !file.endsWith(".xlsx") && !file.endsWith(".xlsm"))
			FileDataRetriever.getInstance().get_numeric_col_data(file, idx, cols);
		else
			FileDataRetriever.getInstance().get_excel_numeric_data(file);
		
		
		/*System.out.println("Numeric columns for source \'" + source + "\': ");
		System.out.println(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), source, true));
		System.out.println("\nAll columns for source \'" + source + "\': ");
		System.out.println(MetricsRecordUtil.getAllColumns(DataIntegrityAutomation.getInstance().getMetricsRecords(), source, false));*/
	}
	
	public boolean runRowCountAndMetrics(int index)
	{	
		return (rowCountAndMetrics.get(index).getRowCount().equals("Y") || rowCountAndMetrics.get(index).getDistinctValueCount().equals("Y") || rowCountAndMetrics.get(index).getSumAvgValue().equals("Y") || rowCountAndMetrics.get(index).getMinMaxValue().equals("Y") || rowCountAndMetrics.get(index).getNullCount().equals("Y"));
	}
	
	/**
	 * Defines the sequence of execution based on configuration parameters in Metadata file
	 */

	public void sequencer(JobTypeParser obj) throws InvalidInformationException, AccessViolationException, ObjectNotFoundException, MissingInformationException 
	{
		// String curr_dir = System.getProperty("user.dir").toString();
		// String wrapper_file = curr_dir + "\\wrapper file\\";

		int idx = 0;
		for(String source : reader.getSources())
		{
			if(reader.getPreliminaryChecks(wb.getSheet("Row Count & Metrics Collection")).get(idx).getSourceTypeCd() != 2 || !runRowCountAndMetrics(idx))	// if the source is not a file, or Row Count and Metrics checks are not to be run on this input, move to next source
			{
				++idx;
				continue;
			}
			
			retrieveMetricsOnFiles(source, idx);
			
			++idx;
		}
		
		idx = 0;
		for(String target : reader.getTargets())
		{
			if(reader.getPreliminaryChecks(wb.getSheet("Row Count & Metrics Collection")).get(idx).getTargetTypeCd() != 2 || !runRowCountAndMetrics(idx))	// if the source is not a file, or Row Count and Metrics checks are not to be run on this input, move to next source.
			{
				++idx;
				continue;
			}
			
			retrieveMetricsOnFiles(target, idx);
			
			++idx;
		}
		
		//MetricsRecordUtil.printMap(DataIntegrityAutomation.getInstance().getMetricsRecords());
		
		logger.info("======================================================================================");
		logger.info("=                                                                          		 =");
		logger.info("=				Beginning Module Execution				 =");
		logger.info("=			  Do not close this window unless prompted			 =");
		logger.info("=                                                                          		 =");
		logger.info("======================================================================================");

		String modules = ConfigurationManager.getInstance().getUserConfig()
				.getRunModules();

		String[] steps = modules.split(","); // splitting comma separated data
												// (which modules to run) from
												// config.properties file
		// boolean one_executed = false;

		if (steps.length == 1 && (steps[0].equals("") || steps[0].isEmpty())) {
			logger.error("No Steps Found in Configuration file - exiting execution");
			return;
		}

		Connection conn = null;
		
		try {
			conn = new TeradataDataSource().getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
			instantiateAllBatchStatements(conn);
		} catch (SQLException e2) {
			JobTypeParser.getApplicationlogger().error("Connection Retrieval encountered an exception in Sequencer: " + e2);
		}

		numOfRecords = FileDataRetriever.getInstance().metadata_line_count(ConfigurationManager.getInstance().getUserConfig().getInputFilePath().toString(), true);

		//System.out.println("number of records: " + numOfRecords);
		ExecutorService service = null;
		try {
			service = Executors.newFixedThreadPool(14);
		} catch(Exception e){
			e.printStackTrace();
		} finally {
		}

		Set<Callable<Void>> callables = new HashSet<Callable<Void>>();
			
		for (String step : steps) {
			if(step.trim().equals("19"))
				DataLoadAutomation.getInstance().DataLoadScheduler();
			
			if(step.trim().equalsIgnoreCase("all"))
			{
				callables.add(new ExecutionStatusTestAutomator());
				callables.add(new ObjectLevelRowCountTestAutomator());
				callables.add(new ScriptLevelRowCountTestAutomator());
				callables.add(new RowCountTestAutomator("Row_Count_Rslt", true));
				callables.add(new DistinctCountTestAutomator("Distinct_Value_Count_Rslt", true));
				callables.add(new SumAvgTestAutomator("Sum_Avg_Recon_Rslt", true));
				callables.add(new MinMaxTestAutomator("Min_Max_Recon_Rslt", true));
				callables.add(new RITestAutomator());
				callables.add(new NullCountTestAutomator("Null_Value_Count_Rslt", true));
				List<DuplicateRecordData> data = reader.getDuplicateInputData();		// data retrieved from excel
				callables.add(new DuplicateRecordTestAutomator(data));
				callables.add(new HistoryTestAutomator());
				callables.add(new MinusTestAutomator());
				break;
			}
			
			if (step.trim().equals("3")) {
				callables.add(new ExecutionStatusTestAutomator());
			}
			
			else if (step.trim().equals("4")) {
				callables.add(new ObjectLevelRowCountTestAutomator());
			}

			else if (step.trim().equals("5")) {
				callables.add(new ScriptLevelRowCountTestAutomator());
			}
			
			else if (step.trim().equals("6")) {
				callables.add(new RowCountTestAutomator("Row_Count_Rslt", true));
			}
			
			else if (step.trim().equals("7")) {
				callables.add(new DistinctCountTestAutomator("Distinct_Value_Count_Rslt", true));
			}
			
			else if (step.trim().equals("8")) {
				callables.add(new SumAvgTestAutomator("Sum_Avg_Recon_Rslt", true));
			}
			
			else if (step.trim().equals("9")) {
				callables.add(new MinMaxTestAutomator("Min_Max_Recon_Rslt", true));
			}
			
			else if (step.trim().equals("10")) {
				callables.add(new RITestAutomator());
			}
			
			else if (step.trim().equals("11")) {
				callables.add(new NullCountTestAutomator("Null_Value_Count_Rslt", true));
			}
			
			else if (step.trim().equals("12")) {
				List<DuplicateRecordData> data = reader.getDuplicateInputData();		// data retrieved from excel
				callables.add(new DuplicateRecordTestAutomator(data));
			}
			
			else if (step.trim().equals("13")) {
				callables.add(new HistoryTestAutomator());
			}

			else if (step.trim().equals("14")) {
				callables.add(new MinusTestAutomator());
			}
			
			else if (step.trim().equals("15")) {
				callables.add(new SurrogateKeyTestAutomator());
			}
			
			else if (step.trim().equals("16")) {
				callables.add(new RootCauseAnalysis());
			}
			
			else if (!QueryGenerator.containsValue(valid_steps, step.trim(), new boolean[1])) {
				throw new InvalidInformationException(JobTypeParser.class,
						new Exception(), "Invalid step number \'" + step
						+ "\' entered - skipping this step.");
			}
		}
		
		try {
			List<Future<Void>> futures = service.invokeAll(callables);	// invoking all threads

			for(Future<Void> future : futures) {
				applicationLogger.info("future.get() = " + future.get());
			}
		} catch (InterruptedException | ExecutionException e) {
			applicationLogger.error("Exception in JobTypeParser: " + e);
		}
		
		service.shutdown();

		try {
			executeBatchStatements(summaryBatchStatements);		// execute statements making insertions in summary table
			closeAllBatchStatements();							// close all batch statements before exiting
		} catch (SQLException e1) {
			applicationLogger.error("Batch Execution encountered an exception" + e1);
		} finally {
			try {
				if(conn != null)
					conn.close();
			} catch (SQLException e) {
				applicationLogger.error("Connection closing encountered an exception in Sequencer" + e);
			}
		}
	
		enterIntoReleaseLookup(environment); // enter success of this test cycle in Release_Lookup table
		//System.out.println("Exiting from sequencer");
	}

	private String[] convertToStringArray(List<String> fileColumns) 
	{
		String [] returnCols = new String [fileColumns.size()];
		
		for(int i=0; i<fileColumns.size(); ++i)
			returnCols[i] = fileColumns.get(i);
		
		return returnCols;
	}

	public static void main(String[] args) {
		
		/*LicenseView form = null;
	    String filepath = "/resources/license.dat";
	    
	    //String filepath = System.getProperty("user.dir").toString() + "src/resources/license.dat";
		
		File licenseFile = new File(filepath);
        boolean licenseValidated = false;
        if (!licenseFile.exists()) {
            licenseValidated = false;
        }

        LicenseVO license = LicenseController.getInstance().readFromLicenseFile();
        System.out.println("license: " + license);
        if (license != null) {
            licenseValidated = LicenseController.getInstance().validateLicense(
                    license.getUserId(),
                    license.getLicenseKey(),
                    license.getRegistrationKey());
        }
        
        if(!licenseValidated) {
        	form = new LicenseView();        	
        }*/
        
		JobTypeParser obj = new JobTypeParser();
		
		/**
		 * Creates output tables if they do not exist already. 
		 * Creates lookup tables each time to reflect any changes in input file
		 */
		if(ConfigurationManager.getInstance().getUserConfig().getCreateResultTables().equals("T"))	// create result and lookup tables only if create result tables flag is true in MTD sheet
			ApplicationDatabaseStructure.getInstance().application_ddl();
		else
		{
			int delimeter_cd_col_id = reader.getColId("File_Delimiter_Type_Cd", "Lookup Values", JobTypeParser.getWorkbook());
			int delimeter_type_col_id = reader.getColId("File_Delimiter", "Lookup Values", JobTypeParser.getWorkbook());
			Sheet lookup_sheet = wb.getSheet("Lookup Values");
			
			for(int i=0; i < lookup_sheet.getLastRowNum(); ++i)
			{
				if(lookup_sheet.getRow(i+1).getCell(delimeter_cd_col_id) == null)
					continue;
				ApplicationDatabaseStructure.getInstance().setDelimeterMap(lookup_sheet, i, delimeter_cd_col_id, delimeter_type_col_id);
			}
		}
		
		
		// call sequencing function to run application
		
		/*for(ColumnMapping colMapping : JobTypeParser.getColumnMapping())
		{
			System.out.println(colMapping);
		}*/
		
		try {
			obj.sequencer(obj);
		} catch (InvalidInformationException e) {
			JobTypeParser.applicationLogger.error("Exception in JobTypeParser: " + e);
		} catch (AccessViolationException e) {
			JobTypeParser.applicationLogger.error("Exception in JobTypeParser: " + e);
		} catch (ObjectNotFoundException e) {
			JobTypeParser.applicationLogger.error("Exception in JobTypeParser: " + e);
		} catch (MissingInformationException e) {
			JobTypeParser.applicationLogger.error("Exception in JobTypeParser: " + e);
		}
		
		//System.out.println("Exiting from JobTypeParser");
		
		/*String value1 = "Frankfurt";
		String value2 = "Amsterdam";
		
		if(value1.compareToIgnoreCase(value2) < 0)
			System.out.println("\'" + value1 + "\' is smaller than \'" + value2 + "\'");
		else
			System.out.println("\'" + value1 + "\' is greater than \'" + value2 + "\'");*/
	}
}
