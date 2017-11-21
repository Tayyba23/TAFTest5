package com.td.tafd.modules.dd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.AccessViolationException;
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.validation.Validator;
import com.td.tafd.vo.DuplicateColumnInfo;
import com.td.tafd.vo.DuplicateRecordData;

public class DuplicateRecordTestAutomator implements Callable<Void> {
	private List<DuplicateRecordData> data;

	public DuplicateRecordTestAutomator(List<DuplicateRecordData> data) {
		this.data = data;
		//System.out.println("Size in constructor: " + this.data.size());
	}

	/**
	 * 
	 * @param sqlStmts
	 * @return
	 * @throws AccessViolationException
	 */
	private boolean executeBatchStatement(final List<String> sqlStmts,
			Connection conn) throws AccessViolationException {
		boolean res = false;
		Statement statement = null;
		if (conn != null && sqlStmts != null && sqlStmts.size() > 0) {
			try {
				statement = conn.createStatement();
				for (String sql : sqlStmts) {
					statement.addBatch(sql);
				}
				int[] results = statement.executeBatch();
				if (results != null && results.length > 0) {
					res = true;
				}

			} catch (SQLException e) {
				if (e.getSQLState().equals("HY000")) {
					DuplicateRecordUtil.throwAccessViolationException();
				} else {
					JobTypeParser.getApplicationlogger().debug(
							"Duplicate primary key in summary_tbl");
				}
			}
		}

		return res;
	}

	/**
	 * Populating HashMap with table name, pk column names, pk duplicate count
	 * (-1 initially) and full row duplicate count (-1 initially) if marked
	 * "PK", "FR" or "All" in column "Dup_type", add the fully-qualified table
	 * name, column name, pkDupCount and fullRowDupCount to HashMap
	 * 
	 * @param conditions
	 * @param env_value
	 * @return
	 * @throws AccessViolationException
	 * @throws MissingInformationException
	 */
	public HashMap<String, DuplicateColumnInfo> populateTables(Connection conn)
			throws AccessViolationException, MissingInformationException {
		HashMap<String, DuplicateColumnInfo> tables = new HashMap<>();

		List<String> invalidTables = new ArrayList<>();
		List<String> sqlStmts = new ArrayList<>();
		int index = 0;
		
		for (DuplicateRecordData drd : data) {
			String sql = null;
			String tableName = drd.getDatabaseName() + "." + drd.getTableName();
			String env_value = drd.getEnvId();
			String condition = drd.getCondition();
			
			//System.out.println("Current table: " + tableName);
			//System.out.println("Current condition: " + condition);
			
			if ("PK".equalsIgnoreCase(condition)
					|| "FR".equalsIgnoreCase(condition)
					|| condition.equalsIgnoreCase("ALL")) {
				// in summary_tbl, insert with 'unsuccessful' status
				sql = DuplicateRecordSQLUtil.getInstance()
						.createInsertStatement(tableName, env_value, condition,
								false);

				// if tblToDupCount already contains pk column(s), append this
				// column to previous one, separated by a comma
				if (tables.containsKey(tableName)
						&& !tables.get(tableName).getColumnName()
								.contains((drd.getPkColumnName()))) {
					String oldValue = tables.get(tableName).getColumnName();
					tables.get(tableName).setColumnName(
							new StringBuilder().append(oldValue).append(",")
									.append(drd.getPkColumnName()).toString());

				}
				// otherwise simply add the incoming data into the HashMap
				else {
					DuplicateColumnInfo dci = new DuplicateColumnInfo();

					dci.setColumnName(drd.getPkColumnName());
					dci.setFullRowDuplicateCount("-1");
					dci.setPkDuplicateCount("-1");
					
					//System.out.println("Adding table \'" + tableName + "\' to HashMap.");
					
					tables.put(tableName, dci);
				}
			}
			/**
			 * if anything other than "PK", "FR" or "All" encountered in column
			 * "Dup_type", skip this test for that table and log error and
			 * relevant information
			 * 
			 */
			else {
				if (!tables.containsKey(tableName))
					invalidTables.add(tableName);

				if (!tableName.equals(".") && !tables.containsKey(tableName)) {

					sql = DuplicateRecordSQLUtil.getInstance()
							.createInsertStatement(tableName, env_value,
									condition, true);
				}

				else if ((tableName.equals(".") || tableName.startsWith(".") || tableName
						.endsWith(".")) && condition.equals("")) {
					throw new MissingInformationException(
							JobTypeParser.class,
							new Exception(),
							"Database Name and/or Table Name is missing in Input File. Please check columns \'DB_Name\' and \'Table_Name\' in row "
									+ (index + 2)
									+ " of sheet \'Duplicate Data Verification\' in file \""
									+ ConfigurationManager.getInstance()
											.getUserConfig().getInputFilePath()
									+ "\"");
				}

			}

			sqlStmts.add(sql);

			index++;
		}

		executeBatchStatement(sqlStmts, conn);

		for (String invalidTbl : invalidTables)
			tables.remove(invalidTbl);

		JobTypeParser.getApplicationlogger().info(
				"In function \'populateTblToDupMap\', returning HashMap: \'tblToDupCount\' = "
						+ tables);

		return tables;
	}

	/*
	 * --------------------------------------------------------------------------
	 * -------------------- | Function runs checks for pk duplicates and full row
	 * duplicates | | It enters the results into pk_rslt table in the database
	 * and updates summary table as well | ----------------
	 * ----------------------------------------------------------
	 * --------------------
	 */

	public Void call() {
		long startTime = System.currentTimeMillis();
		long endTime = System.currentTimeMillis();
		
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'DUPLICATE RECORD CHECK\'			|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");

		HashMap<String, DuplicateColumnInfo> tblToDupCount = new HashMap<>();

		String env = null;
		PreparedStatement ps = null;

		int index = 0;
		int numOfTables = 0;
		
		ArrayList<String> allCols = new ArrayList<String>();
		Connection conn = null;
		ResultSet rs = null;
		boolean shouldContinue = true;
		while (shouldContinue) {
			try {

				TeradataDataSource tdSource = new TeradataDataSource();

				try {
					conn = tdSource.getConnection(ConfigurationManager
							.getInstance().getUserConfig().getHostname());
				} catch (SQLException e) {
					JobTypeParser
							.getApplicationlogger()
							.error("Connection creation encountered an exception in duplicate value check");
					shouldContinue = false;
				}
				if (conn != null) {
					/**
					 * call populateTables HashMap in order to assign required
					 * values into HashMap
					 */
					try {
						tblToDupCount = populateTables(conn);
					} catch (AccessViolationException
							| MissingInformationException e1) {
						JobTypeParser.getLogger().error(
								ConfigurationManager.getInstance()
										.getExceptionConfig()
										.getInvalidInformationCode()
										+ ": " + e1.getMessage());
						shouldContinue = false;
					}

					String previousTableName = "";
					
					for (DuplicateRecordData drd : data) {
						String tableName = drd.getDatabaseName() + "." + drd.getTableName();
						
						/*System.out.println("data.size(): " + data.size() + ", index: " + index + ", numOfTables: " + numOfTables);
						if( (index == (data.size()-1)) && numOfTables > 0)
						{
							endTime = System.currentTimeMillis();
							Validator.printModuleCompletionPrompt("PK and Full Row Duplicate Verification", startTime, endTime, "Successful", numOfTables);
							break;
							//JobTypeParser.getLogger().info("Modules \'PK and Full Row Duplicate identification\' completed successfully for " + (numOfTables+1) + " inputs");
						}*/
						
						if(previousTableName.equals(tableName))		// if the table is the same, move to next entry in object 'data'
						{	
							++index;
							continue;
						}
						
						DuplicateColumnInfo dci = tblToDupCount.get(tableName);

						/*DuplicateRecordUtil.checkStreams(drd.getStreamId(),
								drd.getSubStreamId(), index);*/
						// check for table existence
						if (!tdSource.objectExists(tableName, conn)) {
							
							if(!tableName.equals("."))
								JobTypeParser
								.getLogger()
								.error(ConfigurationManager.getInstance()
										.getExceptionConfig()
										.getObjectNotFoundCode()
										+ ": "
										+ "Error: Object \'"
										+ tableName
										+ "\' does not exist - Modules \'PK Duplicate Check\' and \'Full Row Duplicate Check\' cannot be executed");
							shouldContinue = false;
						} else {
							// check for column existence
							for (String col : tblToDupCount.get(tableName)
									.getColumnName().split(",")) {
								if (!tdSource.columnExists(
										tableName.split("\\.")[0],
										tableName.split("\\.")[1], col.trim(),
										conn)) {
									JobTypeParser
											.getLogger()
											.error(ConfigurationManager
													.getInstance()
													.getExceptionConfig()
													.getObjectNotFoundCode()
													+ ": "
													+ "Error: Column \'" + col + "\' does not exist in object \'"
													+ tableName
													+ "\' - Modules \'PK Duplicate Check\' and \'Full Row Duplicate Check\' cannot be executed");
									shouldContinue = false;

								}
							}

							// clear allCols ArrayList before retrieving new list of columns
							
							allCols.clear();
							env = drd.getEnvId();

							try {
								String updateSummaryRow = null;
								String fullRowStatus = "NA";
								String pkDupStatus = "NA";
								String pkDuplicateQuery = "";
								String fullRowDupQuery = "";
								String condition = drd.getCondition();
								String columnNames = tblToDupCount.get(
										tableName).getColumnName();
								if (condition.equalsIgnoreCase("PK")) {
									pkDuplicateQuery = DuplicateRecordSQLUtil
											.getInstance().getPKDuplicateQuery(
													condition, tableName,
													columnNames);
									updateSummaryRow = DuplicateRecordSQLUtil
											.getInstance().getSummaryRowQuery(
													condition, tableName, env,
													null);
									// as Full Row Duplicate is not to be run
									tblToDupCount.get(tableName)
											.setFullRowDuplicateCount("NA");
								} else if (condition.equalsIgnoreCase("FR")) {
									fullRowDupQuery = QueryGenerator.getQuery(
											conn, tableName, "duplicate",
											allCols, false);
									updateSummaryRow = DuplicateRecordSQLUtil
											.getInstance().getSummaryRowQuery(
													condition, tableName, env,
													null);
									// as PK is not to be run
									tblToDupCount.get(tableName)
											.setPkDuplicateCount("NA");
								} else if (condition.equalsIgnoreCase("all")) {
									pkDuplicateQuery = DuplicateRecordSQLUtil
											.getInstance().getPKDuplicateQuery(
													condition, tableName,
													columnNames);
									fullRowDupQuery = QueryGenerator.getQuery(
											conn, tableName, "duplicate",
											allCols, false);
									updateSummaryRow = DuplicateRecordSQLUtil
											.getInstance().getSummaryRowQuery(
													condition, tableName, env,
													null);
								}
								/**
								 * Ensure PK Duplicate query is not empty before
								 * executing it, so as to avoid SQL Exceptions
								 * 
								 */
								if (!pkDuplicateQuery.isEmpty()) {
									// execute pkDuplicate query
									ps = conn
											.prepareStatement(pkDuplicateQuery);
									rs = ps.executeQuery();
									tblToDupCount.get(tableName)
											.setPkDuplicateCount("0");
									while (rs.next()) {
										tblToDupCount
												.get(tableName)
												.setPkDuplicateCount(rs.getString(1));
									}
									rs.close();
									ps.close();

									// set pkDupStatus as 'passed' if PK
									// duplicate
									// count is 0
									pkDupStatus = (tblToDupCount.get(tableName)
											.getPkDuplicateCount().equals("0") ? "Passed"
											: "Failed");
								}
								/**
								 * else, fullRowDupQuery is an invalid query,
								 * possible because of absence of technical
								 * columns or invalid technical column names
								 */
								if (fullRowDupQuery.equals("invalid query")) {
									updateSummaryRow = DuplicateRecordSQLUtil
											.getInstance().getSummaryRowQuery(
													condition, tableName, env,
													drd.getDuplicateType());
									/**
									 * update summary_tbl with "unsuccessful"
									 */
									ps = conn
											.prepareStatement(updateSummaryRow);
									ps.execute();
									ps.close();
								}
								/**
								 * Ensure Full Row Duplicate query is not empty
								 * before executing it, so as to avoid SQL
								 * Exceptions
								 */
								else if (!fullRowDupQuery.isEmpty()) {
									long queryStartTime = System
											.currentTimeMillis();
									// execute fullRowDuplicate query
									ps = conn.prepareStatement(fullRowDupQuery);
									rs = ps.executeQuery();
									JobTypeParser
											.getDuplicateRecordLogger()
											.info("Query execution (full row duplicate) completed in "
													+ (System
															.currentTimeMillis() - queryStartTime)
													+ " (milliseconds)");
									tblToDupCount.get(tableName)
											.setFullRowDuplicateCount("0");

									while (rs.next()) {
										tblToDupCount
												.get(tableName)
												.setFullRowDuplicateCount(rs.getString(1));
									}
									rs.close();
									ps.close();
									/**
									 * set fullRowStatus as 'passed' if full row
									 * duplicate count is 0
									 */
									fullRowStatus = (tblToDupCount.get(
											tableName)
											.getFullRowDuplicateCount().equals("0") ? "Passed"
											: "Failed");
								}
								/**
								 * update summary_tbl with "success" or
								 * "partially run", based on the value
								 * encountered in Dup_type column in input
								 * metadata file
								 */
								ps = conn.prepareStatement(updateSummaryRow);
								ps.execute();
								ps.close();

								/**
								 * set overallStatus as 'Passed' iff both
								 * pkDupCount and fullRowDupCount have passed
								 */
								String overallStatus = (((fullRowStatus
										.equals("Passed") && pkDupStatus
										.equals("NA"))
										|| (pkDupStatus.equals("Passed") && fullRowStatus
												.equals("Passed")) || (pkDupStatus
										.equals("Passed") && fullRowStatus
										.equals("NA"))) ? "Passed" : "Failed");

								String insertStmt = DuplicateRecordSQLUtil
										.getInsertStatement(drd, dci,
												fullRowStatus, pkDupStatus,
												pkDuplicateQuery,
												fullRowDupQuery, overallStatus);
								// insert values into result table 'pk_rslt'
								
								if(!insertStmt.equals(""))
								{
									long insertStartTime = System
											.currentTimeMillis();
									ps = conn.prepareStatement(insertStmt);
									ps.execute();
									JobTypeParser
									.getDuplicateRecordLogger()
									.info("One row Insertion (pk_rslt) completed in "
											+ (System.currentTimeMillis() - insertStartTime)
											+ " (milliseconds)");

									//System.out.println("previousTableName: " + previousTableName + ", tableName: " + tableName);
									if(!previousTableName.equalsIgnoreCase(tableName))
										++numOfTables;
									
									previousTableName = tableName;
								}
								
							} catch (SQLException e) {
								//e.printStackTrace();
								if (e.getSQLState().equals("HY000")) {
									JobTypeParser
											.getLogger()
											.error(ConfigurationManager
													.getInstance()
													.getExceptionConfig()
													.getAccessViolationCode()
													+ ": "
													+ "The user \'"
													+ ConfigurationManager
															.getInstance()
															.getUserConfig()
															.getUsername()
													+ "\' does not have \'insert\' and/or \'update\' rights to object \'"
													+ ApplicationDatabaseStructure
															.getInstance()
															.getDbName()
													+ ".summary_tbl\' or object \'"
													+ ApplicationDatabaseStructure
															.getInstance()
															.getDbName()
													+ ".\'");
									shouldContinue = false;
								} else {
									JobTypeParser.getApplicationlogger().error(
											e.getMessage());
									shouldContinue = false;
								}
							} catch(Exception e) {
								JobTypeParser.getDuplicateRecordLogger().error("Exception encountered in DuplicateRecordTestAutomator: " + e);
							}

						}
						
						/*if( (index == (data.size()-1)) && numOfTables > 0)
							JobTypeParser.getLogger().info("Modules \'PK and Full Row Duplicate identification\' completed successfully");*/
						
						//System.out.println("data.size(): " + data.size() + ", index: " + index + ", numOfTables: " + numOfTables);
						if( (index == (data.size()-1)) && numOfTables > 0)
						{
							endTime = System.currentTimeMillis();
							Validator.printModuleCompletionPrompt("PK and Full Row Duplicate Verification", startTime, endTime, "Successful", numOfTables);
							break;
							//JobTypeParser.getLogger().info("Modules \'PK and Full Row Duplicate identification\' completed successfully for " + (numOfTables+1) + " inputs");
						}
						
						index++;
					}
				} else {
					JobTypeParser.getDuplicateRecordLogger().error(
							"Database connection is null");
					shouldContinue = false;
				}
			} /*catch (InvalidInformationException e1) {
				JobTypeParser.getLogger().error(
						ConfigurationManager.getInstance().getExceptionConfig()
								.getInvalidInformationCode()
								+ ": " + e1.getMessage());
				shouldContinue = false;
			}*/ catch (Exception e) {
				JobTypeParser.getApplicationlogger().error("Exception encountered in duplicate value check");
				JobTypeParser.getApplicationlogger().error("Exception is: ", e);
				//e.printStackTrace();
			} finally {
				try {
					if (rs != null)
						rs.close();
					if (ps != null)
						ps.close();
					if (conn != null)
						conn.close();
					shouldContinue = false;
				}

				catch (SQLException e) {
					JobTypeParser
							.getApplicationlogger()
							.error("Connection, resultset or PreparedStatement Closing encountered an exception");
					shouldContinue = false;
				}
			}
		}
		
		//System.out.println("Exiting from duplicate test");
		return null;
	}

}
