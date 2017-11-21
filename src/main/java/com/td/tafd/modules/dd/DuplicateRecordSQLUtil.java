/**
 * 
 */
package com.td.tafd.modules.dd;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.vo.DuplicateRecordData;
import com.td.tafd.vo.DuplicateColumnInfo;

/**
 * @author mr255048
 *
 */
public class DuplicateRecordSQLUtil {

	private static DuplicateRecordSQLUtil instance;

	public synchronized static DuplicateRecordSQLUtil getInstance() {
		if (instance == null) {
			instance = new DuplicateRecordSQLUtil();
		}
		return instance;
	}

	private DuplicateRecordSQLUtil() {

	}

	public String createInsertStatement(final String tableName,
			final String env_value, final String condition,
			final boolean isSuccessfull) {
		String sql = null;
		StringBuilder buffer = new StringBuilder()
				.append("insert into "
						+ ApplicationDatabaseStructure.getInstance()
								.getDbName()
						+ ".Summary_Tbl (test_type_cd, test_cycle_id, source, target, metadata_file_input, application_output, status) values (")
				.append("\'")
				.append(Integer.parseInt(ConfigurationManager.getInstance()
						.getAppConfig().getPkDup()))
				.append("\', \'")
				.append(env_value)
				.append("_")
				.append(ConfigurationManager.getInstance().getAppConfig()
						.getTestCycle()).append("\', \'").append(tableName)
				.append("\', \'NA\', \'").append(condition);
		buffer.append("\', \'N\',");
		buffer.append("\'");
		if (true == isSuccessfull) {
			buffer.append("Not Run");
		} else {
			buffer.append("Unsuccessful");
		}
		buffer.append("\');");
		sql = buffer.toString();
		return sql;
	}

	public String getSummaryRowQuery(final String condition,
			final String tableName, final String env, final String dupType) {
		String query = null;
		if (null != tableName && null != env && null != condition) {
			if ("PK".equalsIgnoreCase(condition)
					|| "FR".equalsIgnoreCase(condition)
					|| "ALL".equalsIgnoreCase(condition)) {
				query = new StringBuilder()
						.append("update "
								+ ApplicationDatabaseStructure.getInstance()
										.getDbName()
								+ ".Summary_Tbl set application_output = \'Y\', status = \'Successful\' where source = \'")
						.append(tableName)
						.append("\' and test_type_cd = \'")
						.append(Integer.parseInt(ConfigurationManager
								.getInstance().getAppConfig().getPkDup()))
						.append("\' and test_cycle_id = \'")
						.append(env)
						.append("_")
						.append(ConfigurationManager.getInstance()
								.getAppConfig().getTestCycle()).append("\';")
						.toString();
			} else if (null != dupType) {
				query = new StringBuilder("insert into ")
						.append(ApplicationDatabaseStructure.getInstance()
								.getDbName())
						.append(".Summary_Tbl (test_type_cd, test_cycle_id, source, target, metadata_file_input, application_output, status) values (")
						.append("\'")
						.append(Integer.parseInt(ConfigurationManager
								.getInstance().getAppConfig().getPkDup()))
						.append("\', \'")
						.append(env)
						.append("_")
						.append(ConfigurationManager.getInstance()
								.getAppConfig().getTestCycle())
						.append("\', \'").append(tableName)
						.append("\', \'NA\', \'").append(dupType)
						.append("\', \'N\', \'Unsuccessful\');").toString();
			}
		}
		return query;
	}

	public String getPKDuplicateQuery(final String condition,
			final String tableName, final String columnNames) {
		String query = null;
		if (null != condition && null != tableName && null != columnNames) {
			if ("PK".equalsIgnoreCase(condition)
					|| "all".equalsIgnoreCase(condition)) {
				query = new StringBuilder().append("SELECT COUNT(A.cnt) FROM (select ")
						.append(columnNames).append(", count(*) as cnt from ")
						.append(tableName).append(" group by ")
						.append(columnNames).append(" having cnt > 1) AS A;")
						.toString();
			}
		}
		return query;
	}

	public static String getColumnLiteralValue(final String column) {
		return "\'" + column + "\'";
	}

	public static String getInsertStatement(final DuplicateRecordData drd,
			final DuplicateColumnInfo dci, final String fullRowStatus,
			final String pkDupStatus, final String pkDuplicateQuery,
			final String fullRowDupQuery, final String overallStatus) 
	{	
		String insertStmt = "";
		
		try {
			insertStmt = new StringBuilder()
			.append("insert into "
					+ ApplicationDatabaseStructure.getInstance()
					.getDbName()
					+ ".Pk_Rslt (stream_id, sub_stream_id, test_cycle_id, test_type_cd, full_row_status, primary_key_status, table_name, pk_columns, full_row_count, primary_key_count, business_date, user_id, env_id, execution_timestamp, script_text, test_status) values (")
					.append(drd.getStreamId())
					.append(", ")
					.append(drd.getSubStreamId())
					.append(", \'")
					.append(drd.getEnvId())
					.append("_")
					.append(ConfigurationManager.getInstance().getAppConfig()
							.getTestCycle())
							.append("\', ")
							.append(Integer.parseInt(ConfigurationManager.getInstance()
									.getAppConfig().getPkDup()))
									.append("," + getColumnLiteralValue(fullRowStatus))
									.append("," + getColumnLiteralValue(pkDupStatus))
									.append("," + getColumnLiteralValue(drd.getTableName()))
									.append("," + getColumnLiteralValue(dci.getColumnName()))
									.append(","
											+ getColumnLiteralValue(dci.getFullRowDuplicateCount()))
											.append("," + getColumnLiteralValue(dci.getPkDuplicateCount()))
											.append(","
													+ getColumnLiteralValue(ConfigurationManager
															.getInstance().getUserConfig()
															.getBusinessDate()))
															.append(","
																	+ getColumnLiteralValue(ConfigurationManager
																			.getInstance().getAppConfig().getUserId()))
																			.append("," + getColumnLiteralValue(drd.getEnvId()))
																			.append(","
																					+ getColumnLiteralValue(new SimpleDateFormat(
																							"yyyy.MM.dd.HH.mm.ss").format(new Date())))
																							.append(","
																									+ getColumnLiteralValue((drd.getCondition()
																											.equalsIgnoreCase("all")) ? pkDuplicateQuery.substring(26, pkDuplicateQuery.length()-7)
																													.concat("; ").concat(fullRowDupQuery.substring(22, fullRowDupQuery.length()-4))
																													: (drd.getCondition().equalsIgnoreCase("pk") ? pkDuplicateQuery.substring(26, pkDuplicateQuery.length()-7)
																															: fullRowDupQuery.substring(22, fullRowDupQuery.length()-4))))

																															.append("," + getColumnLiteralValue(overallStatus))
																															.append(" );").toString();

			
		} catch(StringIndexOutOfBoundsException e) {
			JobTypeParser.getDuplicateRecordLogger().error("Exception: " + e);	
			JobTypeParser.getDuplicateRecordLogger().debug("fullRowDupQuery: " + fullRowDupQuery);
			JobTypeParser.getDuplicateRecordLogger().debug("pkDuplicateQuery: " + pkDuplicateQuery);
			return "";
		}
		
		return insertStmt;
	}
}
