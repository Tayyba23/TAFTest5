/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author mb255051
 *
 */
public class HistoryVerification {
	private int streamId;
	private int subStreamId;
	private String envId;
	private String dbName;
	private String tableName;
	private String columnName;
	private String pkColumn;
	private String startTSColumn;
	private String endTSColumn;
	

	/**
	 * @return the streamId
	 */
	public int getStreamId() {
		return streamId;
	}

	/**
	 * @param streamId
	 *            the streamId to set
	 */
	public void setStreamId(int streamId) {
		this.streamId = streamId;
	}

	/**
	 * @return the subStreamId
	 */
	public int getSubStreamId() {
		return subStreamId;
	}

	/**
	 * @param subStreamId
	 *            the subStreamId to set
	 */
	public void setSubStreamId(int subStreamId) {
		this.subStreamId = subStreamId;
	}

	/**
	 * @return the envId
	 */
	public String getEnvId() {
		return envId;
	}

	/**
	 * @param envId
	 *            the envId to set
	 */
	public void setEnvId(String envId) {
		this.envId = envId;
	}

	/**
	 * @return the dbName
	 */
	public String getDbName() {
		return dbName;
	}

	/**
	 * @param dbName
	 *            the dbName to set
	 */
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName
	 *            the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * @return the pkColumn
	 */
	public String getPkColumn() {
		return pkColumn;
	}

	/**
	 * @param pkColumn
	 *            the pkColumn to set
	 */
	public void setPkColumn(String pkColumn) {
		this.pkColumn = pkColumn;
	}

	/**
	 * @return the startTSColumn
	 */
	public String getStartTSColumn() {
		return startTSColumn;
	}

	/**
	 * @param startTSColumn
	 *            the startTSColumn to set
	 */
	public void setStartTSColumn(String startTSColumn) {
		this.startTSColumn = startTSColumn;
	}

	/**
	 * @return the endTSColumn
	 */
	public String getEndTSColumn() {
		return endTSColumn;
	}

	/**
	 * @param endTSColumn
	 *            the endTSColumn to set
	 */
	public void setEndTSColumn(String endTSColumn) {
		this.endTSColumn = endTSColumn;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("HistoryVerification [streamId=").append(streamId).append(", subStreamId=").append(subStreamId)
				.append(", envId=").append(envId).append(", dbName=").append(dbName).append(", tableName=")
				.append(tableName).append(", columnName=").append(columnName).append(", pkColumn=").append(pkColumn)
				.append(", startTSColumn=").append(startTSColumn).append(", endTSColumn=").append(endTSColumn)
				.append("]");
		return builder.toString();
	}
}
