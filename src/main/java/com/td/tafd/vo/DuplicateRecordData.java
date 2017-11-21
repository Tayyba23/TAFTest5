package com.td.tafd.vo;

public class DuplicateRecordData {

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	public String getEnvId() {
		return envId;
	}

	public void setEnvId(String envId) {
		this.envId = envId;
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

	public String getSubStreamId() {
		return subStreamId;
	}

	public void setSubStreamId(String subStreamId) {
		this.subStreamId = subStreamId;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPkColumnName() {
		return pkColumnName;
	}

	public void setPkColumnName(String pkColumnName) {
		this.pkColumnName = pkColumnName;
	}

	public String getDuplicateType() {
		return duplicateType;
	}

	public void setDuplicateType(String duplicateType) {
		this.duplicateType = duplicateType;
	}

	private String condition;
	private String envId;
	private String streamId;
	private String subStreamId;
	private String databaseName;
	private String tableName;
	private String pkColumnName;
	private String duplicateType;

}
