/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author kt186036
 *
 */
public class TableAndDuplicateTypeInfo 
{
	private String dbName;
	private String tableName;
	private String dupType;
	private String openRecords;
	private String endDateColumn;
	
	/**
	 * @return the dbName
	 */
	
	public String getDbName() {
		return dbName;
	}
	
	/**
	 * @param dbName the dbName to set
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
	 * @param tableName the tableName to set
	 */
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	/**
	 * @return the dupType
	 */
	
	public String getDupType() {
		return dupType;
	}
	
	/**
	 * @param dupType the dupType to set
	 */
	
	public void setDupType(String dupType) {
		this.dupType = dupType;
	}

	/**
	 * @return the openRecords
	 */
	public String getOpenRecords() {
		return openRecords;
	}

	/**
	 * @param openRecords the openRecords to set
	 */
	public void setOpenRecords(String openRecords) {
		this.openRecords = openRecords;
	}

	/**
	 * @return the endDateColumn
	 */
	public String getEndDateColumn() {
		return endDateColumn;
	}

	/**
	 * @param endDateColumn the endDateColumn to set
	 */
	public void setEndDateColumn(String endDateColumn) {
		this.endDateColumn = endDateColumn;
	}
}
