/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author kt186036
 *
 */
public class HistoryTestDetail 
{
	private String dbName;
	private String tableName;
	private String reverse;
	private String overlap;
	private String gap;
	private String testDetail;	// full or partial, identified in MTD sheet 
	
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
	 * @return the reverse
	 */
	
	public String getReverse() {
		return reverse;
	}
	
	/**
	 * @param reverse the reverse to set
	 */
	
	public void setReverse(String reverse) {
		this.reverse = reverse;
	}
	
	/**
	 * @return the overlap
	 */
	
	public String getOverlap() {
		return overlap;
	}
	
	/**
	 * @param overlap the overlap to set
	 */
	
	public void setOverlap(String overlap) {
		this.overlap = overlap;
	}
	
	/**
	 * @return the gap
	 */
	
	public String getGap() {
		return gap;
	}
	
	/**
	 * @param gap the gap to set
	 */
	
	public void setGap(String gap) {
		this.gap = gap;
	}

	/**
	 * @return the testDetail
	 */
	public String getTestDetail() {
		return testDetail;
	}

	/**
	 * @param testDetail the testDetail to set
	 */
	public void setTestDetail(String testDetail) {
		this.testDetail = testDetail;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("HistoryTestDetail [dbName=").append(dbName).append(", tableName=").append(tableName)
				.append(", reverse=").append(reverse).append(", overlap=").append(overlap).append(", gap=").append(gap)
				.append("]");
		return builder.toString();
	}
}
