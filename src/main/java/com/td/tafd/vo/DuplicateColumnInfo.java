/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author mr255048
 *
 */
public class DuplicateColumnInfo {

	private String columnName;
	private String pkDuplicateCount;
	private String fullRowDuplicateCount;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getPkDuplicateCount() {
		return pkDuplicateCount;
	}

	public void setPkDuplicateCount(String pkDuplicateCount) {
		this.pkDuplicateCount = pkDuplicateCount;
	}

	public String getFullRowDuplicateCount() {
		return fullRowDuplicateCount;
	}

	public void setFullRowDuplicateCount(String fullRowDuplicateCount) {
		this.fullRowDuplicateCount = fullRowDuplicateCount;
	}

}
