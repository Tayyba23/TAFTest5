/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author mb255051
 *
 */
public class TechnicalColumnInfo {
	private String columnName;
	private String columnNo;

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
	 * @return the columnNo
	 */
	public String getColumnNo() {
		return columnNo;
	}

	/**
	 * @param columnNo
	 *            the columnNo to set
	 */
	public void setColumnNo(String columnNo) {
		this.columnNo = columnNo;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TechnicalColumnInfo [columnName=").append(columnName).append(", columnNo=").append(columnNo)
				.append("]");
		return builder.toString();
	}
}
