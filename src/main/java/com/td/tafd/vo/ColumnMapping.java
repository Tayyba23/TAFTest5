/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author kt186036
 *
 */
public class ColumnMapping 
{
	private String sourceDb;
	private String targetDb;
	private String sourceName;
	private String targetName;
	private String sourceColumn;
	private String targetColumn;
	private boolean isNumeric;
	private String autoDetectDatatype;
	
	/**
	 * @return the sourceDb
	 */
	
	public String getSourceDb() {
		return sourceDb;
	}
	
	/**
	 * @param sourceDb the sourceDb to set
	 */
	
	public void setSourceDb(String sourceDb) {
		this.sourceDb = sourceDb;
	}
	
	/**
	 * @return the targetDb
	 */
	
	public String getTargetDb() {
		return targetDb;
	}
	
	/**
	 * @param targetDb the targetDb to set
	 */
	
	public void setTargetDb(String targetDb) {
		this.targetDb = targetDb;
	}
	
	/**
	 * @return the sourceName
	 */
	
	public String getSourceName() {
		return sourceName;
	}
	
	/**
	 * @param sourceName the sourceName to set
	 */
	
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}
	
	/**
	 * @return the targetName
	 */
	
	public String getTargetName() {
		return targetName;
	}
	
	/**
	 * @param targetName the targetName to set
	 */
	
	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}
	
	/**
	 * @return the sourceColumn
	 */
	
	public String getSourceColumn() {
		return sourceColumn;
	}
	
	/**
	 * @param sourceColumn the sourceColumn to set
	 */
	
	public void setSourceColumn(String sourceColumn) {
		this.sourceColumn = sourceColumn;
	}
	
	/**
	 * @return the targetColumn
	 */
	
	public String getTargetColumn() {
		return targetColumn;
	}
	
	/**
	 * @param targetColumn the targetColumn to set
	 */
	
	public void setTargetColumn(String targetColumn) {
		this.targetColumn = targetColumn;
	}

	/**
	 * @return the isNumeric
	 */
	public boolean isNumeric() {
		return isNumeric;
	}

	/**
	 * @param isNumeric the isNumeric to set
	 */
	public void setNumeric(boolean isNumeric) {
		this.isNumeric = isNumeric;
	}

	public String getIsNumeric()
	{
		return (isNumeric ? "Y" : "N");
	}
	
	/**
	 * @return the autoDetectDatatype
	 */
	public String getAutoDetectDatatype() {
		return autoDetectDatatype;
	}

	/**
	 * @param autoDetectDatatype the autoDetectDatatype to set
	 */
	public void setAutoDetectDatatype(String autoDetectDatatype) {
		this.autoDetectDatatype = autoDetectDatatype;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ColumnMapping [sourceDb=").append(sourceDb).append(", targetDb=").append(targetDb)
				.append(", sourceName=").append(sourceName).append(", targetName=").append(targetName)
				.append(", sourceColumn=").append(sourceColumn).append(", targetColumn=").append(targetColumn)
				.append(", isNumeric=").append(isNumeric).append(", autoDetectDatatype=").append(autoDetectDatatype).append("]");
		return builder.toString();
	}

	
	
}
