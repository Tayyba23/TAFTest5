/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author mb255051
 *
 */
public class RIVerification {
	private int streamId;
	private int subStreamId;
	private String envId;
	private String parentDbName;
	private String parentTableName;
	private String pkColumns;
	private String childDbName;
	private String childTableName;
	private String fkColumns;

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
	 * @return the parentDbName
	 */
	public String getParentDbName() {
		return parentDbName;
	}

	/**
	 * @param parentDbName
	 *            the parentDbName to set
	 */
	public void setParentDbName(String parentDbName) {
		this.parentDbName = parentDbName;
	}

	/**
	 * @return the parentTableName
	 */
	public String getParentTableName() {
		return parentTableName;
	}

	/**
	 * @param parentTableName
	 *            the parentTableName to set
	 */
	public void setParentTableName(String parentTableName) {
		this.parentTableName = parentTableName;
	}

	/**
	 * @return the pkColumns
	 */
	public String getPkColumns() {
		return pkColumns;
	}

	/**
	 * @param pkColumns
	 *            the pkColumns to set
	 */
	public void setPkColumns(String pkColumns) {
		this.pkColumns = pkColumns;
	}

	/**
	 * @return the childDbName
	 */
	public String getChildDbName() {
		return childDbName;
	}

	/**
	 * @param childDbName
	 *            the childDbName to set
	 */
	public void setChildDbName(String childDbName) {
		this.childDbName = childDbName;
	}

	/**
	 * @return the childTableName
	 */
	public String getChildTableName() {
		return childTableName;
	}

	/**
	 * @param childTableName
	 *            the childTableName to set
	 */
	public void setChildTableName(String childTableName) {
		this.childTableName = childTableName;
	}

	/**
	 * @return the fkColumns
	 */
	public String getFkColumns() {
		return fkColumns;
	}

	/**
	 * @param fkColumns
	 *            the fkColumns to set
	 */
	public void setFkColumns(String fkColumns) {
		this.fkColumns = fkColumns;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RIVerification [streamId=").append(streamId).append(", subStreamId=").append(subStreamId)
				.append(", envId=").append(envId).append(", parentDbName=").append(parentDbName)
				.append(", parentTableName=").append(parentTableName).append(", pkColumns=").append(pkColumns)
				.append(", childDbName=").append(childDbName).append(", childTableName=").append(childTableName)
				.append(", fkColumns=").append(fkColumns).append("]");
		return builder.toString();
	}
}
