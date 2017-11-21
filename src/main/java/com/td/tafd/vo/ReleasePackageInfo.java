package com.td.tafd.vo;

public class ReleasePackageInfo {
	private String envID;
	private String dbName;
	private String objectName;
	private String objectType;
	private String status;
	private String processName;
	private String processType;

	/**
	 * @return the envID
	 */
	public String getEnvID() {
		return envID;
	}

	/**
	 * @param envID
	 *            the envID to set
	 */
	public void setEnvID(String envID) {
		this.envID = envID;
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
	 * @return the objectName
	 */
	public String getObjectName() {
		return objectName;
	}

	/**
	 * @param objectName
	 *            the objectName to set
	 */
	public void setObjectName(String objectName) {
		this.objectName = objectName;
	}

	/**
	 * @return the objectType
	 */
	public String getObjectType() {
		return objectType;
	}

	/**
	 * @param objectType
	 *            the objectType to set
	 */
	public void setObjectType(String objectType) {
		this.objectType = objectType;
	}

	/**
	 * @return the status
	 */
	public String getStatus() {
		return status;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	public void setStatus(String status) {
		this.status = status;
	}

	/**
	 * @return the processName
	 */
	public String getProcessName() {
		return processName;
	}

	/**
	 * @param processName
	 *            the processName to set
	 */
	public void setProcessName(String processName) {
		this.processName = processName;
	}

	/**
	 * @return the processType
	 */
	public String getProcessType() {
		return processType;
	}

	/**
	 * @param processType
	 *            the processType to set
	 */
	public void setProcessType(String processType) {
		this.processType = processType;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ReleasePackageInfo [envID=").append(envID).append(", dbName=").append(dbName)
				.append(", objectName=").append(objectName).append(", objectType=").append(objectType)
				.append(", status=").append(status).append(", processName=").append(processName)
				.append(", processType=").append(processType).append("]");
		return builder.toString();
	}
}
