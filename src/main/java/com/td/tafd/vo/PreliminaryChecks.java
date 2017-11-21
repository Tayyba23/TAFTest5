/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author mb255051
 *
 */
public class PreliminaryChecks {
	private int streamId;
	private int subStreamId;
	private String envId;
	private String sourcePath;
	private String sourceName;
	private int sourceTypeCd;
	private String targetPath;
	private String targetName;
	private int targetTypeCd;
	private String fileDelimiterTypeCd;
	private String rowCount;
	private String distinctValueCount;
	private String sumAvgValue;
	private String minMaxValue;
	private String nullCount;
	private String mappingSpecifications;
	
	/**
	 * @return the mappingSpecifications
	 */
	public String getMappingSpecifications() {
		return mappingSpecifications;
	}

	/**
	 * @param mappingSpecifications the mappingSpecifications to set
	 */
	public void setMappingSpecifications(String mappingSpecifications) {
		this.mappingSpecifications = mappingSpecifications;
	}

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
	 * @return the sourcePath
	 */
	public String getSourcePath() {
		return sourcePath;
	}

	/**
	 * @param sourcePath
	 *            the sourcePath to set
	 */
	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	/**
	 * @return the sourceName
	 */
	public String getSourceName() {
		return sourceName;
	}

	/**
	 * @param sourceName
	 *            the sourceName to set
	 */
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	/**
	 * @return the sourceTypeCd
	 */
	public int getSourceTypeCd() {
		return sourceTypeCd;
	}

	/**
	 * @param sourceTypeCd
	 *            the sourceTypeCd to set
	 */
	public void setSourceTypeCd(int sourceTypeCd) {
		this.sourceTypeCd = sourceTypeCd;
	}

	/**
	 * @return the targetPath
	 */
	public String getTargetPath() {
		return targetPath;
	}

	/**
	 * @param targetPath
	 *            the targetPath to set
	 */
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}

	/**
	 * @return the targetName
	 */
	public String getTargetName() {
		return targetName;
	}

	/**
	 * @param targetName
	 *            the targetName to set
	 */
	public void setTargetName(String targetName) {
		this.targetName = targetName;
	}

	/**
	 * @return the targetTypeCd
	 */
	public int getTargetTypeCd() {
		return targetTypeCd;
	}

	/**
	 * @param targetTypeCd
	 *            the targetTypeCd to set
	 */
	public void setTargetTypeCd(int targetTypeCd) {
		this.targetTypeCd = targetTypeCd;
	}

	/**
	 * @return the fileDelimiterTypeCd
	 */
	public String getFileDelimiterTypeCd() {
		return fileDelimiterTypeCd;
	}

	/**
	 * @param fileDelimiterTypeCd
	 *            the fileDelimiterTypeCd to set
	 */
	public void setFileDelimiterTypeCd(String fileDelimiterTypeCd) {
		this.fileDelimiterTypeCd = fileDelimiterTypeCd;
	}

	/**
	 * @return the rowCount
	 */
	public String getRowCount() {
		return rowCount;
	}

	/**
	 * @param rowCount
	 *            the rowCount to set
	 */
	public void setRowCount(String rowCount) {
		this.rowCount = rowCount;
	}

	/**
	 * @return the distinctValueCount
	 */
	public String getDistinctValueCount() {
		return distinctValueCount;
	}

	/**
	 * @param distinctValueCount
	 *            the distinctValueCount to set
	 */
	public void setDistinctValueCount(String distinctValueCount) {
		this.distinctValueCount = distinctValueCount;
	}

	/**
	 * @return the sumValue
	 */
	public String getSumAvgValue() {
		return sumAvgValue;
	}

	/**
	 * @param sumValue
	 *            the sumValue to set
	 */
	public void setSumAvgValue(String sumAvgValue) {
		this.sumAvgValue = sumAvgValue;
	}

	/**
	 * @return the minValue
	 */
	public String getMinMaxValue() {
		return minMaxValue;
	}

	/**
	 * @param minValue
	 *            the minValue to set
	 */
	public void setMinMaxValue(String minMaxValue) {
		this.minMaxValue = minMaxValue;
	}

	/**
	 * @return the nullCount
	 */
	public String getNullCount() {
		return nullCount;
	}

	/**
	 * @param nullCount
	 *            the nullCount to set
	 */
	public void setNullCount(String nullCount) {
		this.nullCount = nullCount;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PreliminaryChecks [streamId=").append(streamId).append(", subStreamId=").append(subStreamId)
				.append(", envId=").append(envId).append(", sourcePath=").append(sourcePath).append(", sourceName=")
				.append(sourceName).append(", sourceTypeCd=").append(sourceTypeCd).append(", targetPath=")
				.append(targetPath).append(", targetName=").append(targetName).append(", targetTypeCd=")
				.append(targetTypeCd).append(", fileDelimiterTypeCd=").append(fileDelimiterTypeCd).append(", rowCount=")
				.append(rowCount).append(", distinctValueCount=").append(distinctValueCount).append(", sumAvgValue=")
				.append(sumAvgValue).append(", minMaxValue=").append(minMaxValue)
				.append(", nullCount=").append(nullCount)
				.append(", mappingSpecifications=").append(mappingSpecifications).append("]");
		return builder.toString();
	}

	
}
