/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author kt186036
 *
 */
public class SurrogateKey 
{
	private int streamId;
	private int subStreamId;
	
	private String envId;
	private String sourceDbName;
	private String sourceTblName;
	private String surrKeyDbName;
	private String surrKeyTblName;
	private String naturalKeyCols;
	private String surrogateKeyCol;
	private String concatenatedNaturalKeyCol;
	private String naturalKeyFunction;
	
	/**
	 * @return the streamId
	 */
	
	public int getStreamId() {
		return streamId;
	}
	
	/**
	 * @param streamId the streamId to set
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
	 * @param subStreamId the subStreamId to set
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
	 * @param envId the envId to set
	 */
	
	public void setEnvId(String envId) {
		this.envId = envId;
	}
	
	/**
	 * @return the sourceDbName
	 */
	
	public String getSourceDbName() {
		return sourceDbName;
	}
	
	/**
	 * @param sourceDbName the sourceDbName to set
	 */
	
	public void setSourceDbName(String sourceDbName) {
		this.sourceDbName = sourceDbName;
	}
	
	/**
	 * @return the sourceTblName
	 */
	
	public String getSourceTblName() {
		return sourceTblName;
	}
	
	/**
	 * @param sourceTblName the sourceTblName to set
	 */
	
	public void setSourceTblName(String sourceTblName) {
		this.sourceTblName = sourceTblName;
	}
	
	/**
	 * @return the surrKeyDbName
	 */
	
	public String getSurrKeyDbName() {
		return surrKeyDbName;
	}
	
	/**
	 * @param surrKeyDbName the surrKeyDbName to set
	 */
	
	public void setSurrKeyDbName(String surrKeyDbName) {
		this.surrKeyDbName = surrKeyDbName;
	}
	
	/**
	 * @return the surrKeyTblName
	 */
	
	public String getSurrKeyTblName() {
		return surrKeyTblName;
	}
	
	/**
	 * @param surrKeyTblName the surrKeyTblName to set
	 */
	
	public void setSurrKeyTblName(String surrKeyTblName) {
		this.surrKeyTblName = surrKeyTblName;
	}
	
	/**
	 * @return the naturalKeyCols
	 */
	
	public String getNaturalKeyCols() {
		return naturalKeyCols;
	}
	
	/**
	 * @param naturalKeyCols the naturalKeyCols to set
	 */
	
	public void setNaturalKeyCols(String naturalKeyCols) {
		this.naturalKeyCols = naturalKeyCols;
	}
	
	/**
	 * @return the surrogateKeyCols
	 */
	
	public String getSurrogateKeyCol() {
		return surrogateKeyCol;
	}
	
	/**
	 * @param surrogateKeyCols the surrogateKeyCols to set
	 */
	
	public void setSurrogateKeyCol(String surrogateKeyCol) {
		this.surrogateKeyCol = surrogateKeyCol;
	}
	
	/**
	 * @return the naturalKeyFunction
	 */
	
	public String getNaturalKeyFunction() {
		return naturalKeyFunction;
	}
	
	/**
	 * @param naturalKeyFunction the naturalKeyFunction to set
	 */
	
	public void setNaturalKeyFunction(String naturalKeyFunction) {
		this.naturalKeyFunction = naturalKeyFunction;
	}

	/**
	 * @return the concatenatedNaturalKeyCols
	 */
	
	public String getConcatenatedNaturalKeyCol() {
		return concatenatedNaturalKeyCol;
	}

	/**
	 * @param concatenatedNaturalKeyCols the concatenatedNaturalKeyCols to set
	 */
	
	public void setConcatenatedNaturalKeyCol(String concatenatedNaturalKeyCol) {
		this.concatenatedNaturalKeyCol = concatenatedNaturalKeyCol;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("SurrogateKey [streamId = ").append(streamId).append(", subStreamId").append(subStreamId).append(", envId= ").append(envId)
		.append(", sourceDbName= ").append(sourceDbName).append(", sourceTblName= ").append(sourceTblName).append(", surrKeyDbName= ").append(surrKeyDbName)
		.append(", surrKeyTblName= ").append(surrKeyTblName).append(", naturalKeyCols= ").append(naturalKeyCols)
		.append(", surrogateKeyCol= ").append(surrogateKeyCol).append(", naturalKeyFunction= ").append(naturalKeyFunction)
		.append(", concatenatedNaturalKeyCol= ").append(concatenatedNaturalKeyCol).append("]");
		
		return builder.toString();
	}
	
}
