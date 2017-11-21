/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author KT186036
 *
 */
public class MinusTest 
{
	private int streamId;
	private int subStreamId;
	
	private String envId;
	private String dbNameA;
	private String tblNameA;
	private String dbNameB;
	private String tblNameB;
	
	private String aMinusB;
	private String bMinusA;
	private String excludeTechnicalCols;
	private String mappingSpecification;
	
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
	 * @return the dbNameA
	 */
	
	public String getDbNameA() {
		return dbNameA;
	}
	
	/**
	 * @param dbNameA the dbNameA to set
	 */
	
	public void setDbNameA(String dbNameA) {
		this.dbNameA = dbNameA;
	}
	
	/**
	 * @return the tblNameA
	 */
	
	public String getTblNameA() {
		return tblNameA;
	}
	
	/**
	 * @param tblNameA the tblNameA to set
	 */
	
	public void setTblNameA(String tblNameA) {
		this.tblNameA = tblNameA;
	}
	
	/**
	 * @return the dbNameB
	 */
	
	public String getDbNameB() {
		return dbNameB;
	}
	
	/**
	 * @param dbNameB the dbNameB to set
	 */
	
	public void setDbNameB(String dbNameB) {
		this.dbNameB = dbNameB;
	}
	
	/**
	 * @return the tblNameB
	 */
	
	public String getTblNameB() {
		return tblNameB;
	}
	
	/**
	 * @param tblNameB the tblNameB to set
	 */
	
	public void setTblNameB(String tblNameB) {
		this.tblNameB = tblNameB;
	}
	
	/**
	 * @return the aMinusB
	 */
	
	public String getaMinusB() {
		return aMinusB;
	}
	
	/**
	 * @param aMinusB the aMinusB to set
	 */
	
	public void setaMinusB(String aMinusB) {
		this.aMinusB = aMinusB;
	}
	
	/**
	 * @return the bMinusA
	 */
	
	public String getbMinusA() {
		return bMinusA;
	}
	
	/**
	 * @param bMinusA the bMinusA to set
	 */
	
	public void setbMinusA(String bMinusA) {
		this.bMinusA = bMinusA;
	}

	/**
	 * @return the excludeTechnicalCols
	 */
	public String getExcludeTechnicalCols() {
		return excludeTechnicalCols;
	}

	/**
	 * @param excludeTechnicalCols the excludeTechnicalCols to set
	 */
	public void setExcludeTechnicalCols(String excludeTechnicalCols) {
		this.excludeTechnicalCols = excludeTechnicalCols;
	}

	/**
	 * @return the mappingSpecification
	 */
	public String getMappingSpecification() {
		return mappingSpecification;
	}

	/**
	 * @param mappingSpecification the mappingSpecification to set
	 */
	public void setMappingSpecification(String mappingSpecification) {
		this.mappingSpecification = mappingSpecification;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("MinusTest [, envId= ").append(envId).append(", streamId= ").append(streamId).append(", subStreamId= ").append(subStreamId)
				.append(", dbNameA= ").append(dbNameA).append(", tblNameA= ").append(tblNameA)
				.append(", dbNameB= ").append(dbNameB).append(", tblNameB= ").append(tblNameB)
				.append(", aMinusB= ").append(aMinusB).append(", bMinusA= ").append(bMinusA)
				.append(", excludeTechnicalCols= ").append(excludeTechnicalCols).append(", mappingSpecification= ").append(mappingSpecification)
				.append("]");
		
		return builder.toString();
	}
}
