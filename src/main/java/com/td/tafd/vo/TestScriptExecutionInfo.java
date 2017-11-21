/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author KT186036
 *
 */
public class TestScriptExecutionInfo 
{
	private int streamId;
	private int subStreamId;
	private String envId;
	
	private String testScript;
	private String testResultsetDb;
	private String testResultsetTbl;
	private String integratedLayerDb;
	private String integratedLayerTbl;
	private String integratedLayerScript;
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
	 * @return the testScript
	 */
	public String getTestScript() {
		return testScript;
	}
	/**
	 * @param testScript the testScript to set
	 */
	public void setTestScript(String testScript) {
		this.testScript = testScript;
	}
	/**
	 * @return the testResultsetDb
	 */
	public String getTestResultsetDb() {
		return testResultsetDb;
	}
	/**
	 * @param testResultsetDb the testResultsetDb to set
	 */
	public void setTestResultsetDb(String testResultsetDb) {
		this.testResultsetDb = testResultsetDb;
	}
	/**
	 * @return the testResultsetTbl
	 */
	public String getTestResultsetTbl() {
		return testResultsetTbl;
	}
	/**
	 * @param testResultsetTbl the testResultsetTbl to set
	 */
	public void setTestResultsetTbl(String testResultsetTbl) {
		this.testResultsetTbl = testResultsetTbl;
	}
	/**
	 * @return the integratedLayerDb
	 */
	public String getIntegratedLayerDb() {
		return integratedLayerDb;
	}
	/**
	 * @param integratedLayerDb the integratedLayerDb to set
	 */
	public void setIntegratedLayerDb(String integratedLayerDb) {
		this.integratedLayerDb = integratedLayerDb;
	}
	/**
	 * @return the integratedLayerTbl
	 */
	public String getIntegratedLayerTbl() {
		return integratedLayerTbl;
	}
	/**
	 * @param integratedLayerTbl the integratedLayerTbl to set
	 */
	public void setIntegratedLayerTbl(String integratedLayerTbl) {
		this.integratedLayerTbl = integratedLayerTbl;
	}
	/**
	 * @return the integratedLayerScript
	 */
	public String getIntegratedLayerScript() {
		return integratedLayerScript;
	}
	/**
	 * @param integratedLayerScript the integratedLayerScript to set
	 */
	public void setIntegratedLayerScript(String integratedLayerScript) {
		this.integratedLayerScript = integratedLayerScript;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("TestScriptExecutionInfo [streamId= ").append(streamId).append(", subStreamId= ").append(subStreamId).append(", envId= ").append(envId)
				.append(", testScript= ").append(testScript).append(", testResultsetDb= ").append(testResultsetDb).append(", testResultsetTbl= ").append(testResultsetTbl)
				.append(", integratedLayerDb= ").append(integratedLayerDb).append(", integratedLayerTbl= ").append(integratedLayerTbl).append(", integratedLayerScript= ").append(integratedLayerScript);
		return builder.toString();
	}
	
	
}
