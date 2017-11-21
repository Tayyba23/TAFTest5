/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author mr255048
 *
 */
public class ModuleTimeInfo {

	private String moduleName;
	private long executionTime;

	public ModuleTimeInfo(String nm, long time)
	{
		moduleName = nm;
		executionTime = time;
	}
	
	public String getModuleName() {
		return moduleName;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	public long getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(long executionTime) {
		this.executionTime = executionTime;
	}

}
