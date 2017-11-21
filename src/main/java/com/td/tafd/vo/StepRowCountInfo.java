/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author KT186036
 *
 */
public class StepRowCountInfo 
{
	private String stepName;
	
	private int rowCount1;
	private int rowCount2;
	private int rowCount3;
	/**
	 * @return the stepName
	 */
	public String getStepName() {
		return stepName;
	}
	/**
	 * @param stepName the stepName to set
	 */
	public void setStepName(String stepName) {
		this.stepName = stepName;
	}
	/**
	 * @return the rowCount1
	 */
	public int getRowCount1() {
		return rowCount1;
	}
	/**
	 * @param rowCount1 the rowCount1 to set
	 */
	public void setRowCount1(int rowCount1) {
		this.rowCount1 = rowCount1;
	}
	/**
	 * @return the rowCount2
	 */
	public int getRowCount2() {
		return rowCount2;
	}
	/**
	 * @param rowCount2 the rowCount2 to set
	 */
	public void setRowCount2(int rowCount2) {
		this.rowCount2 = rowCount2;
	}
	/**
	 * @return the rowCount3
	 */
	public int getRowCount3() {
		return rowCount3;
	}
	/**
	 * @param rowCount3 the rowCount3 to set
	 */
	public void setRowCount3(int rowCount3) {
		this.rowCount3 = rowCount3;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("StepRowCountInfo [stepName=").append(stepName)
				.append(", rowCount1=").append(rowCount1)
				.append(", rowCount2=").append(rowCount2)
				.append(", rowCount3=").append(rowCount3).append("]");
		return builder.toString();
	}
}
