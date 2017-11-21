/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author kt186036
 *
 */
public class PieChartInfo 
{
	private double passCount;
	/**
	 * @return the passCount
	 */
	public double getPassCount() {
		return passCount;
	}
	/**
	 * @return the failCount
	 */
	public double getFailCount() {
		return failCount;
	}

	private double failCount;
	
	public PieChartInfo(double passCnt, double failCnt) {
		passCount = passCnt;
		failCount = failCnt;
	}
	/**
	 * @return the passPercentage
	 */
	public double getPassPercentage() {
		return (passCount / (passCount + failCount) ) * 100 ;
	}
	
	/**
	 * @param passPercentage the passPercentage to set
	 */
	public void setPassCount(double passCount) {
		this.passCount = passCount;
	}
	
	/**
	 * @return the failPercentage
	 */
	public double getFailPercentage() {
		return (failCount / (passCount + failCount) ) * 100;
	}
	
	/**
	 * @param failPercentage the failPercentage to set
	 */
	public void setFailCount(double failCount) {
		this.failCount = failCount;
	}
}
