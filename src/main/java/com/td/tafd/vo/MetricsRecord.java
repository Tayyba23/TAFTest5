/**
 * 
 */
package com.td.tafd.vo;

/**
 * MetricsRecord corresponds to the results of calculation of sum, average, minimum, maximum values
 * of each column in the object under consideration
 * 
 * @author kt186036
 */

public class MetricsRecord 
{
	private String columnName;
	private boolean columnIsNumeric;
	
	private double sum;
	private double average;
	
	private String minimum;
	private String maximum;
	
	private int nullCount;
	private int distinctCount;
	
	/**
	 * @return the sum
	 */
	public double getSum() {
		return sum;
	}
	
	/**
	 * @param sum the sum to set
	 */
	public void setSum(double sum) {
		this.sum = sum;
	}
	
	/**
	 * @return the average
	 */
	public double getAverage() {
		return average;
	}
	
	/**
	 * @param average the average to set
	 */
	public void setAverage(double average) {
		this.average = average;
	}
	
	/**
	 * @return the minimum
	 */
	public String getMinimum() {
		return minimum;
	}
	
	/**
	 * @param minimum the minimum to set
	 */
	public void setMinimum(String minimum) {
		this.minimum = minimum;
	}
	
	/**
	 * @return the maximum
	 */
	public String getMaximum() {
		return maximum;
	}
	
	/**
	 * @param maximum the maximum to set
	 */
	public void setMaximum(String maximum) {
		this.maximum = maximum;
	}
	
	/**
	 * @return the nullCount
	 */
	public int getNullCount() {
		return nullCount;
	}
	
	/**
	 * @param nullCount the nullCount to set
	 */
	public void setNullCount(int nullCount) {
		this.nullCount = nullCount;
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * @return the isNumeric
	 */
	public boolean columnIsNumeric() {
		return columnIsNumeric;
	}

	/**
	 * @param isNumeric the isNumeric to set
	 */
	public void setColumnIsNumeric(boolean isNumeric) {
		this.columnIsNumeric = isNumeric;
	}

	/**
	 * @return the distinctCount
	 */
	public int getDistinctCount() {
		return distinctCount;
	}

	/**
	 * @param distinctCount the distinctCount to set
	 */
	public void setDistinctCount(int distinctCount) {
		this.distinctCount = distinctCount;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("columnName= ").append(columnName).append(", columnIsNumeric=").append(columnIsNumeric)
				.append(", sum=").append(sum).append(", average=").append(average)
				.append(", minimum=").append(minimum).append(", maximum=").append(maximum)
				.append(", nullCount=").append(nullCount);
		return builder.toString();
	}
	
}
