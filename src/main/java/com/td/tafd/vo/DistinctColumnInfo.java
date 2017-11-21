/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author kt186036
 *
 */
public class DistinctColumnInfo {

	private String name;
	private int count;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DistinctColumnInfo [name=").append(name)
				.append(", count=").append(count).append("]");
		return builder.toString();
	}
}
