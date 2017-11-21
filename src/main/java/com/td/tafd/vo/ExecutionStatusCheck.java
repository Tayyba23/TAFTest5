/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author Muhammad Bilal
 *
 */
public class ExecutionStatusCheck {
	private String query;

	/**
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query
	 *            the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ExecutionStatusCheck [query=").append(query).append("]");
		return builder.toString();
	}
}
