package com.td.tafd.vo;

/**
 * @author Muhammad Bilal
 *
 */
public class ConfigParams {
	private String configParameter;
	private String value;

	/**
	 * @return the configParameter
	 */
	public String getConfigParameter() {
		return configParameter;
	}

	/**
	 * @param configParameter
	 *            the configParameter to set
	 */
	public void setConfigParameter(String configParameter) {
		this.configParameter = configParameter;
	}

	/**
	 * @return the value
	 */
	public String getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ConfigParams [configParameter=").append(configParameter).append(", value=").append(value)
				.append("]");
		return builder.toString();
	}
}
