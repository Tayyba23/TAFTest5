/**
 * 
 */
package com.td.tafd.vo;

/**
 * @author Muhammad Bilal
 *
 */
public class LicenseModel {
	private String userId;
	private String licenseKey;
	private String regKey;
	private boolean isLicenseValid;
	
	public LicenseModel() {
		userId = "";
		licenseKey = "";
		regKey = "";
		isLicenseValid = false;
	}
	
	public boolean isLicenseValid() {
		return isLicenseValid;
	}

	public void setLicenseValid(boolean isLicenseValid) {
		this.isLicenseValid = isLicenseValid;
	}

	public String getUserId() {
		return userId;
	}
	
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	public String getLicenseKey() {
		return licenseKey;
	}
	
	public void setLicenseKey(String licenseKey) {
		this.licenseKey = licenseKey.replaceAll("[\n\r]", "");
	}
	
	public String getRegKey() {
		return regKey;
	}
	
	public void setRegKey(String regKey) {
		this.regKey = regKey.replaceAll("[\n\r]", "");
	}
}
