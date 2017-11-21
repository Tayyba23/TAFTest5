/**
 * 
 */
package com.td.tafd.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.td.tafd.core.JobTypeParser;

/**
 * @author kt186036
 *
 */
public class ExceptionConfiguration 
{
	private String invalidInformationCode;
	private String missingInformationCode;
	private String objectNotFoundCode;
	private String accessViolationCode;
	
	public String getInvalidInformationCode() {
		return invalidInformationCode;
	}
	public void setInvalidInformationCode(String invalidInformationCode) {
		this.invalidInformationCode = invalidInformationCode;
	}
	public String getMissingInformationCode() {
		return missingInformationCode;
	}
	public void setMissingInformationCode(String missingInformationCode) {
		this.missingInformationCode = missingInformationCode;
	}
	public String getObjectNotFoundCode() {
		return objectNotFoundCode;
	}
	public void setObjectNotFoundCode(String objectNotFoundCode) {
		this.objectNotFoundCode = objectNotFoundCode;
	}
	public String getAccessViolationCode() {
		return accessViolationCode;
	}
	public void setAccessViolationCode(String accessViolationCode) {
		this.accessViolationCode = accessViolationCode;
	}
	public void setExceptionConfiguration()
	{
		Properties props = null;
		try {
			InputStream is = new FileInputStream( new File(System.getProperty("user.dir").toString() + JobTypeParser.getFileSeparator() + "src" + JobTypeParser.getFileSeparator() + "main" + JobTypeParser.getFileSeparator() + "resources" + JobTypeParser.getFileSeparator() + "exception_config.properties"));
			props = new Properties();
			props.load(is);
			is.close();
			
			setInvalidInformationCode(props.getProperty("invalid_information_code"));
			setMissingInformationCode(props.getProperty("missing_information_code"));
			setObjectNotFoundCode(props.getProperty("object_not_found_code"));
			setAccessViolationCode(props.getProperty("access_violation_code"));
			
		} catch (FileNotFoundException e) {
		} catch (IOException e) {
		}
	}
	
}
