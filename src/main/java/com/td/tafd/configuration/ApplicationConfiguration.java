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
import com.td.tafd.vo.ConfigParams;

/**
 * @author kt186036
 *
 */

public class ApplicationConfiguration 
{
	private String streamId;
	private String subStreamId;
	
	private String userId;
	
	private String testCycleMinor;
	private String testCycle;

	private String execCheck;
	private String objZeroRowCount;
	private String scriptZeroRowCount;
	private String rowCount;
	private String distinctCount;
	private String sumAvg;
	private String minMax;
	private String riVer;
	private String nullValue;
	private String pkDup;
	private String historyOverlap;
	private String historyGap;
	private String historyReverse;
	private String minusTest;
	private String surrogateKeyTest;
	
	private String allTdHosts;
	private String allTdUsers;
	private String allTdPasswords;
	
	public ApplicationConfiguration(){
	}
	
	public String getTestCycleMinor() {
		return testCycleMinor;
	}
	public void setTestCycleMinor(String testCycleMinor) {
		this.testCycleMinor = testCycleMinor;
	}
	public String getTestCycle() {
		return testCycle;
	}
	public void setTestCycle(String testCycle) {
		this.testCycle = testCycle;
	}
	public String getExecCheck() {
		return execCheck;
	}
	public void setExecCheck(String execCheck) {
		this.execCheck = execCheck;
	}
	public String getObjZeroRowCount() {
		return objZeroRowCount;
	}
	public void setObjZeroRowCount(String objZeroRowCount) {
		this.objZeroRowCount = objZeroRowCount;
	}
	public String getScriptZeroRowCount() {
		return scriptZeroRowCount;
	}
	public void setScriptZeroRowCount(String scriptZeroRowCount) {
		this.scriptZeroRowCount = scriptZeroRowCount;
	}
	public String getRowCount() {
		return rowCount;
	}
	public void setRowCount(String rowCount) {
		this.rowCount = rowCount;
	}
	public String getDistinctCount() {
		return distinctCount;
	}
	public void setDistinctCount(String distinctCount) {
		this.distinctCount = distinctCount;
	}
	public String getSumAvg() {
		return sumAvg;
	}
	public void setSumAvg(String sumAvg) {
		this.sumAvg = sumAvg;
	}
	public String getMinMax() {
		return minMax;
	}
	public void setMinMax(String minMax) {
		this.minMax = minMax;
	}
	public String getRiVer() {
		return riVer;
	}
	public void setRiVer(String riVer) {
		this.riVer = riVer;
	}
	public String getNullValue() {
		return nullValue;
	}
	public void setNullValue(String nullValue) {
		this.nullValue = nullValue;
	}
	public String getPkDup() {
		return pkDup;
	}
	public void setPkDup(String pkDup) {
		this.pkDup = pkDup;
	}
	public String getHistoryOverlap() {
		return historyOverlap;
	}
	public void setHistoryOverlap(String historyOverlap) {
		this.historyOverlap = historyOverlap;
	}
	public String getHistoryGap() {
		return historyGap;
	}
	public void setHistoryGap(String historyGap) {
		this.historyGap = historyGap;
	}
	public String getHistoryReverse() {
		return historyReverse;
	}
	public void setHistoryReverse(String historyReverse) {
		this.historyReverse = historyReverse;
	}
	public String getHistoryCase(String caseType) {
		switch (caseType)
		{
			case "gap":
				return getHistoryGap();
			case "overlap":
				return getHistoryOverlap();
			case "reverse":
				return getHistoryReverse();
			default:
				return "";
		}
	}
	public String getAllTdHosts() {
		return allTdHosts;
	}
	public void setAllTdHosts(String allTdHosts) {
		this.allTdHosts = allTdHosts;
	}
	public String getAllTdUsers() {
		return allTdUsers;
	}
	public void setAllTdUsers(String allTdUsers) {
		this.allTdUsers = allTdUsers;
	}
	public String getAllTdPasswords() {
		return allTdPasswords;
	}
	public void setAllTdPasswords(String allTdPasswords) {
		this.allTdPasswords = allTdPasswords;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getStreamId() {
		return streamId;
	}
	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}
	public String getSubStreamId() {
		return subStreamId;
	}
	public void setSubStreamId(String subStreamId) {
		this.subStreamId = subStreamId;
	}
	/**
	 * @return the minusTest
	 */
	public String getMinusTest() {
		return minusTest;
	}

	/**
	 * @param minusTest the minusTest to set
	 */
	public void setMinusTest(String minusTest) {
		this.minusTest = minusTest;
	}
	
	/**
	 * @return the surrogateKeyTest
	 */
	
	public String getSurrogateKeyTest() {
		return surrogateKeyTest;
	}

	/**
	 * @param surrogateKeyTest the surrogateKeyTest to set
	 */
	
	public void setSurrogateKeyTest(String surrogateKeyTest) {
		this.surrogateKeyTest = surrogateKeyTest;
	}
	
	public String getProperty(String propertyName)
	{
		Properties props = null;
		
		try {
			InputStream is = new FileInputStream( new File(System.getProperty("user.dir").toString() + JobTypeParser.getFileSeparator() + "src" + JobTypeParser.getFileSeparator() + "main" + JobTypeParser.getFileSeparator() + "resources" + JobTypeParser.getFileSeparator() + "app_config.properties"));
			props = new Properties();
			props.load(is);
			is.close();
		} catch (FileNotFoundException e) {
			
		} catch (IOException e) {
		}
		
		return (props == null) ? "" : props.getProperty(propertyName);
	}
	
	public void setApplicationConfiguration() {
		try 
		{
			InputStream is = new FileInputStream( new File(System.getProperty("user.dir").toString() + JobTypeParser.getFileSeparator() + "src" + JobTypeParser.getFileSeparator() + "main" + JobTypeParser.getFileSeparator() + "resources" + JobTypeParser.getFileSeparator() + "app_config.properties"));
			Properties props = new Properties();
			props.load(is);
			is.close();

			setStreamId(props.getProperty("stream_id"));
			setSubStreamId(props.getProperty("sub_stream_id"));
			setUserId(props.getProperty("user_id"));
			setTestCycleMinor(props.getProperty("test_cycle_minor"));
			setTestCycle(props.getProperty("test_cycle"));
			setExecCheck(props.getProperty("exec_check"));
			setObjZeroRowCount(props.getProperty("obj_zero_row_count"));
			setScriptZeroRowCount(props.getProperty("script_zero_row_count"));
			setRowCount(props.getProperty("row_count"));
			setDistinctCount(props.getProperty("distinct_count"));
			setSumAvg(props.getProperty("sum_avg"));
			setMinMax(props.getProperty("min_max"));
			setRiVer(props.getProperty("ri_ver"));
			setNullValue(props.getProperty("null_value"));
			setPkDup(props.getProperty("pk_dup"));
			setHistoryOverlap(props.getProperty("history_overlap"));
			setHistoryGap(props.getProperty("history_gap"));
			setHistoryReverse(props.getProperty("history_reverse"));
			setMinusTest(props.getProperty("minus_test"));
			setSurrogateKeyTest(props.getProperty("surrogate_key_test"));
			
			for(ConfigParams cp : JobTypeParser.getConfigParameters())
			{
				if(cp.getConfigParameter().equalsIgnoreCase("All Teradata Hosts"))
					setAllTdHosts(cp.getValue());
				if(cp.getConfigParameter().equalsIgnoreCase("All Teradata Users"))
					setAllTdUsers(cp.getValue());
				if(cp.getConfigParameter().equalsIgnoreCase("All Teradata Passwords"))
					setAllTdPasswords(cp.getValue());
			}
		
		} catch (FileNotFoundException e) {
			JobTypeParser.getLogger().error("File (Metadata Sheet) not found");
		} catch (IOException e) {
			JobTypeParser.getApplicationlogger().error("IOException encountered in class ApplicationConfiguration");
		}
	}
}
