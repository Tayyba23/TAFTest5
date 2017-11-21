/**
 * 
 */
package com.td.tafd.configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.td.tafd.core.JobTypeParser;
import com.td.tafd.parsers.excel.ExcelReader;

/**
 * @author kt186036
 *
 */

public class UserConfiguration 
{
	private String inputFilePath;
	
	private String bteqFileExts;
	private String fastloadFileExts;
	private String multiloadFileExts;
	private String tptFileExts;
	
	private String hostname;
	private String databaseName;
	private String username;
	private String password;
	
	private String runModules;
	
	private String businessDate;
	private String environment;
	
	private String serverAccessUsername;
	private String serverAccessPassword;
	
	private String releasePackageId;
	private String resetTestCycle;
	
	private String createResultTables;
	private boolean continueExecuting;
	
	/**
	 * @return the continueExecuting
	 */
	public boolean isContinueExecuting() {
		return continueExecuting;
	}
	/**
	 * @param continueExecuting the continueExecuting to set
	 */
	public void setContinueExecuting(boolean continueExecuting) {
		this.continueExecuting = continueExecuting;
	}
	public String getInputFilePath() {
		return inputFilePath;
	}
	public void setInputFilePath(String inputFilePath) {
		this.inputFilePath = inputFilePath;
	}
	public String getBteqFileExts() {
		return bteqFileExts;
	}
	public void setBteqFileExts(String bteqFileExts) {
		this.bteqFileExts = bteqFileExts;
	}
	public String getFastloadFileExts() {
		return fastloadFileExts;
	}
	public void setFastloadFileExts(String fastloadFileExts) {
		this.fastloadFileExts = fastloadFileExts;
	}
	public String getMultiloadFileExts() {
		return multiloadFileExts;
	}
	public void setMultiloadFileExts(String multiloadFileExts) {
		this.multiloadFileExts = multiloadFileExts;
	}
	public String getTptFileExts() {
		return tptFileExts;
	}
	public void setTptFileExts(String tptFileExts) {
		this.tptFileExts = tptFileExts;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getRunModules() {
		return runModules;
	}
	public void setRunModules(String runModules) {
		this.runModules = runModules;
	}
	public String getBusinessDate() {
		return businessDate;
	}
	public void setBusinessDate(String businessDate) {
		this.businessDate = businessDate;
	}
	public String getServerAccessUsername() {
		return serverAccessUsername;
	}
	public void setServerAccessUsername(String serverAccessUsername) {
		this.serverAccessUsername = serverAccessUsername;
	}
	public String getServerAccessPassword() {
		return serverAccessPassword;
	}
	public void setServerAccessPassword(String serverAccessPassword) {
		this.serverAccessPassword = serverAccessPassword;
	}
	public String getReleasePackageId() {
		return releasePackageId;
	}
	public void setReleasePackageId(String releasePackageId) {
		this.releasePackageId = releasePackageId;
	}
	public String getResetTestCycle() {
		return resetTestCycle;
	}
	public void setResetTestCycle(String resetTestCycle) {
		this.resetTestCycle = resetTestCycle;
	}
	
	/**
	 * @return the createResultTables
	 */
	public String getCreateResultTables() {
		return createResultTables;
	}
	/**
	 * @param createResultTables the createResultTables to set
	 */
	public void setCreateResultTables(String createResultTables) {
		this.createResultTables = createResultTables;
	}
	public String getEnvironment() {
		return environment;
	}
	public void setEnvironment(String environment) {
		this.environment = environment;
	}
	
	public UserConfiguration(){
	}
	
	public String getProperty(String propertyName)
	{
		String cellValue = null;
		Workbook wb = null;
		
		try {
		ExcelReader rd = new ExcelReader();
		
		if(inputFilePath.endsWith("xlsx") || inputFilePath.endsWith("xlsm"))
			wb = new XSSFWorkbook(new FileInputStream(inputFilePath));
		else
			wb = new HSSFWorkbook(new FileInputStream(inputFilePath));
		
		Sheet sheet = wb.getSheet("Configuration Parameters");
		
		cellValue = sheet.getRow(rd.getRowId("Configuration Parameter", propertyName, sheet)).getCell(1).getStringCellValue().trim();
		
		} catch(FileNotFoundException e) {
		} catch (IOException e) {
		} finally {
			try {
				wb.close();
			} catch (IOException e) {
			}
		}
		return cellValue;
	}
	
	public void setUserConfiguration() {
		try
		{	
			String inputFilePath = System.getProperty("user.dir").toString() + JobTypeParser.getFileSeparator() + "Input Parameter Files" + JobTypeParser.getFileSeparator() + "Test Automation Framework Metadata.xlsm";
			
			//System.out.println("inputFilePath: " + inputFilePath);
			
			ExcelReader rd = new ExcelReader();
			
			Workbook wb = null;
			if(inputFilePath.endsWith("xlsx") || inputFilePath.endsWith("xlsm"))
				wb = new XSSFWorkbook(new FileInputStream(new File(inputFilePath)));
			else
				wb = new HSSFWorkbook(new FileInputStream(new File(inputFilePath)));
			
			Sheet sheet = wb.getSheet("Configuration Parameters");
			
			setInputFilePath(inputFilePath);
			setBteqFileExts(sheet.getRow(rd.getRowId("Configuration Parameter", "BTEQ File Extensions", sheet)).getCell(1).getStringCellValue().trim());
			setFastloadFileExts(sheet.getRow(rd.getRowId("Configuration Parameter", "FASTLOAD File Extensions", sheet)).getCell(1).getStringCellValue().trim());
			setMultiloadFileExts(sheet.getRow(rd.getRowId("Configuration Parameter", "MULTILOAD File Extensions", sheet)).getCell(1).getStringCellValue().trim());
			setTptFileExts(sheet.getRow(rd.getRowId("Configuration Parameter", "TPT File Extensions", sheet)).getCell(1).getStringCellValue().trim());
			setHostname(sheet.getRow(rd.getRowId("Configuration Parameter", "Hostname", sheet)).getCell(1).getStringCellValue().trim());
			setDatabaseName(sheet.getRow(rd.getRowId("Configuration Parameter", "Database", sheet)).getCell(1).getStringCellValue().trim());
			setUsername(sheet.getRow(rd.getRowId("Configuration Parameter", "Username", sheet)).getCell(1).getStringCellValue().trim());
			
			//System.out.println("Password row: " + rd.getRowId("Configuration Parameter", "Password", sheet));
			//System.out.println("Password: " + sheet.getRow(rd.getRowId("Configuration Parameter", "Password", sheet)).getCell(1).getStringCellValue().trim());
			
			setPassword(sheet.getRow(rd.getRowId("Configuration Parameter", "Password", sheet)).getCell(1).getStringCellValue().trim());
			
			String modules = "";
			Cell moduleCell = sheet.getRow(rd.getRowId("Configuration Parameter", "Modules To Run", sheet)).getCell(1);
			
			switch(moduleCell.getCellTypeEnum())
			{
				case NUMERIC:
					modules = "" + (int)moduleCell.getNumericCellValue();
					break;
					
				case STRING:
					modules = moduleCell.getStringCellValue();
					break;
					
				default:
					break;
					
			}
			
			setRunModules(modules.trim());
			setBusinessDate(new DataFormatter().formatCellValue(sheet.getRow(rd.getRowId("Configuration Parameter", "Business Date", sheet)).getCell(1)).trim());
			setEnvironment(sheet.getRow(rd.getRowId("Configuration Parameter", "Environment", sheet)).getCell(1).getStringCellValue().trim());
			setServerAccessUsername(sheet.getRow(rd.getRowId("Configuration Parameter", "Server Access Username", sheet)).getCell(1).getStringCellValue().trim());
			setServerAccessPassword(sheet.getRow(rd.getRowId("Configuration Parameter", "Server Access Password", sheet)).getCell(1).getStringCellValue().trim());
			setReleasePackageId(sheet.getRow(rd.getRowId("Configuration Parameter", "Release Package ID", sheet)).getCell(1).getStringCellValue().trim());
			setResetTestCycle(sheet.getRow(rd.getRowId("Configuration Parameter", "Reset Test Cycle", sheet)).getCell(1).getStringCellValue().trim());
			setCreateResultTables(sheet.getRow(rd.getRowId("Configuration Parameter", "Create Result Tables", sheet)).getCell(1).getStringCellValue().trim());
			setContinueExecuting(sheet.getRow(rd.getRowId("Configuration Parameter", "Continue Execution If Objects Do Not Exist", sheet)).getCell(1).getStringCellValue().trim().equalsIgnoreCase("T"));
			
			wb.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
