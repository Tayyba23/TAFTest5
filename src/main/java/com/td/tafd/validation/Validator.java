package com.td.tafd.validation;

import java.io.File;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.constants.ConstantsInterface;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.parsers.excel.ExcelReader;

public class Validator implements ConstantsInterface
{
	private static Validator validator = null;
	
	private Validator(){
	}
	
	public static Validator getInstance()
	{
		if(validator == null)
			validator = new Validator();
		return validator;
	}
	
	public boolean validateIP(String ipAddress)				// validates whether IP address passed to it is valid or not
	{
		JobTypeParser.getApplicationlogger().info("In function \'validateIP\', parameter: \'ipAddress\' = " + ipAddress);
		if(ipAddress == null || ipAddress.isEmpty())
		{
			JobTypeParser.getApplicationlogger().info("In function \'validateIP\', returning \'false\'");
			return false;
		}
		
		try {
			Object checkIP = InetAddress.getByName(ipAddress);
			JobTypeParser.getApplicationlogger().info("In function \'validateIP\', returning \'" + (checkIP instanceof Inet4Address || checkIP instanceof Inet6Address) + "\'");
			return ( checkIP instanceof Inet4Address || checkIP instanceof Inet6Address);
		} catch (UnknownHostException e) {
			JobTypeParser.getApplicationlogger().info("In function \'validateIP\', returning \'false\'");
			return false;
		}
	}
	
	/**
	 * Validates that the Stream_Id and Sub_Stream_Id columns are numeric in all relevant tabs in the Input Metadata Sheet
	 * @return true if and only if all Stream_Id and Sub_Stream_Id columns are numeric in each tab of the Metadata Sheet 
	 */
	
	public boolean validateAllStreams()
	{
		JobTypeParser.getApplicationlogger().info("In function \'validateAllStreams\'");
		boolean pcStreamsAreNumeric = false;
		boolean ddStreamsAreNumeric = false;
		boolean riStreamsAreNumeric = false;
		boolean hhStreamsAreNumeric = false;
		
		for(int i=0; i < JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getLastRowNum(); ++i)
		{
			if(JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(i+1) == null 
					|| ((JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(i+1).getCell(0) == null) || (JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(i+1).getCell(0).getCellTypeEnum().equals(CellType.BLANK))))
				break;
			pcStreamsAreNumeric = validateStreamsAreNumeric(JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection"), i);
			if(!pcStreamsAreNumeric)	// if a single entry is non-numeric, break from loop
				break;
		}
		
		for(int i=0; i < JobTypeParser.getWorkbook().getSheet("Duplicate Data Verification").getLastRowNum(); ++i)
		{
			if(JobTypeParser.getWorkbook().getSheet("Duplicate Data Verification").getRow(i+1) == null || JobTypeParser.getWorkbook().getSheet("Duplicate Data Verification").getRow(i+1).getCell(0) == null || JobTypeParser.getWorkbook().getSheet("Duplicate Data Verification").getRow(i+1).getCell(0).getCellTypeEnum().equals(CellType.BLANK))
			{
				//System.out.println("breaking from loop. i= " + i);
				break;
			}
			ddStreamsAreNumeric = validateStreamsAreNumeric(JobTypeParser.getWorkbook().getSheet("Duplicate Data Verification"), i);
			if(!ddStreamsAreNumeric)	// if a single entry is non-numeric, break from loop
				break;
		}
		
		for(int i=0; i < JobTypeParser.getWorkbook().getSheet("RI Verification").getLastRowNum(); ++i)
		{
			if(JobTypeParser.getWorkbook().getSheet("RI Verification").getRow(i+1) == null || JobTypeParser.getWorkbook().getSheet("RI Verification").getRow(i+1).getCell(0).getCellTypeEnum().equals(CellType.BLANK))
				break;
			riStreamsAreNumeric = validateStreamsAreNumeric(JobTypeParser.getWorkbook().getSheet("RI Verification"), i);
			if(!riStreamsAreNumeric)	// if a single entry is non-numeric, break from loop
				break;
		}
		
		for(int i=0; i < JobTypeParser.getWorkbook().getSheet("History Verification").getLastRowNum(); ++i)
		{
			if(JobTypeParser.getWorkbook().getSheet("History Verification").getRow(i+1) == null || JobTypeParser.getWorkbook().getSheet("History Verification").getRow(i+1).getCell(0) == null || JobTypeParser.getWorkbook().getSheet("History Verification").getRow(i+1).getCell(0).getCellTypeEnum().equals(CellType.BLANK))
				break;
			hhStreamsAreNumeric = validateStreamsAreNumeric(JobTypeParser.getWorkbook().getSheet("History Verification"), i);
			if(!hhStreamsAreNumeric)	// if a single entry is non-numeric, break from loop
				break;
		}
		
		/*System.out.println("pcStreamsAreNumeric: " + pcStreamsAreNumeric);
		System.out.println("ddStreamsAreNumeric: " + ddStreamsAreNumeric);
		System.out.println("riStreamsAreNumeric: " + riStreamsAreNumeric);
		System.out.println("hhStreamsAreNumeric: " + hhStreamsAreNumeric);*/
		
		JobTypeParser.getApplicationlogger().info("Returning : " + (pcStreamsAreNumeric && ddStreamsAreNumeric && riStreamsAreNumeric && hhStreamsAreNumeric));
		return (pcStreamsAreNumeric && ddStreamsAreNumeric && riStreamsAreNumeric && hhStreamsAreNumeric);
	}
	
	/**
	 * Validates that the Stream_Id and Sub_Stream_Id columns are numeric in current tab in the Input Metadata Sheet
	 * @return true if and only if all Stream_Id and Sub_Stream_Id columns are numeric in current tab
	 */
	
	public boolean validateStreamsAreNumeric(Sheet sheet, int i)
	{	
		JobTypeParser.getApplicationlogger().info("In function \'validateInputStreams\', parameters: \'sheet\' = " + sheet.getSheetName() + ", \'i\' = " + i);
		
		boolean streamIsNumeric = false;
		boolean subStreamIsNumeric = false;
		
		try {
			if(NumberUtils.isNumber(sheet.getRow(i+1).getCell(0).getStringCellValue()))
				streamIsNumeric = false;
		} catch(IllegalStateException e) {
			streamIsNumeric = true;
		}
		try {
			if(NumberUtils.isNumber(sheet.getRow(i+1).getCell(1).getStringCellValue()))
				subStreamIsNumeric = false;
		} catch(IllegalStateException e) {
			subStreamIsNumeric = true;
		}
		
		JobTypeParser.getApplicationlogger().info("Returning : " + (streamIsNumeric && subStreamIsNumeric));
		
		return (streamIsNumeric && subStreamIsNumeric);
	}
	
	/* -------------------------------------------------------------------------------------------------------------------------
	 * | Function Validates input Metadata file to verify all the required sheets and columns exist before beginning execution |
	 * ------------------------------------------------------------------------------------------------------------------------- */
	
	public boolean validate_input_file(String metadata_file_name)		
	{	
		JobTypeParser.getApplicationlogger().info("In function \'validate_input_file\', parameter: \'metadata_file_name\' = " + metadata_file_name);
		ExcelReader rd = new ExcelReader();
		
		Workbook wbook;
		wbook = rd.getRequiredWorkbook(metadata_file_name, metadata_file_name.endsWith("xls"));
		
		for(String sheet : necessarySheets)
		{
			if(!rd.sheetExists(wbook, sheet))
			{
				JobTypeParser.getLogger().error("Error: Missing Information - Required Sheet \'" + sheet + "\' not found in file \"" + metadata_file_name + "\"");
				JobTypeParser.getApplicationlogger().info("In function \'validate_input_file\', returning \'false\'");
				return false;
			}
		}
		
		Sheet p_check_sheet = wbook.getSheet("Row Count & Metrics Collection");
		Sheet ri_sheet = wbook.getSheet("RI Verification");
	
		for(String col : preliminaryCheckCols)
		{
			if(rd.getColId(col, p_check_sheet.getSheetName(), wbook) == -1)
			{
				JobTypeParser.getLogger().error("Error: Missing Information - Column \'" + col + "\' not found in Sheet \"" + p_check_sheet.getSheetName() + "\"");
				JobTypeParser.getApplicationlogger().info("In function \'validate_input_file\', returning \'false\'");
				return false;
			}
		}
		
		for(String col : riVerificationCols)
		{
			if(rd.getColId(col, ri_sheet.getSheetName(), wbook) == -1)
			{
				JobTypeParser.getLogger().error("Error: Missing Information - Column \'" + col + "\' not found in Sheet \"" + ri_sheet.getSheetName() + "\"");
				JobTypeParser.getApplicationlogger().info("In function \'validate_input_file\', returning \'false\'");
				return false;
			}
		}
		
		JobTypeParser.getApplicationlogger().info("In function \'validate_input_file\', returning \'true\'");
		return true;
	}
	
	public String [] getObjectInformation(char type, String objectType, String objName, int i)
	{
		JobTypeParser.getApplicationlogger().info("In function \'validateObjectPresence\', parameters: \'type\' = " + type + ", \'objectType\' = " + objectType + ", \'objName\' = " + objName + ", \'int i\' = " + i);
		String [] objectInfo = new String [3];
		
		JobTypeParser.getApplicationlogger().debug("Type: \'" + type + "\' for object " + objName);
		
		if(type == 't')
		{	
			if(objName.endsWith(".") || objName.startsWith("."))
			{
				JobTypeParser.getLogger().error("Error: Missing Information - " + objectType + " object name and/or path not found in input file. Please check " + objectType + "'s path and Name columns in row " + (i+1) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				objectInfo[0] = null;
			}
			
			else
			{
				objectInfo[0] = ".";
				objectInfo[1] = objName.split("\\.")[0];
				objectInfo[2] = objName.split("\\.")[1];	
			}
		}

		else if(type == 'f')
		{
			if(objName.endsWith("\\") || objName.startsWith("\\"))
			{
				JobTypeParser.getLogger().error("Error: Missing Information - " + objectType + " object name and/or path not found in input file. Please check " + objectType + "'s path and Name columns in row " + (i+1) + " of sheet \'Row Count & Metrics Collection\' in file \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				objectInfo[0] = null;
			}
			
			else
			{
				File temp_s = new File(objName);
				objectInfo[0] = "\\";
				objectInfo[1] = temp_s.getParent();
				objectInfo[2] = temp_s.getName();
			}
		}
		
		JobTypeParser.getApplicationlogger().info("In function \'validateObjectPresence\', returning \'objectInfo\'");
		return objectInfo;
	}
	
	public boolean isDateValid(String dateToValidate, String dateFormat)
	{
		if(dateToValidate == null){
			return false;
		}

		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		sdf.setLenient(false);

		try {
			//if not valid, it will throw ParseException
			sdf.parse(dateToValidate);
			//JobTypeParser.getLogger().info("Business Date Entered: " + date);

		} catch (ParseException e) {
			return false;
		}

		return true;
	}
	
	public static void printModuleCompletionPrompt(String moduleName, long start, long end, String status)
	{
		StringBuilder builder = new StringBuilder();
		String timeInMinAndSeconds = (((end - start) / 1000) / 60) + " minutes " + ((end - start) / 1000) + "." + ((end - start) % 1000);
		
		builder.append("--------------------------------------------------------------------------------------\n").append(
										"\t\t\t    |		" + moduleName.toUpperCase() + "			|\n").append(
										"\t\t\t    |	Start: " + new Timestamp(start) + "		|\n").append(
										"\t\t\t    |	End: " + new Timestamp(end) + "		|\n").append(
										"\t\t\t    |	Duration: " + timeInMinAndSeconds + " seconds	|\n").append(
										"\t\t\t    |	Status: " + status + "			|\n").append(
										"\t\t\t    --------------------------------------------------------------------------------------\n");
		
		JobTypeParser.getLogger().info(builder.toString());
	}
	
	public static void printModuleCompletionPrompt(String moduleName, long start, long end, String status, int numOfInputs)
	{
		StringBuilder builder = new StringBuilder();
		String timeInMinAndSeconds = (((end - start) / 1000) / 60) + " minutes " + ((end - start) / 1000) + "." + ((end - start) % 1000);
		
		builder.append("--------------------------------------------------------------------------------------\n").append(
										"\t\t\t    |		" + moduleName.toUpperCase() + "			|\n").append(
										"\t\t\t    |	Start: " + new Timestamp(start) + "		|\n").append(
										"\t\t\t    |	End: " + new Timestamp(end) + "		|\n").append(
										"\t\t\t    |	Duration: " + timeInMinAndSeconds + " seconds	|\n").append(
										"\t\t\t    |	Status: " + status + "			|\n").append(
										"\t\t\t    |	Inputs Processed: " + numOfInputs + "			|\n").append(
										"\t\t\t    --------------------------------------------------------------------------------------\n");
		
		JobTypeParser.getLogger().info(builder.toString());
	}
}
