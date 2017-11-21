
/*
* @(#)Excel_Reader.java 1.0 03/30/2017
* Copyright (c) 2016-2017
*/

package com.td.tafd.parsers.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.td.tafd.constants.AppConstants;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.vo.ColumnMapping;
import com.td.tafd.vo.ConfigParams;
import com.td.tafd.vo.DuplicateDataVerification;
import com.td.tafd.vo.DuplicateRecordData;
import com.td.tafd.vo.ExecutionStatusCheck;
import com.td.tafd.vo.HistoryTestDetail;
import com.td.tafd.vo.HistoryVerification;
import com.td.tafd.vo.LookupValues;
import com.td.tafd.vo.MinusTest;
import com.td.tafd.vo.MinusTestColumnMapping;
import com.td.tafd.vo.PreliminaryChecks;
import com.td.tafd.vo.RIVerification;
import com.td.tafd.vo.ReleasePackageInfo;
import com.td.tafd.vo.SurrogateKey;
import com.td.tafd.vo.TableAndDuplicateTypeInfo;
import com.td.tafd.vo.TechnicalColumnInfo;
import com.td.tafd.vo.TestScriptExecutionInfo;


/**
 * Description
 * 
 * @author <a href="mailto:kanwal.tariq@teradata.com">Kanwal Tariq</a>
 * @version 1.0
 * @see <a href=”spec.html#section”>Java Spec</a>
 */

public class ExcelReader {
	private List<String> sources;
	private List<String> targets;

	public ExcelReader() {
		sources = new ArrayList<String>();
		targets = new ArrayList<String>();
	}

	public void setSources(List<String> values) {
		sources = values;
	}

	public List<String> getSources() {
		return sources;
	}

	public void setTargets(List<String> values) {
		targets = values;
	}

	public List<String> getTargets() {
		return targets;
	}

	public Workbook getRequiredWorkbook(String path, boolean xls) {
		JobTypeParser.getApplicationlogger()
				.info("In function \'getRequiredWorkbook\', parameters: \'path\' = " + path + ", \'xls\' = " + xls);

		try {
			if (!new File(path).exists())
				return null;

			if (path.endsWith("xlsx") || path.endsWith("xlsm")) { // Handling for newer file formats in Excel
				xls = false;
				JobTypeParser.getApplicationlogger()
						.info("In function \'getRequiredWorkbook\', returning XSSFWorkbook");
				return new XSSFWorkbook(new FileInputStream(path));
			}

			else if (path.endsWith("xls")) { // Handling for older Excel formats
				xls = true;
				JobTypeParser.getApplicationlogger()
						.info("In function \'getRequiredWorkbook\', returning HSSFWorkbook");
				return new HSSFWorkbook(new FileInputStream(path));
			}

			else { // Incorrect file extension handling
				JobTypeParser.getLogger().debug("\nUnexpected file extension - Not recognized as an Excel file\n");
				// throw new IllegalArgumentException("The specified file is not an Excel file");
			}
		}

		catch (IOException e) {
			e.printStackTrace();
		}

		JobTypeParser.getApplicationlogger().info("In function \'getRequiredWorkbook\', returning null");
		return null;
	}

	/**
	 * Takes the name of the heading of a column and returns all the values in that column as an ArrayList
	 */

	public List<String> readColumnWithHeading(String colName, Sheet currSheet, int iteration, int s_or_t) {
		JobTypeParser.getApplicationlogger()
				.info("In function \'readColumnWithHeading\', parameters: \'colName\' = " + colName
						+ ", \'currSheet\' = " + currSheet.getSheetName() + ", \'iteration\' = " + iteration
						+ ", \'s_or_t\' = " + s_or_t);

		List<String> appenders = new ArrayList<String>();
		List<String> column_values = new ArrayList<String>();

		if (colName.toUpperCase().contains("Source_Name".toUpperCase())) {
			appenders = readColumnWithHeading("Source_Path", currSheet, 1, 1);
		}

		else if (colName.toUpperCase().contains("Target_Name".toUpperCase())) {
			appenders = readColumnWithHeading("Target_Path", currSheet, 1, 2);
		}

		Iterator<Row> rowIterator = currSheet.iterator(); // rowIterator allows for row-wise reading of the file

		Row headingRow = rowIterator.next();
		Cell cell;

		int colIndex = -1;

		Iterator<Cell> headingRowIterator = headingRow.cellIterator(); // cellIterator for row

		while (headingRowIterator.hasNext()) {
			cell = headingRowIterator.next();

			if (cell.getStringCellValue().trim().equals(colName)) {
				colIndex = cell.getColumnIndex();
				break;
			}
		}

		if (colIndex == -1) {
			JobTypeParser.getLogger().debug("Column name " + colName + " not found in current sheet");
			return column_values;
		}

		Row dataRow = null;
		String col_val = "";
		int i = 0;

		while (rowIterator.hasNext()) {
			dataRow = rowIterator.next();
			if(dataRow.getCell(0) == null)
				continue;

			if (dataRow.getLastCellNum() <= colIndex)
				break;

			// System.out.println(colIndex);
			Cell val = dataRow.getCell(colIndex);
			if (val == null)
				continue;
			col_val = val.getStringCellValue().trim();

			// if called for 'source path' and/or 'target path' columns, and source and target path
			// need to be appended to them
			if (iteration == 0) {
				int col_id = 0;

				if (s_or_t == 1)
					col_id = getColId("Source_Path", currSheet.getSheetName(), JobTypeParser.getWorkbook());

				else if (s_or_t == 2)
					col_id = getColId("Target_Path", currSheet.getSheetName(), JobTypeParser.getWorkbook());

				if (getObjectType(dataRow.getCell(col_id).getStringCellValue()) == 't')
					column_values.add(appenders.get(i) + "." + col_val);

				else if (getObjectType(dataRow.getCell(col_id).getStringCellValue()) == 'f')
					column_values.add(appenders.get(i) + "\\" + col_val);
			}

			else // if called for any other column
			{
				column_values.add(col_val);
			}

			++i;
		}

		JobTypeParser.getApplicationlogger()
				.info("In function \'readColumnWithHeading\', returning: column_values = " + column_values);
		return column_values;
	}

	/**
	 * returns the type ('f' for file, 't' for table or view)
	 */

	public char getObjectType(String obj_name) {
		char obj_type = 'x'; // initializing with irrelevant value

		File f = new File(obj_name);

		if (f.isDirectory())
			obj_type = 'f';

		else {
			if (!f.isDirectory())
				f = f.getParentFile();

			if (f == null) // obj_name was neither directory, nor file so
							// f.getParentFile() returned null
				obj_type = 't';

			else // otherwise obj_name was not directory, but was a file as
					// f.getParentFile() did not return null
			{
				if (f.exists())
					obj_type = 'f';

				else {
					try {
						new URL(f.toString());
						obj_type = 'f';
					}

					catch (MalformedURLException e) {
						// e.printStackTrace();
						obj_type = 't';
					}
				}
			}
		}

		JobTypeParser.getApplicationlogger().info("In function \'getObjectType\', parameters: \'obj_name\' = "
				+ obj_name + ", returning \'obj_type\' = " + obj_type);
		return obj_type;
	}

	/**
	 * Sets the Sources and Targets for Data Integrity Tests
	 * @param file_name
	 */
	
	public void setSourcesAndTargets(String file_name) {
		int sheet_index = -1;
		Workbook wb = getRequiredWorkbook(file_name, file_name.endsWith("xls"));

		if (wb.equals(null))
			return;

		for (int i = 0; i < wb.getNumberOfSheets(); ++i) {
			if (wb.getSheetName(i).toUpperCase().equals("Row Count & Metrics Collection".toUpperCase())) {
				sheet_index = i;
				break;
			}
		}

		if (sheet_index == -1) {
			JobTypeParser.getLogger()
					.debug("Sheet \"Row Count & Metrics Collection\" not found in file \"" + new File(file_name).getName() + "\"");
			return;
		}

		Sheet sheet = wb.getSheetAt(sheet_index);

		setSources(readColumnWithHeading("Source_Name", sheet, 0, 1));
		setTargets(readColumnWithHeading("Target_Name", sheet, 0, 2));
	}

	/**
	 * checks if a sheet by the name passed to the function exists in the workbook | 
	 * @return true if sheet exists, false otherwise
	 */

	public boolean sheetExists(Workbook wbook, String sheet_name) {
		JobTypeParser.getApplicationlogger().info("In function \'sheetExists\', parameters: \'Workbook\' = " + wbook
				+ ", \'sheet_name\' = " + sheet_name);
		int total_sheets = wbook.getNumberOfSheets();

		for (int i = 0; i < total_sheets; ++i) {
			if (wbook.getSheetAt(i).getSheetName().trim().toUpperCase().equals(sheet_name.toUpperCase())) {
				JobTypeParser.getApplicationlogger().info("In function \'sheetExists\', returning \'true\'");
				return true;
			}
		}

		JobTypeParser.getApplicationlogger().info("In function \'sheetExists\', returning \'false\'");
		return false;
	}

	/**
	 * returns the column index of the column name passed as a parameter
	 */

	public int getColId(String colName, String sheetName, Workbook wb) {
		JobTypeParser.getApplicationlogger().info("In function \'getColId\', parameters: \'colName\' = " + colName
				+ "\'sheetName\' = " + sheetName + "\'Workbook\' = " + wb + ", ");
		int col_id = -1;
		Iterator<Row> rowIterator = wb.getSheet(sheetName).iterator(); // rowIterator allows or row-wise reading of the file

		Row headingRow = rowIterator.next();
		Cell cell;

		Iterator<Cell> headingRowIterator = headingRow.cellIterator(); // cellIterator for row

		while (headingRowIterator.hasNext()) {
			cell = headingRowIterator.next();
			if (cell.getStringCellValue().trim().equals(colName)) {
				col_id = cell.getColumnIndex();
				break;
			}
		}

		JobTypeParser.getApplicationlogger()
				.info("In function \'getColId\', returning Column Index = \'" + col_id + "\'");
		return col_id;
	}

	/**
	 * returns the row index of the column name passed as a parameter
	 */
	
	public int getRowId(String column, String value, Sheet sheet) {
		List<String> allValues = readColumnWithHeading(column, sheet, 1, 0);

		return (allValues.indexOf(value)) + 1;
	}
	
	public Map<String, Object> readExcelData(Workbook workbook) {
		
		Map<String, Object> excelData = new HashMap<>();
		Sheet currentSheet = null;
		String sheetName = null;
		
		// starting index of the loop from 1 to skip the first sheet.
		for(int index = 1; index < (workbook.getNumberOfSheets() - 2); index++) {
			currentSheet = workbook.getSheetAt(index);
			sheetName = currentSheet.getSheetName();
			if(sheetName.equalsIgnoreCase(AppConstants.CONFIGURATION_PARAMETERS)) {
				excelData.put(AppConstants.CONFIGURATION_PARAMETERS, getConfigParams(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.DUPLICATE_DATA_VERIFICATION)) {
				excelData.put(AppConstants.DUPLICATE_DATA_VERIFICATION, getDuplicateData(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.EXECUTION_STATUS_CHECK)) {
				excelData.put(AppConstants.EXECUTION_STATUS_CHECK, getExecutionStatusCheck(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.HISTORY_VERIFICATION)) {
				excelData.put(AppConstants.HISTORY_VERIFICATION, getHistoryDataList(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.LOOKUP_VALUES)) {
				excelData.put(AppConstants.LOOKUP_VALUES, getLookUpValues(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.PRELIMINARY_CHECKS)) {
				excelData.put(AppConstants.PRELIMINARY_CHECKS, getPreliminaryChecks(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.RELEASE_PACKAGE_INFORMATION)) {
				excelData.put(AppConstants.RELEASE_PACKAGE_INFORMATION, getPackageInfo(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.RI_VERIFICATION)) {
				excelData.put(AppConstants.RI_VERIFICATION, getRIList(currentSheet));
			} else if(sheetName.equalsIgnoreCase(AppConstants.TECHNICAL_COLUMNS)) {
				excelData.put(AppConstants.TECHNICAL_COLUMNS, getTechnicalInfoList(currentSheet));
			}			
		}
		
		return excelData;
	}
	
	public List<ConfigParams> getConfigParams(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		ConfigParams configParams = null;
		List<ConfigParams> paramsList = new ArrayList<>();
		//int i=0;
		String value = "";
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			value = "";		// reset value each time
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			//System.out.println(i);
			configParams = new ConfigParams();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			configParams.setConfigParameter(xssfCell.getRichStringCellValue().getString().trim());

			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;

			switch(xssfCell.getCellTypeEnum())
			{
				case NUMERIC:
					value = "" + xssfCell.getNumericCellValue();
					break;
				case STRING:
					value = xssfCell.getRichStringCellValue().getString().trim();
					break;
				case _NONE:
					break;
				default:
					break;
			}
			
			configParams.setValue(value.trim());
			
			paramsList.add(configParams);
		}
		
		return paramsList;
	}
	
	public List<ReleasePackageInfo> getPackageInfo(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		ReleasePackageInfo releaseInfo = null;
		List<ReleasePackageInfo> packageInfoList = new ArrayList<>();
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			releaseInfo = new ReleasePackageInfo();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			releaseInfo.setEnvID(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			releaseInfo.setDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			releaseInfo.setObjectName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			releaseInfo.setObjectType(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			releaseInfo.setStatus(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			releaseInfo.setProcessName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			releaseInfo.setProcessType(xssfCell.getRichStringCellValue().getString().trim());
			
			packageInfoList.add(releaseInfo);
		}
		
		return packageInfoList;
	}
	
	public List<ExecutionStatusCheck> getExecutionStatusCheck(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		ExecutionStatusCheck execCheck = null;
		List<ExecutionStatusCheck> statusesList = new ArrayList<>();
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			execCheck = new ExecutionStatusCheck();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			execCheck.setQuery(xssfCell.getRichStringCellValue().getString().trim());
			
			statusesList.add(execCheck);
		}
		
		return statusesList;
	}
	
	public List<PreliminaryChecks> getPreliminaryChecks(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		PreliminaryChecks preCheck = null;
		List<PreliminaryChecks> checksList = new ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			preCheck = new PreliminaryChecks();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			preCheck.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setSourcePath(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setSourceName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setSourceTypeCd((int)xssfCell.getNumericCellValue());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setTargetPath(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setTargetName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setTargetTypeCd((int)xssfCell.getNumericCellValue());
			cell = row.getCell(9);
			xssfCell = (XSSFCell) cell;
			
			String fileDelValue = "";
			
			switch(xssfCell.getCellTypeEnum())
			{
				case NUMERIC:
					fileDelValue = "" + xssfCell.getNumericCellValue();
					break;
				case STRING:
					fileDelValue = xssfCell.getRichStringCellValue().getString().trim();
					break;
				case _NONE:
					break;
				default:
					break;
			}
			
			preCheck.setFileDelimiterTypeCd(fileDelValue);
			
			cell = row.getCell(10);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setRowCount(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(11);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setDistinctValueCount(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(12);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setSumAvgValue(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(13);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setMinMaxValue(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(14);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setNullCount(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(15);
			xssfCell = (XSSFCell) cell;
			
			preCheck.setMappingSpecifications(xssfCell.getRichStringCellValue().getString().trim());
			
			checksList.add(preCheck);
		}
		
		return checksList;
	}
	
	public List<ColumnMapping> getColumnMapping(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		ColumnMapping columnMapping = null;
		List<ColumnMapping> columnMappingList = new ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			columnMapping = new ColumnMapping();
			
			cell = row.getCell(18);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			columnMapping.setSourceDb(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(19);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setSourceName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(20);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setTargetDb(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(21);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setTargetName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(22);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setSourceColumn(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(23);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setTargetColumn(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(24);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setNumeric(xssfCell.getRichStringCellValue().getString().trim().trim().equalsIgnoreCase("Y"));
			cell = row.getCell(25);
			xssfCell = (XSSFCell) cell;
			
			columnMapping.setAutoDetectDatatype(xssfCell.getRichStringCellValue().getString().trim());
			
			columnMappingList.add(columnMapping);
		}
		
		return columnMappingList;
	}
	
	public List<DuplicateDataVerification> getDuplicateData(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		DuplicateDataVerification duplicateData = null;
		List<DuplicateDataVerification> duplicateDataList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			duplicateData = new DuplicateDataVerification();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			duplicateData.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			duplicateData.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			duplicateData.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			duplicateData.setDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			duplicateData.setTableName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			duplicateData.setColumnName(xssfCell.getRichStringCellValue().getString().trim());
			
			duplicateDataList.add(duplicateData);
		}
		return duplicateDataList;
	}
	
	public List<TableAndDuplicateTypeInfo> getTableAndDuplicateTypeInfo(Sheet sheet)
	{
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		TableAndDuplicateTypeInfo tableAndDupInfo = null;
		List<TableAndDuplicateTypeInfo> tableAndDupInfoList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			tableAndDupInfo = new TableAndDuplicateTypeInfo();
			
			cell = row.getCell(11);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			tableAndDupInfo.setDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(9);
			xssfCell = (XSSFCell) cell;
			
			tableAndDupInfo.setTableName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(10);
			xssfCell = (XSSFCell) cell;
			
			tableAndDupInfo.setDupType(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(11);
			xssfCell = (XSSFCell) cell;
			
			tableAndDupInfo.setOpenRecords(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(12);
			xssfCell = (XSSFCell) cell;
			
			tableAndDupInfo.setEndDateColumn(xssfCell.getRichStringCellValue().getString().trim());
			
			tableAndDupInfoList.add(tableAndDupInfo);
		}
		
		return tableAndDupInfoList;
	}
	
	public List<RIVerification> getRIList(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		RIVerification rIData = null;
		List<RIVerification> rIList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			rIData = new RIVerification();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			rIData.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			rIData.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			rIData.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			rIData.setParentDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			rIData.setParentTableName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			rIData.setPkColumns(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			rIData.setChildDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			rIData.setChildTableName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			rIData.setFkColumns(xssfCell.getRichStringCellValue().getString().trim());
			
			rIList.add(rIData);
		}
		return rIList;
	}
	
	public List<HistoryVerification> getHistoryDataList(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		HistoryVerification historyData = null;
		List<HistoryVerification> HistoryDataList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			historyData = new HistoryVerification();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			historyData.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			historyData.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			historyData.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			historyData.setDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			historyData.setTableName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			historyData.setColumnName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			historyData.setPkColumn(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			historyData.setStartTSColumn(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			historyData.setEndTSColumn(xssfCell.getRichStringCellValue().getString().trim());
			
			HistoryDataList.add(historyData);
		}
		return HistoryDataList;
	}
	
	public List<HistoryTestDetail> getHistoryTestDetail (Sheet sheet)
	{
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		HistoryTestDetail historyDetail = null;
		List<HistoryTestDetail> historyDetailList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			historyDetail = new HistoryTestDetail();
			
			cell = row.getCell(13);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			cell = row.getCell(11);
			xssfCell = (XSSFCell) cell;
			
			historyDetail.setDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(12);
			xssfCell = (XSSFCell) cell;
			
			historyDetail.setTableName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(13);
			xssfCell = (XSSFCell) cell;
			
			historyDetail.setReverse(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(14);
			xssfCell = (XSSFCell) cell;
			
			historyDetail.setOverlap(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(15);
			xssfCell = (XSSFCell) cell;
			
			historyDetail.setGap(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(16);
			xssfCell = (XSSFCell) cell;
			
			historyDetail.setTestDetail(xssfCell.getRichStringCellValue().getString().trim());
			
			historyDetailList.add(historyDetail);
		}
		
		return historyDetailList;
	}
	
	public List<TechnicalColumnInfo> getTechnicalInfoList(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		TechnicalColumnInfo technicalInfo = null;
		List<TechnicalColumnInfo> technicalInfoList = new ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			technicalInfo = new TechnicalColumnInfo();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			technicalInfo.setColumnName(xssfCell.getRichStringCellValue().getString().trim());
			
			technicalInfoList.add(technicalInfo);
		}
		
		return technicalInfoList;
	}
	
	public List<LookupValues> getLookUpValues(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		LookupValues lookupData = null;
		List<LookupValues> lookUpValues = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			lookupData = new LookupValues();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			lookupData.setSourceTypeCd((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setSourceTypeName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setSourceTypeDesc(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setTargetTypeCd((int)xssfCell.getNumericCellValue());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setTargetTypeName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setTargetTypeDesc(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setEnvName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setEnvDesc(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(9);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setFileDelimiterTypeCd(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(10);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			lookupData.setFileDelimiter(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(11);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			lookupData.setFileDelimiterDesc(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(12);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			lookupData.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(13);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setStreamName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(14);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setStreamDesc(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(15);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(16);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setSubStreamName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(17);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setSubStreamDesc(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(18);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setModuleCode(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(19);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				continue;
			
			lookupData.setModuleName(xssfCell.getRichStringCellValue().getString().trim());
			
			lookUpValues.add(lookupData);
		}
		return lookUpValues;
	}

	/**
	 * Function getDuplicateInputData retrieves data from the 'Duplicate Data Verification' Sheet from Metadata file */
	
	public List<DuplicateRecordData> getDuplicateInputData() {
		
		List<DuplicateRecordData> dupData = new ArrayList<DuplicateRecordData>();
		
		DuplicateRecordData oneRowData = null;
		
		//System.out.println("Size is: " + JobTypeParser.getDuplicateData().size());
		
		int dupTypeCount = 0;
		String prevTableName = "";
		String prevCondition = "";
		
		for (int i=0; i< JobTypeParser.getDuplicateData().size(); ++i)
		{
			//System.out.println("index: " + i + ", Value: " + JobTypeParser.getTableAndDupTypeInfo().get(i).getDupType());
			oneRowData = new DuplicateRecordData();
			if(!JobTypeParser.getTableAndDupTypeInfo().get(dupTypeCount).getDupType().trim().equals(""))	// ensure blank entries are not inserted into list
			{
				oneRowData.setStreamId("" + JobTypeParser.getDuplicateData().get(i).getStreamId());
				oneRowData.setSubStreamId("" + JobTypeParser.getDuplicateData().get(i).getSubStreamId());
				oneRowData.setEnvId(JobTypeParser.getDuplicateData().get(i).getEnvId());
				oneRowData.setDatabaseName(JobTypeParser.getDuplicateData().get(i).getDbName());
				oneRowData.setTableName(JobTypeParser.getDuplicateData().get(i).getTableName());
				oneRowData.setPkColumnName(JobTypeParser.getDuplicateData().get(i).getColumnName());
				
				if(!prevTableName.equalsIgnoreCase(oneRowData.getDatabaseName() + "." + oneRowData.getTableName()))
				{
					oneRowData.setCondition(JobTypeParser.getTableAndDupTypeInfo().get(dupTypeCount).getDupType());
					oneRowData.setDuplicateType(JobTypeParser.getTableAndDupTypeInfo().get(dupTypeCount).getDupType());
					if(dupTypeCount >= (JobTypeParser.getTableAndDupTypeInfo().size() - 1))
						dupTypeCount = JobTypeParser.getTableAndDupTypeInfo().size() - 1;
					else
						++dupTypeCount;
				}
				
				else
				{
					oneRowData.setCondition(prevCondition);
					oneRowData.setDuplicateType(prevCondition);
				}
				
				dupData.add(oneRowData);
				
				prevTableName = oneRowData.getDatabaseName() + "." + oneRowData.getTableName();
				prevCondition = oneRowData.getCondition();
				
			}
		}
		
		return dupData;	
	}
	
	public List<MinusTest> getMinusTest(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		MinusTest minusTest = null;
		List<MinusTest> minusTestList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		//int i = 0;
		for(Row row: sheet) {
			
			//System.out.println("i: " + i);
			if(firstRow)
			{
				firstRow = false;
				//++i;
				continue;
			}
			
			minusTest = new MinusTest();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			minusTest.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setDbNameA(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setTblNameA(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setDbNameB(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setTblNameB(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setaMinusB(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setbMinusA(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(9);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setExcludeTechnicalCols(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(10);
			xssfCell = (XSSFCell) cell;
			
			minusTest.setMappingSpecification(xssfCell.getRichStringCellValue().getString().trim());
			
			minusTestList.add(minusTest);
			
			//++i;
		}
		return minusTestList;
	}
	
	public List<MinusTestColumnMapping> getMinusTestColumnMapping(Sheet sheet)
	{
		List<MinusTestColumnMapping> list = new ArrayList<MinusTestColumnMapping>();
		
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		MinusTestColumnMapping minusTestMapping = null;
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			minusTestMapping = new MinusTestColumnMapping();
			
			cell = row.getCell(13);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			minusTestMapping.setDbNameA(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(14);
			xssfCell = (XSSFCell) cell;
			
			minusTestMapping.setTblNameA(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(15);
			xssfCell = (XSSFCell) cell;
			
			minusTestMapping.setDbNameB(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(16);
			xssfCell = (XSSFCell) cell;
			
			minusTestMapping.setTblNameB(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(17);
			xssfCell = (XSSFCell) cell;
			
			minusTestMapping.setColumnA(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(18);
			xssfCell = (XSSFCell) cell;
			
			minusTestMapping.setColumnB(xssfCell.getRichStringCellValue().getString().trim());
			
			list.add(minusTestMapping);
		}
		
		return list;
	}
	
	public List<SurrogateKey> getSurrogateKey(Sheet sheet) {
		Cell cell = null;
		XSSFCell xssfCell = null;
		
		SurrogateKey surrKeyTest = null;
		List<SurrogateKey> surrKeyTestList = new  ArrayList<>();
		
		boolean firstRow = true;
		
		for(Row row: sheet) {
			
			if(firstRow)
			{
				firstRow = false;
				continue;
			}
			
			surrKeyTest = new SurrogateKey();
			
			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell == null)
				break;
			
			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			surrKeyTest.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setSourceDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setSourceTblName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setSurrKeyDbName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setSurrKeyTblName(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setNaturalKeyCols(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setSurrogateKeyCol(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(9);
			xssfCell = (XSSFCell) cell;
			
			surrKeyTest.setConcatenatedNaturalKeyCol(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(10);
			xssfCell = (XSSFCell) cell;
			
			if(xssfCell.getRichStringCellValue().getString().trim().equals(""))
				surrKeyTest.setNaturalKeyFunction(surrKeyTest.getConcatenatedNaturalKeyCol());		// the concatenated column in surrogate key table is considered the natural key function if there is only one natural key and the natural key function is not mentioned
			else
				surrKeyTest.setNaturalKeyFunction(xssfCell.getRichStringCellValue().getString().trim());
			
			surrKeyTestList.add(surrKeyTest);
		}
		
		return surrKeyTestList;
	}

	public List<TestScriptExecutionInfo> getTestScriptExecutionInfo(Sheet sheet) 
	{		
		Cell cell = null;
		XSSFCell xssfCell = null;

		TestScriptExecutionInfo testScriptInfo = null;
		List<TestScriptExecutionInfo> testScriptInfoList = new  ArrayList<>();

		boolean firstRow = true;

		for(Row row : sheet) {

			if(firstRow)
			{
				firstRow = false;
				continue;
			}

			testScriptInfo = new TestScriptExecutionInfo();

			cell = row.getCell(0);
			xssfCell = (XSSFCell) cell;

			if(xssfCell == null)
				break;

			if(xssfCell.getCellTypeEnum() == org.apache.poi.ss.usermodel.CellType.BLANK)	// checking for blank cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			if(xssfCell.getCellTypeEnum() != org.apache.poi.ss.usermodel.CellType.NUMERIC && xssfCell.getRichStringCellValue().getString().trim().equals(""))		// checking for empty cell, so empty rows are not inserted into list (and later Input table)
				continue;
			
			testScriptInfo.setStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(1);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setSubStreamId((int)xssfCell.getNumericCellValue());
			cell = row.getCell(2);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setEnvId(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(3);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setTestScript(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(4);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setTestResultsetDb(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(5);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setTestResultsetTbl(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(6);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setIntegratedLayerDb(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(7);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setIntegratedLayerTbl(xssfCell.getRichStringCellValue().getString().trim());
			cell = row.getCell(8);
			xssfCell = (XSSFCell) cell;
			
			testScriptInfo.setIntegratedLayerScript(xssfCell.getRichStringCellValue().getString().trim());
			
			testScriptInfoList.add(testScriptInfo);
		}
		
		return testScriptInfoList;
	}
}
