package com.td.tafd.modules.di;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.constants.ConstantsInterface;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.parsers.excel.ExcelReader;
import com.td.tafd.vo.MetricsRecord;

public class FileDataRetriever implements ConstantsInterface
{	
	private static FileDataRetriever fileDataRetriever = null;
	
	private FileDataRetriever() {
	}
	
	public static FileDataRetriever getInstance()
	{
		if(fileDataRetriever == null)
			fileDataRetriever = new FileDataRetriever();
		return fileDataRetriever;
	}
	
	/**
	 *	Counts the number of data lines in a file
	 *	@param file_name - name of file
	 *	@param skip_header - skip header of file or not
	 */
	
	public int metadata_line_count(String file_name, boolean skip_header)
	{
		JobTypeParser.getApplicationlogger().info("In function \'metadata_line_count\', parameters: \'file_name\' = " + file_name + ", \'skip_header\' = " + skip_header);
		if(!new File(file_name).exists())			// if file does not exist, return -1
			return -1;
		
		int line_count = 0;	
		
		if(skip_header)
			line_count = -1;		// initialized with -1 in order to skip header
		
		try {
			if(!file_name.endsWith("xlsx") && !file_name.endsWith("xls") && !file_name.endsWith("xlsm"))
			{
				BufferedReader reader;
				
				try {
					URL url = new URL(file_name);
					URLConnection uc = url.openConnection();			// open up URLConnection

					String userpass = ConfigurationManager.getInstance().getUserConfig().getServerAccessUsername() + ":" + ConfigurationManager.getInstance().getUserConfig().getServerAccessPassword();		// user-name and password
					String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());					// formatted request property

					uc.setRequestProperty ("Authorization", basicAuth);
					reader = new BufferedReader(new InputStreamReader(uc.getInputStream()));		// BufferedReader object is associate with URL's input stream
				} catch(MalformedURLException e) {						// Malformed Exception is thrown if the given file_name is not a url, but a file
					reader = new BufferedReader(new InputStreamReader(new FileInputStream(file_name)));		// in which case, the BufferedReader object is associated with a FileInputStream, not the URL's input stream
				}

				String line = "";

				while ((line = reader.readLine()) != null)
				{
					if(!"".equals(line.trim()) && !line.equals("\n"))
						++line_count;
				}

				reader.close();
			}
			
			else
			{
				Workbook wb = null;						// Workbook's programmatic instance
				Sheet sheet = null;						// Holds current sheet of the workbook
				
				wb = JobTypeParser.getReader().getRequiredWorkbook(file_name, file_name.endsWith("xls"));
				
				if(file_name.equals(ConfigurationManager.getInstance().getUserConfig().getInputFilePath()))
					sheet = wb.getSheet("Row Count & Metrics Collection");
				
				else
					sheet = wb.getSheetAt(0);
				
				//System.out.println("Last row number: " + sheet.getLastRowNum() + ", value of first cell in last row: " + sheet.getRow(sheet.getLastRowNum()).getCell(0).getNumericCellValue());
				for(int i=0; i<sheet.getLastRowNum(); ++i)
				{
					if(sheet.getRow(i) == null || sheet.getRow(i).getCell(0) == null)
						break;
					++line_count;
				}
				
				//line_count = sheet.getLastRowNum();
				
			}
		} catch (FileNotFoundException e) {
			JobTypeParser.getLogger().debug("File " + file_name + " not found");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if((line_count == -1))	// in case file exists, and is empty, set line_count to 0. Else it returns as calculated
			line_count = 0;
		
		JobTypeParser.getApplicationlogger().info("In function \'metadata_line_count\', returning \'line_count\' = " + line_count);
		return line_count;
	}
	
	/**
	 * Retrieves columns from file
	 * @param objName - name of file
	 * @param i - index of entry in Excel file
	 */
	
	public static List<String> getFileColumns(String objName, int i) throws IOException
	{
		BufferedReader f_reader;
		
		try
		{
			URL url = new URL(objName);		// create a URL object from the file name
			URLConnection uc = url.openConnection();

			String userpass = ConfigurationManager.getInstance().getUserConfig().getServerAccessUsername() + ":" + ConfigurationManager.getInstance().getUserConfig().getServerAccessPassword();
			String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());

			uc.setRequestProperty ("Authorization", basicAuth);
			f_reader = new BufferedReader(new InputStreamReader(uc.getInputStream()));
		}
		
		catch(MalformedURLException e)			// if an exception is thrown, this file resides on the local machine
		{
			f_reader = new BufferedReader(new InputStreamReader( new FileInputStream(objName)));
		}
		
		double deli_code = Double.parseDouble(JobTypeParser.getRowCountAndMetrics().get(i).getFileDelimiterTypeCd());
		
		String delimiter = delimitersMap.get(deli_code);
		
		List<String> columnsList = new ArrayList<String>();
		String [] columns = null;
		
		try {
			columns = f_reader.readLine().toLowerCase().split(delimiter);
			f_reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String column : columns)
			columnsList.add(column);
		
		return columnsList;
	}
	
	/** 
	 * Function 'get_numeric_col_data' reads a source or target file, calculates sum, avg, min and max, and stores them to
	 * respective data structure. The function returns the column names found in the file
	 * Handles all delimited (, | : ; \t) file types other than Excel (xls and xlsx)
	 */
	
	public void get_numeric_col_data(final String obj_name, final int index, /*ArrayList<String> ret_cols, boolean sum_exec, boolean avg_exec, boolean min_max, */String [] columns)
	{
		JobTypeParser.getApplicationlogger().info("In function \'get_numeric_col_data\', parameters: \'obj_name\' = " + obj_name + ", \'index\' = " + index /*+ "\'sum_exec\' = " + sum_exec + ", \'avg_exec\' = " + avg_exec + ", \'min_max\' = " + min_max*/);
		
		HashMap<String, List<String>> distinctValues = new HashMap<String, List<String>>();
		
		try
		{
			BufferedReader f_reader;
			
			try
			{
				URL url = new URL(obj_name);		// create a URL object from the file name
				URLConnection uc = url.openConnection();

				String userpass = ConfigurationManager.getInstance().getUserConfig().getServerAccessUsername() + ":" + ConfigurationManager.getInstance().getUserConfig().getServerAccessPassword();
				String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());

				uc.setRequestProperty ("Authorization", basicAuth);
				f_reader = new BufferedReader(new InputStreamReader(uc.getInputStream()));
			}
			
			catch(MalformedURLException e)			// if an exception is thrown, this file resides on the local machine
			{
				f_reader = new BufferedReader(new InputStreamReader( new FileInputStream(obj_name)));
			}
			
			double deli_code = (double)JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("File_Delimiter_Type_Cd", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getNumericCellValue();
			
			String delimiter = delimitersMap.get(deli_code);
			
			f_reader.mark(1000);		// mark the place to revert to in reader after reset (here this mark is placed at the start of the file)
			
			columns = StringUtils.splitPreserveAllTokens(f_reader.readLine().toLowerCase(), delimiter);
			String [] vals = StringUtils.splitPreserveAllTokens(f_reader.readLine().toLowerCase(), delimiter);
			
			f_reader.reset();			// return to start of the file
		
			String data_line = " ";

			boolean col_name_row = true;
			int col_index = 0;
			int data_count = -2;
			
			DataIntegrityAutomation.getInstance().getMetricsRecords().put(obj_name, new ArrayList<MetricsRecord>());	// a new list of MetricRecords is added for a new object
			
			while((data_line = f_reader.readLine()) != null)
			{
				if(data_line.equals(""))
					continue;
				
				++data_count;
				String [] col_vals = StringUtils.splitPreserveAllTokens(data_line, delimiter);
				
				col_index = 0;		
				
				for(String col_val : col_vals)
				{	
					//for all columns, whether numeric or not	
					
					if(col_name_row)			// if it is the first row of data, it represents the column names
					{
						MetricsRecord metRecord = new MetricsRecord();
						
						metRecord.setColumnName(columns[col_index]);
						metRecord.setSum(0);
						metRecord.setAverage(0);
						metRecord.setMinimum("zzzzzzzzzz");
						metRecord.setMaximum("0");
						metRecord.setNullCount(0);
						metRecord.setDistinctCount(0);
						metRecord.setColumnIsNumeric(false);	// initial value is false - i.e. by default all columns are set to non-numeric
						
						DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).add(metRecord);		// adding default values for each column
						
						distinctValues.put(columns[col_index], new ArrayList<String>());
					}

					else						// otherwise, this is data
					{	
						if(data_count > 0)		// not first data row
						{
							if(/*!col_val.isEmpty() && */!col_val.trim().equals("?"))
							{
								String temp_max;

								if(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getMaximum() == null)
									temp_max = "";
								else
									temp_max = DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getMaximum();
								
								if(compareValues(col_val, temp_max) > 0) // if the value just read from file is greater than previous maximum, update the overall maximum value
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setMaximum(col_val);
								}

								String temp_min;

								temp_min = DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getMinimum();

								if(compareValues(col_val, temp_min) < 0) // if the value just read from file is smaller than previous minimum, update the overall minimum value
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setMinimum(col_val);

							}
						}
						
						else				// first data row
						{
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setMaximum(col_val);

							if(col_val.isEmpty() || col_val.equals(""))			// if null is encountered at the first row, replace empty string with a maximum string so that the min calculation logic is not affected
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setMinimum("zzzzzzzzzz");
							else
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setMinimum(col_val);
						}

						if(col_val.equals(null) || col_val.isEmpty() || col_val.equals("") || col_val.equals("?"))  	// data_count doesn't matter for null value check
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setNullCount((DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getNullCount()) + 1);		
						
						if(!distinctValues.get(columns[col_index]).contains(col_val))	// if list of distinct values for this column does not contain value just read from file, add it to the list
							distinctValues.get(columns[col_index]).add(col_val);
						
						DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setDistinctCount(distinctValues.get(columns[col_index]).size());	// update number of distinct values so far
					}	


					if(NumberUtils.isNumber(vals[col_index]))
					{	
						if(!col_name_row)						// if it is the first row of data, it represents the column names. Else, this is data
						{
							if(!NumberUtils.isNumber(col_val))
							{
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setColumnIsNumeric(false);
								++col_index;
								continue;
							}
							
							if(data_count > 0)		// not first row
							{	
								if(!col_val.isEmpty())
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setSum(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getSum() + (Double.parseDouble(col_val)));	// add value read from file into total sum of respective column
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setColumnIsNumeric(true);
								}
							}
							
							else					// first row of data
							{
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setSum(Double.parseDouble(col_val));
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setColumnIsNumeric(true);
							}

							DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).setAverage((DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getSum()) / ((double)(data_count - DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(col_index).getNullCount()) + 1));
						}
					}	

					++col_index;
				}
				col_name_row = false;
			}
			
			/*System.out.println("Distinct values in column \'butxt\':");
			System.out.println(distinctValues.get("butxt"));*/
			/*System.out.println("Distinct values per column: ");
			for(String col : columns)
			{
				System.out.println("Column name: \'" + col + "\'");
				System.out.println(distinctValues.get(col));
			}*/
			
			f_reader.close();
		} catch(ArrayIndexOutOfBoundsException e) {
			JobTypeParser.getApplicationlogger().error("Exception in get_numeric_col_data: " + e);
			JobTypeParser.getLogger().error("Error: Object \'" + obj_name + "\' has inconsistencies - please investigate object");
		} catch(FileNotFoundException e) {
			JobTypeParser.getLogger().debug("File \'" + obj_name + "\' not found");
		} catch (IOException e) {
			JobTypeParser.getApplicationlogger().debug("BufferedReader encountered an exception");
			JobTypeParser.getApplicationlogger().error("Exception in get_numeric_col_data: " + e);
		}	
	}
	
	/**
	 * Reads a source or target file, calculates sum, avg, min and max and stores them
	 * to respective data structure. The function returns the column names found in the file
	 * Handles all Excel files (xls and xlsx)
	 * 
	 * @param obj_name - name of file
	 */
	
	public void get_excel_numeric_data(String obj_name/*, ArrayList<String> ret_cols, boolean avg_exec, boolean min_max, boolean null_check*/)
	{	
		JobTypeParser.getApplicationlogger().info("In function \'get_excel_numeric_data\', parameters: \'obj_name\' = " + obj_name /*+ ", \'avg_exec\' = " + avg_exec + ", \'min_max\' = " + min_max + ", \'null_check\' = " + null_check*/);
		/*if(avg_exec && !min_max)		// if sum has been executed and average is now being executed
		{
			JobTypeParser.getApplicationlogger().debug("In function \'get_excel_numeric_data\', returning columns = " + DataIntegrityAutomation.getInstance().getCol_map().keySet());
			ret_cols.addAll(DataIntegrityAutomation.getInstance().getCol_map().keySet());
			return;
		}
		
		else if(!avg_exec && min_max)	// if min or max is being executed
		{
			JobTypeParser.getApplicationlogger().debug("In function \'get_excel_numeric_data\', returning columns = " + DataIntegrityAutomation.getInstance().getMm_col_map().keySet());
			ret_cols.addAll(DataIntegrityAutomation.getInstance().getMm_col_map().keySet());
			return;
		}
		
		else if(avg_exec && min_max && null_check)	// if min or max is being executed
		{
			JobTypeParser.getApplicationlogger().debug("In function \'get_excel_numeric_data\', returning columns = " + DataIntegrityAutomation.getInstance().getMm_col_map().keySet());
			ret_cols.addAll(DataIntegrityAutomation.getInstance().getMm_col_map().keySet());
			return;
		}*/
		
		ArrayList<String> col_names = new ArrayList<String>();
		
		double value = 0;
		String value2 = "";
		String str_value = "";
		
		ExcelReader temp = new ExcelReader();
		
		Workbook wb2 = temp.getRequiredWorkbook(obj_name, obj_name.endsWith("xls"));
		
		Row row;
		Cell cell;
		
		Sheet sheet = wb2.getSheetAt(0);
		
		Iterator<Row> rowIterator = sheet.iterator();	// rowIterator allows for row-wise reading of the file

		int colIndex = 0;
		int data_count = 0;
		
		boolean firstRow = true;
		
		while (rowIterator.hasNext()) 					// continue reading until end of content in the sheet
		{
			colIndex = 0;
			
			row = rowIterator.next();
			Iterator<Cell> cellIterator = row.cellIterator();	// cellIterator for each row
			
			while (cellIterator.hasNext()) 				// continue reading until row's content does not end
			{	
				cell = cellIterator.next();
				
				if(cell.getColumnIndex() != colIndex)	// if a cell was skipped because of being empty (null), update the null value count
				{
					JobTypeParser.getApplicationlogger().debug("cell column is (" + cell.getColumnIndex() + ") and colIndex is: " + colIndex + ", column being " + col_names.get(colIndex));
					DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setNullCount((DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getNullCount()) + 1);
					
					++colIndex;
				}
				
				if(!firstRow)
				{
					JobTypeParser.getApplicationlogger().debug("cell location: (" + cell.getRowIndex() + ", " + cell.getColumnIndex() + ")");
					
					switch (cell.getCellTypeEnum()) 
					{	
						case NUMERIC:
							
							value = cell.getNumericCellValue();
							
							if(data_count > 0)
							{
								if(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex) != null)
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setSum(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getSum() + value);
								
								double temp_max;
								if(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMaximum() == null)
									temp_max = 0;
								else
									temp_max = Double.parseDouble(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMaximum());
								
								if(value > temp_max)
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMaximum("" + value);
								}		
							}
							
							else
							{	
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setSum(value);
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMaximum("" + value);
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setNullCount(0);
							}
							
							double temp_min;
							
							if(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMinimum() == null)
								temp_min = Double.MAX_VALUE;
							else
								temp_min = Double.parseDouble(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMinimum());
							
							if(compareValues((""+value), ("" + temp_min)) < 0)
							{
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMinimum("" + value);
							}
							
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setAverage(data_count);
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setAverage(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getSum() / DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getAverage());
								
							DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setColumnIsNumeric(true);
							break;

						case STRING:
							
							value2 = cell.getStringCellValue();
							
							JobTypeParser.getApplicationlogger().debug("Value in string cell: " + value2);
							JobTypeParser.getApplicationlogger().debug("Column is: " + col_names.get(colIndex));
							
							//String tempy = col_names.get(colIndex);
							//DataIntegrityAutomation.getInstance().getCol_map().remove(tempy);
							
							if(data_count > 0)
							{	
								if(value2 == null || value2.equals(""))
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setNullCount(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getNullCount() + 1);
								}
								
								String temp_max;
								
								if(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMaximum() == null)
									temp_max = "";
								else
									temp_max = DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMaximum();
								
								if(compareValues(value2, temp_max) > 0)
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMaximum(value2);
								}
								/*if(value2.compareToIgnoreCase(temp_max) > 0)
								{
									DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMaximum(value2);
								}*/
							}
							
							else
							{	
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMaximum(value2);
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setNullCount(0);
							}
							
							String temp_min2;
							
							if(DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMinimum() == null)
								temp_min2 = "zzzzzzzzzz";
							else
								temp_min2 = DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).getMinimum();
							
							if(value2.compareToIgnoreCase(temp_min2) < 0)
								DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).get(colIndex).setMinimum(value2);
							
							break;
							
						default:
							break;
					}
				}
				
				else										// else these are all column names
				{
					switch (cell.getCellTypeEnum()) 
					{
						case STRING:
							str_value = cell.getStringCellValue();
							break;

						default:
							break;
					}
					
					col_names.add(str_value);		// saving all column names into col_names ArrayList
					
					MetricsRecord metRecord = new MetricsRecord();
					
					metRecord.setColumnName(col_names.get(colIndex));
					metRecord.setSum(0);
					metRecord.setAverage(0);
					metRecord.setMinimum("zzzzzzzzzz");
					metRecord.setMaximum("0");
					metRecord.setNullCount(0);
					metRecord.setColumnIsNumeric(false);	// initial value is false - i.e. by default all columns are set to non-numeric
					
					DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name).add(metRecord);		// adding default values for each column
					
					/*DataIntegrityAutomation.getInstance().getCol_map().put(str_value, new double[2]);
					DataIntegrityAutomation.getInstance().getCol_map().get(str_value)[0] = 0;
					DataIntegrityAutomation.getInstance().getCol_map().get(str_value)[1] = 0;
					
					DataIntegrityAutomation.getInstance().getMm_col_map().put(str_value, new String[3]);
					DataIntegrityAutomation.getInstance().getMm_col_map().get(str_value)[2] = "0";*/
				}
				++colIndex;
			}
			
			++data_count;
			
			if(firstRow)
				firstRow = false;
		}
		
		/*if(!avg_exec && !min_max && !null_check)
			ret_cols.addAll(DataIntegrityAutomation.getInstance().getCol_map().keySet());
		
		else if(!avg_exec && !min_max && null_check)
			ret_cols.addAll(DataIntegrityAutomation.getInstance().getMm_col_map().keySet());
		
		JobTypeParser.getApplicationlogger().debug("In function \'get_excel_numeric_data\', returning columns = " + ret_cols);*/
	}
	
	public static int compareValues(String val1, String val2)
	{	
		int returnVal = 1;
		
		if(typesMatch(val1, val2))		// if datatypes match, compare values and return result
		{
			return val1.compareToIgnoreCase(val2);
		}
		
		else							// otherwise, follow Teradata's rules for comparison and sorting
		{
			//System.out.println("Modified check");
			if(val1.equals("	")/* || val1.equals("?")*/)		// if val1 is tab (which is minimum in Teradata), return -1, regardless of what val2 is
				returnVal = -1;
			else if(val2.equals("	")/* || val1.equals("?")*/)		// if val2 is tab (which is minimum in Teradata), return -1, regardless of what val2 is
				returnVal = 1;
			else if(val1.equals(" ")/* || val1.equals("?")*/)		// if val1 is space (which is minimum after tab), return -1, regardless of what val2 is
				returnVal = -1;
			else if(val1.equals("") && !val2.equals(" ")/* || val1.equals("?")*/)		// if val1 is blank (which is considered null in the file's data), return -1, regardless of what val2 is
				returnVal = -1;
			if(isTimeStamp(val1) || isTimeStamp(val2))
				returnVal = (timeStampisBefore(val1, val2)) ? -1 : 1;
			if(isDate(val1) || isDate(val2))
				returnVal = (dateisBefore(val1, val2)) ? -1 : 1;
			else if(val1.equals("\n") && !val2.isEmpty() && !val2.equals("") && !val2.equals("\t"))
				returnVal = -1;
			else if( (NumberUtils.isNumber(val1)) && !val2.equals("") && !val2.isEmpty() && !val2.equals(" ") && !val2.equals("\t") && !val2.equals("\n"))
				returnVal = -1;
		}
		
		return returnVal;
	}
	
	/**
	 * Checks if datatypes match
	 * @param val1 - first value
	 * @param val2 - second value
	 * @return True if datatypes of first and second values match
	 */
	
	public static boolean typesMatch(String val1, String val2)
	{
		if ( (val1.equals("") || val1.isEmpty()) && (val2.equals("") || val2.isEmpty()) )
			return true;
		if ( val1.equals(" ") && val2.equals(" ") )			// comparing for spaces
			return true;
		if ( val1.equals("	") && val2.equals("	") )			// comparing for tabs
			return true;
		if ( val1.equals("\n") && val2.equals("\n") )			// comparing for new line
			return true;
		if ( val1.equals("\b") && val2.equals("\b") )			// comparing for backspace
			return true;
		if ( NumberUtils.isNumber(val1) && NumberUtils.isNumber(val2) )	// comparing for numbers
			return true;
		if ( ((StringUtils.isAlphaSpace(val1) || StringUtils.isAlpha(val1) || StringUtils.isAlphanumericSpace(val1) || StringUtils.isAlphanumeric(val1)) && !val1.equals("") && !val1.equals(" ") && !val1.equals("	")) && ( (StringUtils.isAlphaSpace(val2) || StringUtils.isAlpha(val2) ||  StringUtils.isAlphanumericSpace(val2) || StringUtils.isAlphanumeric(val2))  && !val2.equals("") && !val2.equals(" ") && !val2.equals("	")) )
			return true;
		if( (!isTimeStamp(val1) && !isDate(val1)) || (!isTimeStamp(val2) && !isDate(val2)) )	// if the two values are not alphabetic, alphanumeric, numeric or timestamps remove special characters and check again for alphanumericity
		{
			String val1SansSpecialChars = StringUtils.replaceEach(val1, specialChars, blankChars);
			String val2SansSpecialChars = StringUtils.replaceEach(val2, specialChars, blankChars );
			
			if ( ((StringUtils.isAlphaSpace(val1SansSpecialChars) || StringUtils.isAlpha(val1SansSpecialChars) || StringUtils.isAlphanumericSpace(val1SansSpecialChars) || StringUtils.isAlphanumeric(val1SansSpecialChars)) && !val1.equals("") && !val1.equals(" ") && !val1.equals("	")) && ( (StringUtils.isAlphaSpace(val2SansSpecialChars) || StringUtils.isAlpha(val2SansSpecialChars) ||  StringUtils.isAlphanumericSpace(val2SansSpecialChars) || StringUtils.isAlphanumeric(val2SansSpecialChars))  && !val2.equals("") && !val2.equals(" ") && !val2.equals("	")) )
				return true;
		}
		
		return false;
	}
	
	public static boolean timeStampisBefore(String time1, String time2)	// returns true if input string is a timestamp of any of the three formats
	{
		SimpleDateFormat sdf = getTimeStampFormat(time1);
		
		try {
			return (sdf.parse(time1).before(sdf.parse(time2)));
		} catch (ParseException e) {
			return false;
		}
	}
	
	public static boolean dateisBefore(String date1, String date2)	// returns true if input string is a timestamp of any of the three formats
	{
		//System.out.println("date1: " + date1 + ", date2: " + date2);
		SimpleDateFormat sdf = getDateFormat(date1);
		
		try {
			return (sdf.parse(date1).before(sdf.parse(date2)));
		} catch (ParseException e) {
			return false;
		}
	}
	
	public static SimpleDateFormat getDateFormat(String date)	// returns true if input string is a timestamp of any of the three formats
	{
		SimpleDateFormat sdf = null;
		
		if(isDateYMDValid(date, "-"))
			sdf = new SimpleDateFormat("yyyy-MM-dd");
		else if(isDateMDYValid(date, "-"))
			sdf = new SimpleDateFormat("MM-dd-yyyy");
		else if(isDateDMYValid(date, "-"))
			sdf = new SimpleDateFormat("dd-MM-yyyy");
		else if(isDateYMDValid(date, "/"))
			sdf = new SimpleDateFormat("yyyy/MM/dd");
		else if(isDateMDYValid(date, "/"))
			sdf = new SimpleDateFormat("MM/dd/yyyy");
		else if(isDateDMYValid(date, "/"))
			sdf = new SimpleDateFormat("dd/MM/yyyy");
		
		return sdf;
	}
	
	public static SimpleDateFormat getTimeStampFormat(String date)	// returns true if input string is a timestamp of any of the three formats
	{
		SimpleDateFormat sdf = null;
		
		if(isTimeStampYMDValid(date, "-"))
			sdf = new SimpleDateFormat("yyyy-MM-dd");
		else if(isTimeStampMDYValid(date, "-"))
			sdf = new SimpleDateFormat("MM-dd-yyyy");
		else if(isTimeStampDMYValid(date, "-"))
			sdf = new SimpleDateFormat("dd-MM-yyyy");
		else if(isTimeStampYMDValid(date, "/"))
			sdf = new SimpleDateFormat("yyyy/MM/dd");
		else if(isTimeStampMDYValid(date, "/"))
			sdf = new SimpleDateFormat("MM/dd/yyyy");
		else if(isTimeStampDMYValid(date, "/"))
			sdf = new SimpleDateFormat("dd/MM/yyyy");
		
		return sdf;
	}
	
	public static boolean isTimeStamp(String inputString)	// returns true if input string is a timestamp of any of the three formats
	{
		/*System.out.println("inputString: " + inputString);
		System.out.println("returning: " + ( (isTimeStampYMDValid(inputString)) || (isTimeStampMDYValid(inputString)) || (isTimeStampDMYValid(inputString)) ));*/
		
		return ( (isTimeStampYMDValid(inputString, "-")) || (isTimeStampMDYValid(inputString, "-")) || (isTimeStampDMYValid(inputString, "-")) || (isTimeStampYMDValid(inputString, "/")) || (isTimeStampMDYValid(inputString, "/")) || (isTimeStampDMYValid(inputString, "/")) );
	}
	
	public static boolean isDate(String inputString)		// returns true if input string is a date of any of the three formats
	{
		/*System.out.println("inputString: " + inputString);
		System.out.println("returning: " + ( (isTimeStampYMDValid(inputString)) || (isTimeStampMDYValid(inputString)) || (isTimeStampDMYValid(inputString)) ));*/
		return ( (isDateYMDValid(inputString, "-")) || (isDateMDYValid(inputString, "-")) || (isDateDMYValid(inputString, "-")) ||  (isDateYMDValid(inputString, "/")) || (isDateMDYValid(inputString, "/")) || (isDateDMYValid(inputString, "/")) );
	}
	
	public static boolean isTimeStampYMDValid(String inputString, String separator)
	{ 
	    SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd HH:mm:ss");
	    
	    try {
	       format.parse(inputString);
	       return true;
	    } catch(ParseException e) {
	        return false;
	    }
	}
	
	public static boolean isTimeStampMDYValid(String inputString, String separator)
	{ 
	    SimpleDateFormat format = new java.text.SimpleDateFormat("MM" + separator + "dd" + separator + "yyyy HH:mm:ss");
	    
	    try {
	       format.parse(inputString);
	       return true;
	    } catch(ParseException e) {
	        return false;
	    }
	}
	
	public static boolean isTimeStampDMYValid(String inputString, String separator)
	{ 
	    SimpleDateFormat format = new java.text.SimpleDateFormat("dd" + separator + "MM" + separator + "yyyy HH:mm:ss");
	    
	    try {
	       format.parse(inputString);
	       return true;
	    } catch(ParseException e) {
	        return false;
	    }
	}
	
	public static boolean isDateYMDValid(String inputString, String separator)
	{ 
	    SimpleDateFormat format = new java.text.SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd");
	    
	    try {
	       format.parse(inputString);
	       return true;
	    } catch(ParseException e) {
	        return false;
	    }
	}
	
	public static boolean isDateMDYValid(String inputString, String separator)
	{ 
	    SimpleDateFormat format = new java.text.SimpleDateFormat("MM" + separator + "dd" + separator + "yyyy");
	    
	    try {
	       format.parse(inputString);
	       return true;
	    } catch(ParseException e) {
	        return false;
	    }
	}
	
	public static boolean isDateDMYValid(String inputString, String separator)
	{ 
	    SimpleDateFormat format = new java.text.SimpleDateFormat("dd" + separator + "MM" + separator + "yyyy");
	    
	    try {
	       format.parse(inputString);
	       return true;
	    } catch(ParseException e) {
	        return false;
	    }
	}
}
