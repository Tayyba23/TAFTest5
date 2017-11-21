package com.td.tafd.ds;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.td.tafd.modules.di.DataIntegrityAutomation;
import com.td.tafd.vo.DistinctColumnInfo;
import com.td.tafd.vo.MetricsRecord;


public class FileDataSource implements DataSource
{
	@Override
	public Connection getConnection(String dataSourceType) throws SQLException {
		return null;
	}

	@Override
	public List<DistinctColumnInfo> getDistinctValueCount(String obj_name, ArrayList<String> cols) {
		
		List<DistinctColumnInfo> distinctColumnInfoList = new ArrayList<DistinctColumnInfo>();
		
		for(String col : cols)
		{
			for(MetricsRecord metRecord : DataIntegrityAutomation.getInstance().getMetricsRecords().get(obj_name))
			{
				if(metRecord.getColumnName().trim().equalsIgnoreCase(col.trim()))
				{
					DistinctColumnInfo dci = new DistinctColumnInfo();
					dci.setName(metRecord.getColumnName());
					dci.setCount(metRecord.getDistinctCount());

					distinctColumnInfoList.add(dci);
				}
			}
		}
		
		//System.out.println(distinctColumnInfoList);
		return distinctColumnInfoList;
	}

	@Override
	public boolean getRowCount(String obj_name, int[] row_count) {
		return false;
	}

	public String [] convertToStringArray(ArrayList<String> arrayList)
	{
		String [] stringArray = new String [arrayList.size()];
		
		for(int i=0; i<arrayList.size(); ++i)
			stringArray[i] = arrayList.get(i);
		
		return stringArray;
	}
	
	@Override
	/** ----------------------------------------------------------------------------------------------
	 * | Function getNullCount retrieves number of null values of all columns of the File object name |
	 * | passed to it 																				  |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the File object											   		  |
	 * | @param index - index in Metadata File of the current source and target				   		  |
	 * | @param retCols - the columns retrieved from the object and will be returned		   		  |
	 * 		------------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   		  |
	 * | @return list of column names returned in the parameter 'retCols'							  |
	 * ----------------------------------------------------------------------------------------------- */
	
	public boolean getNullCount(String objName, int index, ArrayList<String> retCols) 
	{
		boolean tblExists = false;
		try 
		{
			new URL(objName);
			tblExists = true;
		} 
		
		catch (MalformedURLException e) 
		{
			if(new File(objName).exists())
				tblExists = true;
			else
				return false;
		}	
		
		/*if(objName.endsWith("xlsx") || objName.endsWith("xls"))		// if file is excel file
		{	
			if( ( ( ( ConfigurationManager.getInstance().getUserConfig().getRunModules().contains("8") ) || ( ConfigurationManager.getInstance().getUserConfig().getRunModules().toUpperCase().contains("all".toUpperCase()))  ) && JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("Sum_Avg", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getStringCellValue().equals("Y") ) || (( ( ConfigurationManager.getInstance().getUserConfig().getRunModules().contains("9") ) || ( ConfigurationManager.getInstance().getUserConfig().getRunModules().toUpperCase().contains("all".toUpperCase()))  ) && JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("Min_Max", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getStringCellValue().equals("Y")))
				FileDataRetriever.getInstance().get_excel_numeric_data(objName, retCols, true, true, true);
			else
				FileDataRetriever.getInstance().get_excel_numeric_data(objName, retCols, false, false, true);
		}
		
		else															// for all other file types
		{	
			if( ( ( ( ConfigurationManager.getInstance().getUserConfig().getRunModules().contains("8") ) || ( ConfigurationManager.getInstance().getUserConfig().getRunModules().toUpperCase().contains("all".toUpperCase()))  ) && JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("Sum_Avg", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getStringCellValue().equals("Y") ) || (( ( ConfigurationManager.getInstance().getUserConfig().getRunModules().contains("9") ) || ( ConfigurationManager.getInstance().getUserConfig().getRunModules().toUpperCase().contains("all".toUpperCase()))  ) && JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("Min_Max", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getStringCellValue().equals("Y")))
			{
				// call getFileColumns here
				FileDataRetriever.getInstance().get_numeric_col_data(objName, index, retCols, true, false, true, convertToStringArray(retCols));
			}
			else
			{
				// call getFileColumns here
				FileDataRetriever.getInstance().get_numeric_col_data(objName, index, retCols, false, false, true, convertToStringArray(retCols));
			}
		}*/
		
		return tblExists;
	}

	@Override
	public boolean objectExists(String objName, Connection conn) {
		
		boolean exists = false;
		
		try
		{
			new URL(objName);		// create a URL object from the file name
			exists = true;
			
		} catch(MalformedURLException e) {			// if an exception is thrown, this file resides on the local machine
			exists = new File(objName).exists();	// checking for existence of file on local machine
		}
		
		return exists;
	}

	@Override
	public String getDataSourceType() {
		return "fileDataSource";
	}

	@Override
	/** ----------------------------------------------------------------------------------------------
	 * | Function getSumAvg retrieves sum and avg of all columns of the File object name passed to it |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the File object											   		  |
	 * | @param index - index in Metadata File of the current source and target				   		  |
	 * | @param retCols - the columns retrieved from the object and will be returned		   		  |
	 * 		------------------------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   		  |
	 * | @return list of column names returned in the parameter 'retCols'							  |
	 * ----------------------------------------------------------------------------------------------- */
	
	public boolean getSumAvg(String objName, int index, ArrayList<String> retCols) 
	{
		boolean tblExists = false;
		
		try 
		{
			new URL(objName);
			tblExists = true;
		} 
		
		catch (MalformedURLException e) 
		{
			if(new File(objName).exists())
				tblExists = true;
			else
				return false;
		}	
		
		
		/*if(objName.endsWith("xlsx") || objName.endsWith("xls"))		// if file is excel file
		{	
			FileDataRetriever.getInstance().get_excel_numeric_data(objName, retCols, false, false, false);
		}
		
		else															// for all other file types
		{	
			// call getFileColumns here
			FileDataRetriever.getInstance().get_numeric_col_data(objName, index, retCols, false, false, false, convertToStringArray(retCols));
		}*/
		
		return tblExists;
	}

	@Override
	/** ----------------------------------------------------------------------------------------------
	 * | Function getMinMax retrieves max and min of all columns of the File object name passed to it |
	 * 		-----------------------------------------------------------------------------
	 * | @param objName - name of the File object											   		  |
	 * | @param index - index in Metadata File of the current source and target				   		  |
	 * | @param retCols - the columns retrieved from the object and will be returned		   		  |
	 * 		------------------------------------------------------------------------------------------
	 * | @return true if object exists, false otherwise 									   		  |
	 * | @return list of column names returned in the parameter 'retCols'							  |
	 * ----------------------------------------------------------------------------------------------- */
	
	public boolean getMinMax(String objName, int index, ArrayList<String> retCols) 
	{	
		boolean tblExists = false;
		
		try 
		{
			new URL(objName);
			tblExists = true;
		} 
		
		catch (MalformedURLException e) 
		{
			if(new File(objName).exists())
				tblExists = true;
			else
				return false;
		}	
		
		/*if(objName.endsWith("xlsx") || objName.endsWith("xls"))		// if file is excel file
		{					
			FileDataRetriever.getInstance().get_excel_numeric_data(objName, retCols, false, true, false);
		}
		
		else															// for all other file types
		{	
			//System.out.println("Sum_Avg is: "  + JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("Sum_Avg", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getStringCellValue());
			
			if( ( ( ConfigurationManager.getInstance().getUserConfig().getRunModules().contains("8") ) || ( ConfigurationManager.getInstance().getUserConfig().getRunModules().toUpperCase().contains("all".toUpperCase())) ) && JobTypeParser.getWorkbook().getSheet("Row Count & Metrics Collection").getRow(index+1).getCell(JobTypeParser.getReader().getColId("Sum_Avg", "Row Count & Metrics Collection", JobTypeParser.getWorkbook())).getStringCellValue().equals("Y"))
			{
				// call getFileColumns here
				FileDataRetriever.getInstance().get_numeric_col_data(objName, index, retCols, true, false, true, convertToStringArray(retCols));
			}
			else
			{
				// call getFileColumns here
				FileDataRetriever.getInstance().get_numeric_col_data(objName, index, retCols, false, false, true, convertToStringArray(retCols));
			}
		}*/
		
		return tblExists;
	}
	
}
