/**
 * 
 */
package com.td.tafd.vo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author kt186036
 *
 */
public class MetricsRecordUtil 
{
	public static ArrayList<String> getAllColumns(HashMap<String, List<MetricsRecord>> metricsRecordMap, String fileName, boolean returnNumericColumns)
	{
		ArrayList<String> columns = new ArrayList<String>();
		
		for(MetricsRecord metRecord : metricsRecordMap.get(fileName))
		{
			if(returnNumericColumns)	// if numeric columns are to be returned
			{
				if(metRecord.columnIsNumeric())		// verify column is numeric before adding to column list being returned
					columns.add(metRecord.getColumnName());
			}
			
			else						// otherwise, return all columns
				columns.add(metRecord.getColumnName());
		}
		
		//System.out.println("Returning columns: " + columns);
		return columns;
	}
	
	public static MetricsRecord getRecordsForColumn(List<MetricsRecord> metricsRecordList, String columnName)
	{
		if(metricsRecordList != null)
		{
			for(MetricsRecord metRecord : metricsRecordList)
			{
				if(metRecord.getColumnName() == null)
					continue;
				if(metRecord.getColumnName().equalsIgnoreCase(columnName))
					return metRecord;
			}
		}
		
		return new MetricsRecord();
	}
	
	public static void setRecordsForColumn(List<MetricsRecord> metricsRecordList, MetricsRecord metRecord)
	{
		String columnName = metRecord.getColumnName();

		//System.out.println("Received column name for setting records: " + columnName);
		for(MetricsRecord metRec : metricsRecordList)
		{
			//System.out.println("curr column in list is: " + metRec.getColumnName());
			
			if(metRec.getColumnName() == null)
				continue;
			else if(metRec.getColumnName().equalsIgnoreCase(columnName))
			{
				//System.out.println("Making changes in the record for column \'" + metRec.getColumnName() + "\'");
				if(metRecord.getAverage() != 0)
					metRec.setAverage(metRecord.getAverage());
				if(metRecord.columnIsNumeric())
					metRec.setColumnIsNumeric(metRecord.columnIsNumeric());
				if(metRecord.getColumnName() != null)
					metRec.setColumnName(metRecord.getColumnName());
				if(metRecord.getMaximum() != null)
					metRec.setMaximum(metRecord.getMaximum());
				if(metRecord.getMinimum() != null)
					metRec.setMinimum(metRecord.getMinimum());
				if(metRecord.getNullCount() != 0)
					metRec.setNullCount(metRecord.getNullCount());
				if(metRecord.getSum() != 0)
					metRec.setSum(metRecord.getSum());
				break;
			}
		}
	}
	
	public static boolean recordForColumnExists(List<MetricsRecord> metricsRecordList, String columnName)
	{
		//System.out.println("checking for column existence in map. Looking for column: " + columnName);
		for(MetricsRecord metRecord : metricsRecordList)
		{
			if(metRecord.getColumnName() == null)
				continue;
			if(metRecord.getColumnName().equalsIgnoreCase(columnName))
			{
				//System.out.println("Column found. metRecord.getColumnName(): " + metRecord.getColumnName());
				return true;
			}
		}
		
		//System.out.println("column not found");
		return false;
	}
	
	public static void printMap(HashMap<String, List<MetricsRecord>> metricsRecordMap)
	{
		System.out.println("Printing map");
		
		List<String> keys = new ArrayList<String>();
		
		for(Object key : metricsRecordMap.keySet())
			keys.add(key.toString());
		
		for(int i=0; i<metricsRecordMap.size(); ++i)
			System.out.println(keys.get(i) + ": [" + metricsRecordMap.get(keys.get(i)) + "]");
	}
}
