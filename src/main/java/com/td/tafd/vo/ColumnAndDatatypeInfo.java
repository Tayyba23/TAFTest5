/**
 * 
 */
package com.td.tafd.vo;

import java.util.ArrayList;

/**
 * @author kt186036
 *
 */
public class ColumnAndDatatypeInfo {
	
	private ArrayList<String> columnNames;
	private ArrayList<String> datatypes;
	
	public ArrayList<String> getColumnNames() {
		return columnNames;
	}
	
	public void setColumnNames(ArrayList<String> columnName) {
		this.columnNames = columnName;
	}
	
	public ArrayList<String> getDatatypes() {
		return datatypes;
	}
	
	public void setDatatypes(ArrayList<String> datatype) {
		this.datatypes = datatype;
	}
}
