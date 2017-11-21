package com.td.tafd.ds;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.td.tafd.vo.DistinctColumnInfo;

public interface DataSource 
{
	public Connection getConnection(String dataSourceType) throws SQLException;
	public boolean getSumAvg(String objName, int index, ArrayList<String> retCols);
	public boolean getMinMax(String objName, int index, ArrayList<String> retCols);
	public List<DistinctColumnInfo> getDistinctValueCount(String obj_name, ArrayList<String> cols);
	public boolean getRowCount(String objName, int [] rowCount);
	public boolean getNullCount(String objName, int index, ArrayList<String> retCols);
	public boolean objectExists(String objName, Connection conn);
	public String getDataSourceType();
}
