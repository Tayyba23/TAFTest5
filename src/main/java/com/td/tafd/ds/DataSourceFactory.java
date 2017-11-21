package com.td.tafd.ds;


public class DataSourceFactory 
{
	public static DataSource getDataSource(int sourceType)
	{
		switch(sourceType)
		{
			case 1:
				return new TeradataDataSource();
			case 2:
				return new FileDataSource();
			default:
				return null;
		}
	}
}
