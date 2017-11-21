/**
 * 
 */
package com.td.tafd.exceptions;

import java.sql.SQLException;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;

/**
 * @author kt186036
 *
 */
public class InvalidInformationException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public InvalidInformationException(Class<?> classType, Exception e)
	{
		JobTypeParser.getApplicationlogger().error(ConfigurationManager.getInstance().getExceptionConfig().getInvalidInformationCode() + ": Exception in class \'" + classType.getName() + "\'. Exception message: \"" + e.getMessage() + "\"");
	}
	
	public InvalidInformationException(Class<?> classType, SQLException e, String tableName)
	{
		if(e.getSQLState() == null && e.getErrorCode() == 0)
		{
			JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getInvalidInformationCode() + ": Connection error - connection to \'" + e.getMessage().substring(e.getMessage().indexOf("socket orig=") + 12).split(" ")[0] + "\' cannot be established");
		}
		
		else if(e.getSQLState().equals("28000"))
		{
			JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getInvalidInformationCode() + ": Invalid Username and Password. Please check username and password in file \"config.properties\"");
			System.exit(0);
		}
		
		else if(e.getSQLState().equals("23000"))
		{
			JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getInvalidInformationCode() + ": SQL Error - Duplicate Primary key - cannot insert record into \'" + tableName + "\'");
		}
		
		else
		{
			String [] msg = e.getMessage().split("] ");
			JobTypeParser.getLogger().error("Error: SQL Error - " + msg[msg.length-1]);
			//e.printStackTrace();
		}
	}
	
	public InvalidInformationException(Class<?> className, Exception e, String message)
	{
		JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getInvalidInformationCode() + ": " + message);
		//System.exit(0);
	}

}
