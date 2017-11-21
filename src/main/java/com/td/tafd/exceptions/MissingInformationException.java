/**
 * 
 */
package com.td.tafd.exceptions;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;

/**
 * @author kt186036
 *
 */
public class MissingInformationException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public MissingInformationException(Class<?> classType, Exception e)
	{
		JobTypeParser.getApplicationlogger().error(ConfigurationManager.getInstance().getExceptionConfig().getMissingInformationCode() + ": Exception in class \'" + classType.getName() + "\'. Exception message: \"" + e.getMessage() + "\"");
	}
	
	public MissingInformationException(Class<?> classType, Exception e, String message)
	{
		JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getMissingInformationCode() + ": " + message);
	}
}
