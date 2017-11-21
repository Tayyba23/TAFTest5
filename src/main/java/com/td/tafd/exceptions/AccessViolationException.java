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
public class AccessViolationException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public AccessViolationException(Class<?> classType, Exception e)
	{
		JobTypeParser.getApplicationlogger().error(ConfigurationManager.getInstance().getExceptionConfig().getAccessViolationCode() + ": Exception in class \'" + classType.getName() + "\'. Exception message: \"" + e.getMessage() + "\"");
	}
	
	public AccessViolationException(Class<?> classType, Exception e, String message)
	{
		JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getAccessViolationCode() + ": " + message);
	}
}
