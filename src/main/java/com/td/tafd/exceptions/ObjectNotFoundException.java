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
public class ObjectNotFoundException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public ObjectNotFoundException(Class<?> classType, Exception e)
	{
		JobTypeParser.getApplicationlogger().error(ConfigurationManager.getInstance().getExceptionConfig().getObjectNotFoundCode() + ": Exception in class \'" + classType.getName() + "\'. Exception message: \"" + e.getMessage() + "\"");
	}
	
	public ObjectNotFoundException(Class<?> className, Exception e, String message)
	{
		JobTypeParser.getLogger().error(ConfigurationManager.getInstance().getExceptionConfig().getObjectNotFoundCode() + ": " + message);
	}
	
	public ObjectNotFoundException(boolean sObjExists, boolean tObjExists, String module, int i)
	{
		JobTypeParser.getApplicationlogger().info("ObjectNotFoundException thrown");
		
		if(sObjExists && !tObjExists)
			JobTypeParser.getLogger().error("Error: Target object \'" + JobTypeParser.getReader().getTargets().get(i) + "\' does not exist - Module \'" + module + "\' cannot be executed");

		else if(!sObjExists && tObjExists)
			JobTypeParser.getLogger().error("Error: Source object \'" + JobTypeParser.getReader().getSources().get(i) + "\' does not exist - Module \'" + module + "\' cannot be executed");

		else if(!sObjExists && !tObjExists)
			JobTypeParser.getLogger().error("Error: Source and Target objects \'" +  JobTypeParser.getReader().getSources().get(i) + "\' and \'" + JobTypeParser.getReader().getTargets().get(i) + "\' do not exist - Module \'" + module + "\' cannot be executed");
	}
}
