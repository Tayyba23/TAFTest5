/**
 * 
 */
package com.td.tafd.configuration;


/**
 * @author kt186036
 *
 */
public class ConfigurationManager 
{
	private static ConfigurationManager configurationManager = null;
	
	private UserConfiguration userConfig;
	private ApplicationConfiguration appConfig;
	private ExceptionConfiguration exceptionConfig;
	
	public UserConfiguration getUserConfig() {
		return userConfig;
	}

	public ApplicationConfiguration getAppConfig() {
		return appConfig;
	}
	
	private ConfigurationManager() {
		userConfig = new UserConfiguration();
		appConfig = new ApplicationConfiguration();
		exceptionConfig = new ExceptionConfiguration();
	}
	
	public static ConfigurationManager getInstance()
	{
		if(configurationManager == null)
			configurationManager = new ConfigurationManager();
		return configurationManager;
	}

	public ExceptionConfiguration getExceptionConfig() {
		return exceptionConfig;
	}

	public void setExceptionConfig(ExceptionConfiguration exceptionConfig) {
		this.exceptionConfig = exceptionConfig;
	}
}
