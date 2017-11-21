package com.td.tafd.modules.di;

import java.util.HashMap;
import java.util.List;

import com.td.tafd.core.JobTypeParser;
import com.td.tafd.vo.MetricsRecord;

public class DataIntegrityAutomation 
{
	private static DataIntegrityAutomation dataIntegrityAutomator = null;
	
	private static HashMap<String, List<MetricsRecord>> metricsRecords;	// a map of object name (file or table) to metrics record (column name, sum, avg, min, max, null) mapping
	
	/**
	 * @return the metricsRecords
	 */
	public HashMap<String, List<MetricsRecord>> getMetricsRecords() {
		return metricsRecords;
	}

	/**
	 * @param metricsRecords the metricsRecords to set
	 */
	public void setMetricsRecords(HashMap<String, List<MetricsRecord>> metricsRecords1) {
		metricsRecords = metricsRecords1;
	}

	private DataIntegrityAutomation()
	{	
		metricsRecords = new HashMap<String, List<MetricsRecord>>();
	}
	
	public static DataIntegrityAutomation getInstance(){
		if(dataIntegrityAutomator == null)
		{
			dataIntegrityAutomator = new DataIntegrityAutomation();
			return dataIntegrityAutomator;
		}
		
		return dataIntegrityAutomator;
	}
	
	public void logSourceAndTargetAbsence(boolean s_obj_exists, boolean t_obj_exists, String module, int i)
	{
		JobTypeParser.getApplicationlogger().info("In function \'logSourceAndTargetAbsence\', parameter values: \'s_obj_exists\' = " + s_obj_exists + ", \'t_obj_exists\' = " + t_obj_exists + ", \'module\' = " + module + ", \'int i\' = " + i);
		
		if(s_obj_exists && !t_obj_exists)
			JobTypeParser.getLogger().error("Error: Target object \'" + JobTypeParser.getReader().getTargets().get(i) + "\' does not exist - Module \'" + module + "\' cannot be executed");

		else if(!s_obj_exists && t_obj_exists)
			JobTypeParser.getLogger().error("Error: Source object \'" + JobTypeParser.getReader().getSources().get(i) + "\' does not exist - Module \'" + module + "\' cannot be executed");

		else if(!s_obj_exists && !t_obj_exists)
			JobTypeParser.getLogger().error("Error: Source and Target objects \'" +  JobTypeParser.getReader().getSources().get(i) + "\' and \'" + JobTypeParser.getReader().getTargets().get(i) + "\' do not exist - Module \'" + module + "\' cannot be executed");
	}
	
}
