/**
 * 
 */
package com.td.tafd.modules.dl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;

import com.td.tafd.QueryGenerator;
import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.DataSource;
import com.td.tafd.ds.DataSourceFactory;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.exceptions.MissingInformationException;
import com.td.tafd.validation.Validator;

/**
 * @author kt186036
 *
 */

public class ObjectLevelRowCountTestAutomator implements Callable<Void>
{
	private Connection dbConn;
	private TeradataDataSource tdSource;
	
	private int streamId;
	private int subStreamId;
	private String environment;
	private int userId;
	
	private String moduleStatus;
	/**
	 * @return the dbConn
	 */
	public Connection getDbConn() {
		return dbConn;
	}
	
	/**
	 * @param dbConn the dbConn to set
	 */
	public void setDbConn(Connection dbConn) {
		this.dbConn = dbConn;
	}
	
	/**
	 * @return the tdSource
	 */
	public TeradataDataSource getTdSource() {
		return tdSource;
	}
	
	/**
	 * @param tdSource the tdSource to set
	 */
	public void setTdSource(TeradataDataSource tdSource) {
		this.tdSource = tdSource;
	}
	
	public ObjectLevelRowCountTestAutomator()
	{
		tdSource = new TeradataDataSource();
		dbConn = null;
		streamId = 0;
		subStreamId = 0;
		
		environment = "";
		userId = 0;
		
		moduleStatus = "Unsuccessful";
	}
	
	@Override
	public Void call() throws Exception 
	{
		JobTypeParser.getLogger().info("--------------------------------------------------------------------------------------\n" +
										"\t\t\t    |				Running Module \'OBJECT LEVEL ROW COUNT\'			|\n" + 
										"\t\t\t    --------------------------------------------------------------------------------------");

		long startTime = System.currentTimeMillis();
		long endTime = 0;
		
		/*int dbColId = JobTypeParser.getReader().getColId("Database Name", "Release Package Information", JobTypeParser.getWorkbook());
		int objColId = JobTypeParser.getReader().getColId("Object Name", "Release Package Information", JobTypeParser.getWorkbook());*/
		
		String dbName = "";
		String object = "";
		
		for (int i=0; i < JobTypeParser.getReleasePackageInfo().size() ; ++i)
		{
			if(!JobTypeParser.getReleasePackageInfo().get(i).getObjectType().equalsIgnoreCase("table") && !JobTypeParser.getReleasePackageInfo().get(i).getObjectType().equalsIgnoreCase("view"))
				continue;
			//int env_col_id = JobTypeParser.getReader().getColId("Env_Id", "Release Package Information", JobTypeParser.getWorkbook());
			
			environment = JobTypeParser.getReleasePackageInfo().get(i).getEnvID();
			
			dbName = JobTypeParser.getReleasePackageInfo().get(i).getDbName();
			object = JobTypeParser.getReleasePackageInfo().get(i).getObjectName();
			
			if(dbName.trim().equals("") && !object.trim().equals(""))
			{
				try {
					throw new MissingInformationException(DataLoadAutomation.class, new Exception(), "Column \'Database Name\' is empty in row " + (i+2) + " in sheet \'Release Package Information\' in Input File \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\". Module \'Object Level Row Count cannot be executed for this entry\'");
				} catch (MissingInformationException e) {
				}
				
				continue;
			}
			
			if(object.trim().equals("") && !dbName.trim().equals(""))
			{
				try {
					throw new MissingInformationException(DataLoadAutomation.class, new Exception(), "Column \'Object Name\' is empty in row " + (i+2) + " in sheet \'Release Package Information\' in Input File \"" + ConfigurationManager.getInstance().getUserConfig().getInputFilePath() + "\"");
				} catch (MissingInformationException e) {
				}
				
				continue;
			}
			
			if(object.trim().equals("") && dbName.trim().equals(""))
			{
				continue;
			}
			
			String objName = new StringBuilder(dbName).append(".").append(object).toString();	// fully qualified table/view name
			
			DataSource dataSource = DataSourceFactory.getDataSource(1);
			
			int [] rowCount = new int[1];
			dataSource.getRowCount(objName, rowCount);

			Connection tafdConn = null;
			
			String insertion_query = new StringBuilder("Insert into ").append(ApplicationDatabaseStructure.getInstance().getDbName()).append(".Obj_Level_Row_Count_Rslt (object_name, object_row_count, business_date, db_name, execution_timestamp, env_id, stream_id, sub_stream_id, user_id, test_cycle_id, test_type_cd) VALUES( \'").append(object).append("\', ").append(rowCount[0]).append(", \'").append(ConfigurationManager.getInstance().getUserConfig().getBusinessDate()).append("\', \'").append(dbName).append("\', \'").append(new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date())).append("\', \'").append(environment).append("\', ").append(streamId).append(", ").append(subStreamId).append(", ").append(userId).append(", \'").append(environment).append("_").append(ConfigurationManager.getInstance().getAppConfig().getTestCycle()).append("\', ").append(Integer.parseInt(ConfigurationManager.getInstance().getAppConfig().getObjZeroRowCount())).append(");").toString();

			try {
				tafdConn = dataSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
				JobTypeParser.getObjlevelcountlogger().info(insertion_query);
				PreparedStatement p = tafdConn.prepareStatement(insertion_query);
				p.execute();

				p.close();
				tafdConn.close();
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		QueryGenerator.insertIntoSummary(0, "4", "", "Successful", "", "", "");
		endTime = System.currentTimeMillis();
		moduleStatus = "Successful";
		//JobTypeParser.getLogger().info("Module \'Object Level Row Count\' completed successfully");
		Validator.printModuleCompletionPrompt("Object Level Row Count", startTime, endTime, moduleStatus);
		
		return null;
	}
}
