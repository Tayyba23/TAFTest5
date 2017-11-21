/**
 * 
 */
package com.td.tafd.modules.rca;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.ds.TeradataDataSource;
import com.td.tafd.vo.StepRowCountInfo;

/**
 * @author KT186036
 *
 */
public class RootCauseAnalysis implements Callable<Void>
{
	private TeradataDataSource tdSource;
	private int numOfRCATables;
	private long sessionNumber;
	
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
	
	/**
	 * @return the numOfRCATables
	 */
	public int getNumOfRCATables() {
		return numOfRCATables;
	}

	/**
	 * @param numOfRCATables the numOfRCATables to set
	 */
	public void setNumOfRCATables(int numOfRCATables) {
		this.numOfRCATables = numOfRCATables;
	}

	/**
	 * @return the sessionNumber
	 */
	public long getSessionNumber() {
		return sessionNumber;
	}

	/**
	 * @param sessionNumber the sessionNumber to set
	 */
	public void setSessionNumber(long sessionNumber) {
		this.sessionNumber = sessionNumber;
	}

	public RootCauseAnalysis()
	{
		setTdSource(new TeradataDataSource());
		setNumOfRCATables(0);
		setSessionNumber(15177);
	}
	
	/**
	 * Parses the view passed to it and retrieves the logic from it (joining tables, where filters etc.)
	 */
	
	public void parseViewAndRetrieveLogic()
	{
		// remember that 'case' statements do not affect the number of records being brought in
		// so as far as the row count is concerned, they can be ignored
	}
	
	public Void call()
	{
		// get join condition(s)
		// get filter and where condition(s)
		// AT THE MOMENT, THESE WILL BE DECIPHERED MANUALLY BY GOING THROUGH THE VIEW GENERATING SCRIPT
		
		// create the BTEQ script with Step Info logging enabled
		
		File bteqFile = createRCABteqScript("C:/Users/kt186036/Desktop/TAS/TAS Data Validation/VW_RPL_COMPANY.sql");
		
		// execute the script
		
		if(bteqFile != null)
			ApplicationDatabaseStructure.executeBteqScript(bteqFile);
		
		// query DBQL StepTbl to find out the row counts of each step (join, where etc.)
		
		String dbqlQuery = createDBQLQuery();
		List<StepRowCountInfo> stepInfoList = getStepRowCountInfo(dbqlQuery);
		
		displayStepInformation(stepInfoList);
		
		return null;
	}
	
	public void displayStepInformation(List<StepRowCountInfo> stepInfoList)
	{
		for(StepRowCountInfo srci : stepInfoList)
		{
			System.out.println("StepName: " + srci.getStepName());
			System.out.println("RowCount1: " + srci.getRowCount1());
			System.out.println("RowCount2: " + srci.getRowCount2());
			System.out.println("RowCount3: " + srci.getRowCount3());
		}
	}
	
	/**
	 * Creates a BTEQ script with required commands and format for Row Count Step information
	 * @param viewFile - the name of the file containing the view's SQL
	 * @return BTEQ file with the required format and commands
	 */
	public File createRCABteqScript(String viewFile)
	{
		String viewFileLine = "";
		StringBuilder bteqBuffer = new StringBuilder();
		
		BufferedReader f_reader;
		File bteqFile = null;
		
		try {
			
			try
			{
				URL url = new URL(viewFile);		// create a URL object from the file name
				URLConnection uc = url.openConnection();

				String userpass = ConfigurationManager.getInstance().getUserConfig().getServerAccessUsername() + ":" + ConfigurationManager.getInstance().getUserConfig().getServerAccessPassword();
				String basicAuth = "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes());

				uc.setRequestProperty ("Authorization", basicAuth);
				f_reader = new BufferedReader(new InputStreamReader(uc.getInputStream()));
			}

			catch(MalformedURLException e)			// if an exception is thrown, this file resides on the local machine
			{
				f_reader = new BufferedReader(new InputStreamReader( new FileInputStream(viewFile)));
			}

			bteqBuffer.append(".LOGON ").append(ConfigurationManager.getInstance().getUserConfig().getHostname()).append("/").append(ConfigurationManager.getInstance().getUserConfig().getUsername()).append(",").append(ConfigurationManager.getInstance().getUserConfig().getPassword()).append(";\nDatabase ").append(ApplicationDatabaseStructure.getInstance().getDbName()).append(";\n\n");
			
			// ADD A COMMAND HERE FOR SESSION
			// USE 'sessionNumber' VARIABLE
			
			bteqBuffer.append("BEGIN QUERY LOGGING WITH STEPINFO ON ").append(ConfigurationManager.getInstance().getUserConfig().getUsername()).append(";\n\n");
			
			while((viewFileLine = f_reader.readLine()) != null)		// read from view file and write contents to bteq file
			{
				bteqBuffer.append(viewFileLine).append("\n");
			}
			
			f_reader.close();
			
			bteqBuffer.append("END QUERY LOGGING WITH STEPINFO ON ").append(ConfigurationManager.getInstance().getUserConfig().getUsername()).append(";\n\n");
			bteqBuffer.append("\n.LOGOFF;\n.quit");	// lookup tables creation and insertion commands

			bteqFile = new File(System.getProperty("user.dir").toString() + JobTypeParser.getFileSeparator() + "view_bteq.btq");
			bteqFile.createNewFile();

			PrintWriter bteqWriter = new PrintWriter(bteqFile, "UTF-8");

			bteqWriter.println(bteqBuffer.toString());

			bteqWriter.close();

		} catch (IOException e) {
			JobTypeParser.getApplicationlogger().error("Exception in createRCABteqScript(): " + e);
		}
		
		return bteqFile;
	}
	
	/**
	 * Builds the query required for Row Count Information of each step of a query
	 * @return the generated query
	 */
	
	public String createDBQLQuery()
	{
		// THIS VIEW HAS A ROW FOR EACH STEP OF EXECUTION OF QUERIES
		// THE IDEA IS THAT THIS QUERY WILL RETURN INFO FOR ALL STEPS INVOLVED IN THE VIEW'S SQL
		// SO WE WILL ITERATE THROUGH ALL THE ROWS RETUENED BY THIS QUERY AND RECORD ROW COUNTS FOR EACH STEP
		
		StringBuilder queryBuilder = new StringBuilder();
		
		queryBuilder.append("SELECT StepName, RowCount, RowCount2, RowCount3 ")
					.append("FROM dbc.QryLogSteps ")							// check if 'QryLogSteps' is okay, or do we use 'DBQLSTEPTBL'
					//.append("WHERE cast(CollectTimeStamp as DATE) = DATE ")
					.append("WHERE QueryID in (SELECT QueryID from DBC.DBQLOGTBL WHERE sessionid = ").append(getSessionNumber()).append(");");
					//.append("ORDER BY 2,3,4,5;");
		
		return queryBuilder.toString();
	}
	
	/**
	 * 
	 * @param query
	 * @return the Row Count information for each step of the query passed to it
	 */
	
	public List<StepRowCountInfo> getStepRowCountInfo(String query)
	{
		List<StepRowCountInfo> stepInfoList = new ArrayList<StepRowCountInfo>();
		
		// ITERATING THROUGH EACH ROW AND RECORDING ROW COUNTS FOR EACH STEP
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			conn = tdSource.getConnection(ConfigurationManager.getInstance().getUserConfig().getHostname());
			
			pstmt = conn.prepareStatement(query);
			
			rs = pstmt.executeQuery();
			
			while(rs.next())
			{
				StepRowCountInfo srci = new StepRowCountInfo();
				srci.setStepName(rs.getString(1));
				srci.setRowCount1(rs.getInt(2));
				srci.setRowCount2(rs.getInt(3));
				srci.setRowCount3(rs.getInt(4));
				
				stepInfoList.add(srci);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
			if(rs != null)
				rs.close();
			if(pstmt != null)
				pstmt.close();
			if(conn != null)
				conn.close();
			} catch(SQLException e) {
				e.printStackTrace();
			}
		}
		
		return stepInfoList;
	}
}
