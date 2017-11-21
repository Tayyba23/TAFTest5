package com.td.tafd.constants;

import java.util.HashMap;

public interface ConstantsInterface 
{	
	static final String [] necessarySheets = {"Document Information","Row Count & Metrics Collection","Duplicate Data Verification","RI Verification","History Verification","Exception List", "Lookup Values", "Configuration Parameters", "Release Package Information", "Execution Status Check", "Minus Test", "Surrogate Key Verification"};
	static final String [] preliminaryCheckCols = {"Stream_Id", "Sub_Stream_Id", "Env_Id", "Source_Path", "Source_Name", "Source_Type_Cd", "Target_Path", "Target_Name", "Target_Type_Cd", "File_Delimiter_Type_Cd", "Row_Count", "Distinct_Value_Count", "Sum_Avg", "Min_Max", "Null_Count"};
	static final String [] riVerificationCols = {"Stream_Id", "Sub_Stream_Id", "Env_Id", "Parent_DB_Name", "Parent_Table_Name", "PK_Columns", "Child_DB_Name", "Child_Table_Name", "FK_Columns"};
	
	static final String [] historyCaseTypes = {"Overlap", "Gap", "Reverse"};
	
	static HashMap<Double, String> delimitersMap = new HashMap<Double, String>();
	
	static final String [] valid_steps = {"3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "19", "all"};
	
	static final String [] specialChars = {".", "/", "(", ")", "-", "*", "\"", ","};
	static final String [] blankChars = {"", "", "", "", "", "", "", ""};
	
}
