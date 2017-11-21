/**
 * 
 */
package com.td.tafd.modules.dd;

import com.td.tafd.configuration.ConfigurationManager;
import com.td.tafd.core.JobTypeParser;
import com.td.tafd.db.ApplicationDatabaseStructure;
import com.td.tafd.exceptions.AccessViolationException;

/**
 * @author mr255048
 *
 */
public class DuplicateRecordUtil {

	public static void throwAccessViolationException()
			throws AccessViolationException {
		JobTypeParser
				.getLogger()
				.error(ConfigurationManager.getInstance().getExceptionConfig()
						.getAccessViolationCode()
						+ ": "
						+ "The user \'"
						+ ConfigurationManager.getInstance().getUserConfig()
								.getUsername()
						+ "\' does not have \'insert\' and/or \'update\' rights to object \'"
						+ ApplicationDatabaseStructure.getInstance()
								.getDbName()
						+ ".summary_tbl\' or object \'"
						+ ApplicationDatabaseStructure.getInstance()
								.getDbName() + ".pk_rslt\'");
	}

}
