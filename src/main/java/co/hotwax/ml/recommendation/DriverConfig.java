/*
 * **************************************************************************<BR>
 * This program contains trade secrets and confidential information which<BR>
 * are proprietary to CSC Financial Services Group�.  The use,<BR>
 * reproduction, distribution or disclosure of this program, in whole or in<BR>
 * part, without the express written permission of CSC Financial Services<BR>
 * Group is prohibited.  This program is also an unpublished work protected<BR>
 * under the copyright laws of the United States of America and other<BR>
 * countries.  If this program becomes published, the following notice shall<BR>
 * apply:
 *     Property of Computer Sciences Corporation.<BR>
 *     Confidential. Not for publication.<BR>
 *     Copyright (c) 2000-2014 Computer Sciences Corporation. All Rights Reserved.<BR>
 * **************************************************************************<BR>
 */

package co.hotwax.ml.recommendation;

import java.util.PropertyResourceBundle;

/**
 * <p>
 * New class added to read properties from DriverConfig.properties file.
 * 
 * <p>
 * <b>Modifications: </b> <br>
 * <table border=0 cellspacing=5 cellpadding=5>
 * <thead>
 * <th align=left>Project</th>
 * <th align=left>Release</th>
 * <th align=left>Description</th> </thead>
 * <tr>
 * <tr>
 * <td>LC</td>
 * <td>Version 3</td>
 * <td>Linux Certification</td>
 * </tr>
 * </tr>
 * </table>
 * <p>
 * 
 * @author gvasmatkar
 * @version
 */
public class DriverConfig {

	public static final String PROPERTY_FILE = "DriverConfig";
	private static final String DB_NAME = "co.hc.db.name";
	private static final String DB_USER = "co.hc.db.user";
	private static final String DB_PASSWORD = "co.hc.db.password";
	private static final String DB_HOST = "co.hc.db.host";
	private static final String DB_DRIVER = "co.hc.db.driver";
	private static final String PREDICTIONS_DUMP_PATH = "co.hc.predictions.dump.path";

	private static PropertyResourceBundle configBundle;

	static {
		try {

			configBundle = (PropertyResourceBundle) PropertyResourceBundle.getBundle(PROPERTY_FILE);

		} catch (Exception e) {
			System.out.println("Error while reading property of DriverConfig.properties" + e.getMessage());
		}
	}

	public static String getProperty(String constant) {
		String value = "";

		try {

			value = configBundle.getString(constant);

		} catch (Throwable th) {
			System.err.println("Problems with loading DisclosureForms property: " + constant);
			th.printStackTrace();
		}
		return value;
	}

	public static String getDBName() {
		return getProperty(DB_NAME);
	}

	public static String getDBUser() {
		return getProperty(DB_USER);
	}

	public static String getDBPassword() {
		return getProperty(DB_PASSWORD);
	}

	public static String getDBHost() {
		return getProperty(DB_HOST);
	}
	
	public static String getDBDriver() {
		return getProperty(DB_DRIVER);
	}
	
	public static String getPredictionsDumpPath() {
		return getProperty(PREDICTIONS_DUMP_PATH);
	}

}// end class
