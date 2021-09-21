/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.dataset.ibm.util;

import org.opengroup.osdu.core.ibm.util.IdentityClient;
import org.opengroup.osdu.dataset.TestUtils;

import com.google.common.base.Strings;

public class IBMTestUtils extends TestUtils {
	private static String token;
	private static String noDataAccesstoken;

	@Override
	public synchronized String getToken() throws Exception {
		if (Strings.isNullOrEmpty(token)) {
			token = IdentityClient.getTokenForUserWithAccess();
		}
		return "Bearer " + token;
	}

	@Override
	public synchronized String getNoDataAccessToken() throws Exception {
		if (Strings.isNullOrEmpty(noDataAccesstoken)) {
			noDataAccesstoken = IdentityClient.getTokenForUserWithNoAccess();
		}
		return "Bearer " + noDataAccesstoken;
	}
	
}
