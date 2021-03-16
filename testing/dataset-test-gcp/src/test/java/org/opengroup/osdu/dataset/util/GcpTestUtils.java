/*
 * Copyright 2021 Google LLC
 * Copyright 2021 EPAM Systems, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opengroup.osdu.dataset.util;

import com.sun.jersey.api.client.Client;
import org.opengroup.osdu.dataset.TestUtils;
import org.opengroup.osdu.dataset.configuration.GcpConfig;
import org.opengroup.osdu.dataset.credentials.GoogleServiceAccount;

public class GcpTestUtils extends TestUtils {

	private static String token;
	private static String noDataAccesstoken;

	public String getToken() throws Exception {
		if (token == null || token.isEmpty()) {
			token = new GoogleServiceAccount(GcpConfig.getIntTester())
				.getAuthToken(GcpConfig.getTargetAudience());
		}
		return "Bearer " + token;
	}

	public String getNoDataAccessToken() throws Exception {
		if (noDataAccesstoken == null || noDataAccesstoken.isEmpty()) {
			noDataAccesstoken = new GoogleServiceAccount(GcpConfig.getNoAccessTester())
				.getAuthToken(GcpConfig.getTargetAudience());
		}
		return "Bearer " + token;
	}

	public static Client getClient() {
		return TestUtils.getClient();
	}

}
