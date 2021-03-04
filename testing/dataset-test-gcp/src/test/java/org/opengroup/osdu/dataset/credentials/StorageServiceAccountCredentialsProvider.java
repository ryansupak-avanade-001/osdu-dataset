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

package org.opengroup.osdu.dataset.credentials;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Predicate;
import lombok.extern.java.Log;
import org.opengroup.osdu.dataset.configuration.GcpConfig;
import org.opengroup.osdu.dataset.util.DecodedContentExtractor;

@Log
public class StorageServiceAccountCredentialsProvider {

	public final static Predicate<String> CREDENTIALS_CONTENT_ACCEPTANCE_TESTER = s -> s.trim().startsWith("{");

	private static GoogleCredentials credentials = null;

	public static Credentials getCredentials() {

		if (Objects.isNull(credentials)) {
			log.info("Get GCP_DEPLOY_FILE credentials");
			String serviceAccountValue = GcpConfig.getStorageAccount();
			serviceAccountValue = new DecodedContentExtractor(serviceAccountValue,
				CREDENTIALS_CONTENT_ACCEPTANCE_TESTER).getContent();

			try (InputStream inputStream = new ByteArrayInputStream(serviceAccountValue.getBytes())) {
				credentials = GoogleCredentials.fromStream(inputStream);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return credentials;
	}
}
