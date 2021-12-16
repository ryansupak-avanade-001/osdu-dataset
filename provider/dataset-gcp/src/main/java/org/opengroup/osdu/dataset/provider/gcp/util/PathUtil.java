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

package org.opengroup.osdu.dataset.provider.gcp.util;

import static java.lang.String.format;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.dataset.provider.gcp.config.GcpConfigProperties;
import org.opengroup.osdu.dataset.provider.gcp.di.EnvironmentResolver;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class PathUtil {

	private static final String INVALID_PATH_REASON = "Unsigned url invalid, needs to be full path";
	private static final String MALFORMED_URL = "Malformed URL";

	private final GcpConfigProperties gcpConfigProperties;
	private final EnvironmentResolver environmentResolver;

	public String getBucketPath(TenantInfo tenantInfo) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder
			.append(tenantInfo.getProjectId())
			.append('-')
			.append(tenantInfo.getDataPartitionId())
			.append('-')
			.append(gcpConfigProperties.getFileDmsBucket());
		return stringBuilder.toString();
	}

	public String buildRelativePath() {
		String folderName = UUID.randomUUID().toString();
		String fileName = UUID.randomUUID().toString();

		return format("%s/%s", folderName, fileName);
	}

	public String[] getObjectKeyParts(String unsignedUrl) {
		String[] gsPathParts = unsignedUrl.split(environmentResolver.getTransferProtocol());

		if (gsPathParts.length < 2) {
			throw new AppException(HttpStatus.BAD_REQUEST.value(), MALFORMED_URL,
					INVALID_PATH_REASON);
		}

		String[] gsObjectKeyParts = gsPathParts[1].split("/");
		if (gsObjectKeyParts.length < 1) {
			throw new AppException(HttpStatus.BAD_REQUEST.value(), MALFORMED_URL,
					INVALID_PATH_REASON);
		}
		return gsObjectKeyParts;
	}
}
