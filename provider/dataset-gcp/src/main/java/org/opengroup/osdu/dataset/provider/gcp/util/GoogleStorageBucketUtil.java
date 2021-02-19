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

import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.dataset.provider.gcp.properties.GcpPropertiesConfig;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class GoogleStorageBucketUtil {

	private final GcpPropertiesConfig gcpPropertiesConfig;

	public String getBucketPath(TenantInfo tenantInfo) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder
			.append(tenantInfo.getProjectId())
			.append('-')
			.append(tenantInfo.getDataPartitionId())
			.append('-')
			.append(gcpPropertiesConfig.getFileBucket());
		return stringBuilder.toString();
	}
}
