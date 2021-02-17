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

package org.opengroup.osdu.dataset.provider.gcp.service;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.DataSetType;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpDmsServiceProperties;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

	private Map<String, DmsServiceProperties> resourceTypeToDmsServiceMap = new HashMap<>();

	@Override
	public Map<String, DmsServiceProperties> getResourceTypeToDmsServiceMap() {
		resourceTypeToDmsServiceMap.put(
			"dataset--File.*",
			new GcpDmsServiceProperties(DataSetType.FILE)
		);

		resourceTypeToDmsServiceMap.put(
			"dataset--FileCollection.*",
			new GcpDmsServiceProperties(DataSetType.FILE_COLLECTION)
		);

		return resourceTypeToDmsServiceMap;
	}
}
