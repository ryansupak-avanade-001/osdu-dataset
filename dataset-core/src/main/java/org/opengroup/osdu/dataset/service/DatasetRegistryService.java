// Copyright © 2021 Amazon Web Services
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.opengroup.osdu.dataset.service;

import java.util.List;

import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.dataset.model.response.GetCreateUpdateDatasetRegistryResponse;

public interface DatasetRegistryService {

	GetCreateUpdateDatasetRegistryResponse createOrUpdateDatasetRegistry(List<Record> datasetRegistries);
	GetCreateUpdateDatasetRegistryResponse getDatasetRegistries(List<String> datasetRegistryIds);
	void deleteDatasetRegistry(String datasetRegistryId);

}