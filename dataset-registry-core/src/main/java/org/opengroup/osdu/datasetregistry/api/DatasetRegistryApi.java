// Copyright © 2020 Amazon Web Services
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

package org.opengroup.osdu.datasetregistry.api;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;

import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.core.common.model.storage.StorageRole;
import org.opengroup.osdu.core.common.model.storage.TransferInfo;
import org.opengroup.osdu.core.common.model.storage.validation.ValidationDoc;
import org.opengroup.osdu.core.common.storage.IngestionService;
import org.opengroup.osdu.datasetregistry.response.CreateUpdateDatasetRegistryResponse;
import org.opengroup.osdu.datasetregistry.service.DatasetRegistryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.annotation.RequestScope;

@RestController
@RequestMapping("registry")
@RequestScope
@Validated
public class DatasetRegistryApi {

	@Autowired
	private DpsHeaders headers;

	@Autowired
	private DatasetRegistryService dataRegistryService;

	@PutMapping()
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<CreateUpdateDatasetRegistryResponse> createOrUpdateDatasetRegistry(
			Record record) {

				// TransferInfo transfer = 
		this.dataRegistryService.createOrUpdateDatasetRegistry(null);
				CreateUpdateDatasetRegistryResponse transferResponse = new CreateUpdateDatasetRegistryResponse("asdf");
		return new ResponseEntity<CreateUpdateDatasetRegistryResponse>(transferResponse, HttpStatus.CREATED);
	}
}