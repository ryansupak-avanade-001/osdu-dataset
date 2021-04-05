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

package org.opengroup.osdu.dataset.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.StorageRole;
import org.opengroup.osdu.dataset.logging.AuditLogger;
import org.opengroup.osdu.dataset.model.request.CreateDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.GetCreateUpdateDatasetRegistryResponse;
import org.opengroup.osdu.dataset.service.DatasetRegistryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.annotation.RequestScope;


@RestController
@RequestMapping("/")
@RequestScope
@Validated
public class DatasetRegistryApi {

	@Inject
	private DpsHeaders headers;

	@Inject
	private DatasetRegistryService dataRegistryService;

	@Inject
	private AuditLogger auditLogger;

	@PutMapping("/registerDataset")	
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<GetCreateUpdateDatasetRegistryResponse> createOrUpdateDatasetRegistry(
		@RequestBody @Valid @NotNull CreateDatasetRegistryRequest request) {

			GetCreateUpdateDatasetRegistryResponse response = this.dataRegistryService.createOrUpdateDatasetRegistry(request.datasetRegistries);
			this.auditLogger.registerDatasetSuccess(Collections.singletonList(response.toString()));
			return new ResponseEntity<GetCreateUpdateDatasetRegistryResponse>(response, HttpStatus.CREATED);
	}

	@GetMapping("/getDatasetRegistry")	
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "', '" + StorageRole.VIEWER + "')")
	public ResponseEntity<GetCreateUpdateDatasetRegistryResponse> getDatasetRegistry( 
		@RequestParam(value = "id") String datasetRegistryId) {

			List<String> datasetRegistryIds = new ArrayList<>();
			datasetRegistryIds.add(datasetRegistryId);

			GetCreateUpdateDatasetRegistryResponse response = this.dataRegistryService.getDatasetRegistries(datasetRegistryIds);
			this.auditLogger.readDatasetRegistriesSuccess(Collections.singletonList(response.toString()));
			return new ResponseEntity<GetCreateUpdateDatasetRegistryResponse>(response, HttpStatus.OK);
	}

	@PostMapping("/getDatasetRegistry")	
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "', '" + StorageRole.VIEWER + "')")
	public ResponseEntity<GetCreateUpdateDatasetRegistryResponse> getDatasetRegistry(
		@RequestBody @Valid @NotNull GetDatasetRegistryRequest request) {
			GetCreateUpdateDatasetRegistryResponse response = this.dataRegistryService.getDatasetRegistries(request.datasetRegistryIds);
			this.auditLogger.readDatasetRegistriesSuccess(Collections.singletonList(response.toString()));
			return new ResponseEntity<GetCreateUpdateDatasetRegistryResponse>(response, HttpStatus.OK);
	}
}