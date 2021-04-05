// Copyright © 2021 Amazon Web Services
// Copyright 2017-2019, Schlumberger
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;

import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.core.common.model.storage.StorageRole;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.dataset.logging.AuditLogger;
import org.opengroup.osdu.dataset.model.request.CreateDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.GetCreateUpdateDatasetRegistryResponse;
import org.opengroup.osdu.dataset.service.DatasetRegistryService;
import org.springframework.http.ResponseEntity;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.security.access.prepost.PreAuthorize;

@RunWith(MockitoJUnitRunner.class)
public class DatasetRegistryApiTest {

    private final String USER = "user";
    private final String TENANT = "tenant1";

    @Mock
    private DatasetRegistryService datasetRegistryService;

    @Mock
    private DpsHeaders httpHeaders;

    @Mock
    private AuditLogger auditLogger;

    @InjectMocks
    private DatasetRegistryApi datasetRegistryApi;

    @Before
    public void setup() {
        initMocks(this);

        when(this.httpHeaders.getUserEmail()).thenReturn(this.USER);
        when(this.httpHeaders.getPartitionIdWithFallbackToAccountId()).thenReturn(this.TENANT);

        TenantInfo tenant = new TenantInfo();
        tenant.setName(this.TENANT);
    }

    @Test
    public void should_returnsHttp201_when_creatingOrUpdatingDatasetRegistrySuccessfully() {

        Record r1 = new Record();
        r1.setId("ID1");

        Record r2 = new Record();
        r2.setId("ID2");

        List<Record> records = new ArrayList<>();
        records.add(r1);
        records.add(r2);

        CreateDatasetRegistryRequest request = new CreateDatasetRegistryRequest();
        request.datasetRegistries = records;

        GetCreateUpdateDatasetRegistryResponse expectedResponse = new GetCreateUpdateDatasetRegistryResponse(records);
        when(this.datasetRegistryService.createOrUpdateDatasetRegistry(records)).thenReturn(expectedResponse);

        ResponseEntity response = this.datasetRegistryApi.createOrUpdateDatasetRegistry(request);

        assertEquals(HttpStatus.SC_CREATED, response.getStatusCodeValue());
        assertEquals(expectedResponse, response.getBody());
    }

    @Test
    public void should_allowAccessToCreateOrUpdateDatasetRegistries_when_userBelongsToCreatorOrAdminGroups() throws Exception {

        Method method = this.datasetRegistryApi.getClass().getMethod("createOrUpdateDatasetRegistry", CreateDatasetRegistryRequest.class);
        PreAuthorize annotation = method.getAnnotation(PreAuthorize.class);

        assertFalse(annotation.value().contains(StorageRole.VIEWER));
        assertTrue(annotation.value().contains(StorageRole.CREATOR));
        assertTrue(annotation.value().contains(StorageRole.ADMIN));
    }

    @Test
    public void should_returnsHttp200_when_getSingleDatasetRegistrySuccessfully() {

        Record r1 = new Record();
        r1.setId("ID1");

        List<Record> records = new ArrayList<>();
        records.add(r1);

        List<String> recordIds = new ArrayList<>();
        recordIds.add(r1.getId());

        GetCreateUpdateDatasetRegistryResponse expectedResponse = new GetCreateUpdateDatasetRegistryResponse(records);
        when(this.datasetRegistryService.getDatasetRegistries(recordIds)).thenReturn(expectedResponse);

        ResponseEntity response = this.datasetRegistryApi.getDatasetRegistry(r1.getId());

        assertEquals(HttpStatus.SC_OK, response.getStatusCodeValue());
        assertEquals(expectedResponse, response.getBody());
    }

    @Test
    public void should_returnsHttp200_when_getMultipleDatasetRegistriesSuccessfully() {

        Record r1 = new Record();
        r1.setId("ID1");

        Record r2 = new Record();
        r2.setId("ID2");

        List<Record> records = new ArrayList<>();
        records.add(r1);
        records.add(r2);

        List<String> recordIds = new ArrayList<>();
        recordIds.add(r1.getId());
        recordIds.add(r2.getId());

        GetDatasetRegistryRequest request = new GetDatasetRegistryRequest();
        request.datasetRegistryIds = recordIds;

        GetCreateUpdateDatasetRegistryResponse expectedResponse = new GetCreateUpdateDatasetRegistryResponse(records);
        when(this.datasetRegistryService.getDatasetRegistries(recordIds)).thenReturn(expectedResponse);

        ResponseEntity response = this.datasetRegistryApi.getDatasetRegistry(request);

        assertEquals(HttpStatus.SC_OK, response.getStatusCodeValue());
        assertEquals(expectedResponse, response.getBody());
    }
}
