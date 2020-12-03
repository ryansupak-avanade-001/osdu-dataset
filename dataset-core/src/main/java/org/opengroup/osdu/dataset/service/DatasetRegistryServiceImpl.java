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

package org.opengroup.osdu.dataset.service;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.HttpException;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.MultiRecordIds;
import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.core.common.model.storage.Schema;
import org.opengroup.osdu.core.common.model.storage.SchemaItem;
import org.opengroup.osdu.dataset.model.validation.DatasetRegistryValidationDoc;
import org.opengroup.osdu.dataset.model.request.StorageExceptionResponse;
import org.opengroup.osdu.dataset.model.response.GetCreateUpdateDatasetRegistryResponse;
import org.opengroup.osdu.dataset.storage.CreateUpdateRecordsResponse;
import org.opengroup.osdu.dataset.storage.GetRecordsResponse;
import org.opengroup.osdu.dataset.storage.IStorageFactory;
import org.opengroup.osdu.dataset.storage.IStorageProvider;
import org.opengroup.osdu.dataset.storage.StorageException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class DatasetRegistryServiceImpl implements DatasetRegistryService {

    
    final String DATASET_REGISTRY_DATASET_PROPERTIES_NAME = "DatasetProperties";
    final String DATASET_REGISTRY_SCHEMA_FORMAT = "%s:osdu:dataset-registry:0.0.1";

    @Inject
    private DpsHeaders headers;

    @Inject
    IStorageFactory storageFactory;

    @Override
    public void deleteDatasetRegistry(String datasetRegistryId) {
        // todo: implement
        throw new NotImplementedException("Delete is Not Yet Implemented");
    }

    @Override
    public GetCreateUpdateDatasetRegistryResponse createOrUpdateDatasetRegistry(List<Record> datasetRegistries) {
        
        IStorageProvider storageService = this.storageFactory.create(headers);

        this.validateDatasetRegistries(storageService, datasetRegistries);

        CreateUpdateRecordsResponse storageResponse = null;
        try {
            storageResponse = storageService.createOrUpdateRecords(datasetRegistries);
        } catch (StorageException e) {
            StorageExceptionResponse body = e.getHttpResponse().parseBody(StorageExceptionResponse.class);
            throw new AppException(body.getCode(), "Storage Service: " + body.getReason(), body.getMessage());
        }

        List<String> recordIds = storageResponse.getRecordIds();

        GetRecordsResponse getRecordsResponse = null;

        try {
            MultiRecordIds multiRecordIds = new MultiRecordIds(recordIds, null);
            getRecordsResponse = storageService.getRecords(multiRecordIds);
        }
        catch (StorageException e) {
            StorageExceptionResponse body = e.getHttpResponse().parseBody(StorageExceptionResponse.class);
            throw new AppException(body.getCode(), "Storage Service: " + body.getReason(), body.getMessage());
        }

        GetCreateUpdateDatasetRegistryResponse response = new GetCreateUpdateDatasetRegistryResponse(getRecordsResponse.getRecords());        

        return response;
    }

    public GetCreateUpdateDatasetRegistryResponse getDatasetRegistries(List<String> datasetRegistryIds) {
        
        GetRecordsResponse getRecordsResponse = null;

        try {

            IStorageProvider storageService = this.storageFactory.create(headers);

            MultiRecordIds multiRecordIds = new MultiRecordIds(datasetRegistryIds, null);
            getRecordsResponse = storageService.getRecords(multiRecordIds);
        }
        catch (StorageException e) {
            StorageExceptionResponse body = e.getHttpResponse().parseBody(StorageExceptionResponse.class);
            throw new AppException(body.getCode(), "Storage Service: " + body.getReason(), body.getMessage());
        }

        GetCreateUpdateDatasetRegistryResponse response = new GetCreateUpdateDatasetRegistryResponse(getRecordsResponse.getRecords()); 

        return response;
    }

    /**
     * Return a 400 Bad Request if any validation fails (one bad property on a single dataset registry fails all input dataset registries)
     * in the event of bad input, nothing gets persisted (sent to storage service)
     * validate that 'DatasetProperties' exists as an object under the 'data' section. 
     * validate known ResourceTypeIDs against DatasetProperties. 
     * Future: validate property types match schema definitions
    */
    private boolean validateDatasetRegistries(IStorageProvider storageService, List<Record> datasetRegistries) {

        //todo: consider moving dataset schema into common partition
        String datasetRegistrySchemaName = String.format(DATASET_REGISTRY_SCHEMA_FORMAT, headers.getPartitionId());
        Schema datasetRegistrySchema = null;
        try {
            datasetRegistrySchema = storageService.getSchema(datasetRegistrySchemaName);
            if (datasetRegistrySchema == null) {
                throw new StorageException("schema null", null);
            }
        }
        catch (StorageException e) {
            throw new AppException(HttpStatus.BAD_REQUEST.value(), 
                HttpStatus.BAD_REQUEST.getReasonPhrase(), 
                String.format(DatasetRegistryValidationDoc.MISSING_DATASET_REGISTRY_SCHEMA_ERROR_FORMAT, datasetRegistrySchemaName));
        }
        
        for (Record datasetRegistry : datasetRegistries) {

            //validate record against schema
            this.validateDatasetRegistrySchema(datasetRegistry, datasetRegistrySchema);
            
            //validate dataset properties field exists
            if (!datasetRegistry.getData().containsKey(DATASET_REGISTRY_DATASET_PROPERTIES_NAME))
                throw new AppException(HttpStatus.BAD_REQUEST.value(), HttpStatus.BAD_REQUEST.getReasonPhrase(), DatasetRegistryValidationDoc.MISSING_DATASET_PROPERTIES_VALIDATION);
        }

        return true;
    }

    private boolean validateDatasetRegistrySchema(Record datasetRegistry, Schema datasetRegistrySchema) {

        //kind matches expected schema kind
        if (!datasetRegistry.getKind().equals(datasetRegistrySchema.getKind())) {
            throw new AppException(HttpStatus.BAD_REQUEST.value(), HttpStatus.BAD_REQUEST.getReasonPhrase(), 
                String.format(DatasetRegistryValidationDoc.INVALID_DATASET_REGISTRY_SCHEMA_KIND, datasetRegistrySchema.getKind()));
        }

        Map<String, Object> datasetRegistryData = datasetRegistry.getData();
        
        //All required properties in schema
        for (SchemaItem schemaItem : datasetRegistrySchema.getSchema()) {
            if (!datasetRegistryData.containsKey(schemaItem.getPath())) {
                throw new AppException(HttpStatus.BAD_REQUEST.value(), HttpStatus.BAD_REQUEST.getReasonPhrase(), 
                String.format(DatasetRegistryValidationDoc.DATASET_REGISTRY_MISSING_PROPERTY_VALIDATION_FORMAT, schemaItem.getPath()));
            }

            //TODO: Validate type of value matches

        }

        return true;
    }

}