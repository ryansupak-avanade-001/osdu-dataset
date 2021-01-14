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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.MultiRecordIds;
import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.dataset.dms.DmsException;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.dms.IDmsFactory;
import org.opengroup.osdu.dataset.dms.IDmsProvider;
import org.opengroup.osdu.dataset.model.request.DmsExceptionResponse;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.request.StorageExceptionResponse;
import org.opengroup.osdu.dataset.model.response.DatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.response.GetDatasetStorageInstructionsResponse;
import org.opengroup.osdu.dataset.model.validation.DmsValidationDoc;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.opengroup.osdu.dataset.storage.GetRecordsResponse;
import org.opengroup.osdu.dataset.storage.IStorageFactory;
import org.opengroup.osdu.dataset.storage.IStorageProvider;
import org.opengroup.osdu.dataset.storage.StorageException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class DatasetDmsServiceImpl implements DatasetDmsService {

    public static final String RESOURCE_TYPE_ID_PROPERTY = "ResourceTypeID";
    public static final String RESOURCE_TYPE_ID_PATTERN_REGEX = "\\w+:+\\w+:+[\\w-]+";    

    @Inject
    private DpsHeaders headers;

    @Inject
    private IDmsFactory dmsFactory;

    @Inject
    private IStorageFactory storageFactory;

    @Inject
    private IDatasetDmsServiceMap dmsServiceMap;

    Pattern resourceTypeIdPattern = Pattern.compile(RESOURCE_TYPE_ID_PATTERN_REGEX);

    private ObjectMapper jsonObjectMapper = new ObjectMapper();

    public DatasetDmsServiceImpl() {
       
    }

    @Override
    public GetDatasetStorageInstructionsResponse getStorageInstructions(String resourceType) {

        Map<String,DmsServiceProperties> resourceTypeToDmsServiceMap = dmsServiceMap.getResourceTypeToDmsServiceMap();

        DmsServiceProperties dmsServiceProperties = resourceTypeToDmsServiceMap.get(resourceType);

        if (dmsServiceProperties == null) {
            throw new AppException(HttpStatus.BAD_REQUEST.value(), 
                HttpStatus.BAD_REQUEST.getReasonPhrase(), 
                String.format(DmsValidationDoc.RESOURCE_TYPE_NOT_REGISTERED_ERROR, resourceType));
        }        

        if (!dmsServiceProperties.isAllowStorage()) {
            HttpStatus status = HttpStatus.METHOD_NOT_ALLOWED;            
            throw new AppException(status.value(), "DMS - Storage Not Supported", String.format(DmsValidationDoc.DMS_STORAGE_NOT_SUPPORTED_ERROR, resourceType));
        }

        GetDatasetStorageInstructionsResponse response = null;

        try {

            IDmsProvider dmsProvider = dmsFactory.create(headers, dmsServiceProperties);
            response = dmsProvider.getStorageInstructions();
            
        }
        catch(DmsException e) {
            DmsExceptionResponse body = e.getHttpResponse().parseBody(DmsExceptionResponse.class);
            throw new AppException(body.getCode(), "DMS Service: " + body.getReason(), body.getMessage());
        }

        return response;
    }
    
    /**
     *  1. Pull down dataset registries
     *      a. Call storage query records
     *      b. validate all dataset registries exist, fail if not
     * 
     *  2. Check all dataset registries to see if they have registered resource type dms handlers
     *      a. map datasets to DMS for use by DMS caller
     *      b. throw exception if any unhandled
     * 
     *  3. Call each DMS one by one and get the delivery instructions
     *      a. group all same type dms types
     *      b. merge all responses into a single delivery object
     * 
     */
    @Override
    public GetDatasetRetrievalInstructionsResponse getDatasetRetrievalInstructions(List<String> datasetRegistryIds) {

        Map<String,DmsServiceProperties> resourceTypeToDmsServiceMap = dmsServiceMap.getResourceTypeToDmsServiceMap();

        GetRecordsResponse getRecordsResponse = null;

        try {

            IStorageProvider storageService = this.storageFactory.create(headers);

            MultiRecordIds multiRecordIds = new MultiRecordIds(datasetRegistryIds, null);
            getRecordsResponse = storageService.getRecords(multiRecordIds);

            if (getRecordsResponse.getInvalidRecords().size() > 0 || getRecordsResponse.getRetryRecords().size() > 0) {                
                try {
                    throw new AppException(HttpStatus.BAD_REQUEST.value(), "Storage Service: Invalid or Failed Record Get", jsonObjectMapper.writeValueAsString(getRecordsResponse));
                }                
                catch (JsonProcessingException e) {
                    throw new AppException(HttpStatus.BAD_REQUEST.value(), "Storage Service: Invalid or Failed Record Get", "Invalid or Failed Record Get");
                }
            }
        }
        catch (StorageException e) {
            StorageExceptionResponse body = e.getHttpResponse().parseBody(StorageExceptionResponse.class);
            throw new AppException(body.getCode(), "Storage Service: " + body.getReason(), body.getMessage());
        }

        List<Record> datasetRegistries = getRecordsResponse.getRecords();

        HashMap<String, GetDatasetRegistryRequest> datasetRegistryRequestMap = new HashMap<>();

        for (Record datasetRegistry : datasetRegistries) {
            Map<String, Object> datasetRegistryData = datasetRegistry.getData();
            Object resourceTypeIdObj = datasetRegistryData.get(RESOURCE_TYPE_ID_PROPERTY);
            if (resourceTypeIdObj == null) {
                throw new AppException(HttpStatus.BAD_REQUEST.value(), HttpStatus.BAD_REQUEST.getReasonPhrase(), "Dataset Registry: Missing Resource Type ID");
            }

            String resourceTypeId = (String)resourceTypeIdObj;

            Matcher matcher = resourceTypeIdPattern.matcher(resourceTypeId);
            boolean matchFound = matcher.find();
            
            if (!matchFound) {
                throw new AppException(HttpStatus.BAD_REQUEST.value(), HttpStatus.BAD_REQUEST.getReasonPhrase(), "Dataset Registry: Missing Resource Type ID");
            }

            String recordResourceType = matcher.group();

            if (!resourceTypeToDmsServiceMap.containsKey(recordResourceType)) {
                throw new AppException(HttpStatus.BAD_REQUEST.value(), 
                HttpStatus.BAD_REQUEST.getReasonPhrase(), 
                String.format(DmsValidationDoc.RESOURCE_TYPE_NOT_REGISTERED_ERROR, recordResourceType));
            }

            if (!datasetRegistryRequestMap.containsKey(recordResourceType)) {
                GetDatasetRegistryRequest request = new GetDatasetRegistryRequest();
                request.datasetRegistryIds = new ArrayList<String>();
                request.datasetRegistryIds.add(datasetRegistry.getId());
                datasetRegistryRequestMap.put(recordResourceType, request);
            }
            else {
                GetDatasetRegistryRequest request = datasetRegistryRequestMap.get(recordResourceType);
                request.datasetRegistryIds.add(datasetRegistry.getId());               
            }           

        }
        
        GetDatasetRetrievalInstructionsResponse mergedResponse = new GetDatasetRetrievalInstructionsResponse(new ArrayList<DatasetRetrievalDeliveryItem>());

          
        for (Map.Entry<String,GetDatasetRegistryRequest> datasetRegistryRequestEntry : datasetRegistryRequestMap.entrySet()) {

            try {
    
                IDmsProvider dmsProvider = dmsFactory.create(headers, resourceTypeToDmsServiceMap.get(datasetRegistryRequestEntry.getKey()));
                GetDatasetRetrievalInstructionsResponse entryResponse = dmsProvider.getDatasetRetrievalInstructions(datasetRegistryRequestEntry.getValue());
                mergedResponse.getDelivery().addAll(entryResponse.getDelivery());                           
                
            }
            catch(DmsException e) {
                DmsExceptionResponse body = e.getHttpResponse().parseBody(DmsExceptionResponse.class);
                throw new AppException(body.getCode(), "DMS Service: " + body.getReason(), body.getMessage());
            }    

        }
        

        return mergedResponse;
    }


    
}
