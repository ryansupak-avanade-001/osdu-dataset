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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.opengroup.osdu.core.common.http.json.HttpResponseBodyMapper;
import org.opengroup.osdu.core.common.http.json.HttpResponseBodyParsingException;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.MultiRecordInfo;
import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.core.common.model.storage.StorageException;
import org.opengroup.osdu.core.common.storage.IStorageFactory;
import org.opengroup.osdu.core.common.storage.IStorageService;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.request.StorageExceptionResponse;
import org.opengroup.osdu.dataset.model.response.DatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.response.GetDatasetStorageInstructionsResponse;
import org.opengroup.osdu.dataset.provider.gcp.config.GcpConfigProperties;
import org.opengroup.osdu.dataset.provider.gcp.di.EnvironmentResolver;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.FileInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpDatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpGetDatasetStorageInstructionsResponse;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileCollectionStorageService;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileStorageService;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class FileServiceImpl implements IFileService {

	private final HttpResponseBodyMapper bodyMapper;
	private final ObjectMapper objectMapper;
	private final IFileStorageService fileStorageService;
	private final IFileCollectionStorageService collectionStorageService;
	private final IStorageFactory storageFactory;
	private final GcpConfigProperties propertiesConfig;
	private final DpsHeaders headers;
	private final EnvironmentResolver providerKeyResolver;

	@Override
	public GetDatasetStorageInstructionsResponse getFileUploadInstructions() {
		return new GcpGetDatasetStorageInstructionsResponse(fileStorageService.getFileUploadItem(),
				objectMapper, providerKeyResolver.getProviderKey());
	}

	@Override
	public GetDatasetStorageInstructionsResponse getCollectionUploadInstructions() {
		return new GcpGetDatasetStorageInstructionsResponse(collectionStorageService.getCollectionUploadItem(),
			objectMapper, providerKeyResolver.getProviderKey());
	}

	@Override
	public GetDatasetRetrievalInstructionsResponse getFileRetrievalInstructions(
		GetDatasetRegistryRequest getDatasetRegistryRequest) {

		MultiRecordInfo getRecordsResponse = getMultiRecordInfo(getDatasetRegistryRequest);
		List<DatasetRetrievalDeliveryItem> delivery = getFileDelivery(getRecordsResponse.getRecords());
		return new GetDatasetRetrievalInstructionsResponse(delivery);
	}

	@Override
	public GetDatasetRetrievalInstructionsResponse getCollectionRetrievalInstructions(
		GetDatasetRegistryRequest getDatasetRegistryRequest) {
		MultiRecordInfo getRecordsResponse = getMultiRecordInfo(getDatasetRegistryRequest);
		List<DatasetRetrievalDeliveryItem> delivery = getFileCollectionDelivery(getRecordsResponse.getRecords());
		return new GetDatasetRetrievalInstructionsResponse(delivery);
	}

	private MultiRecordInfo getMultiRecordInfo(GetDatasetRegistryRequest getDatasetRegistryRequest) {
		IStorageService storageService = this.storageFactory.create(headers);

		MultiRecordInfo getRecordsResponse = null;

		try {
			getRecordsResponse = storageService.getRecords(getDatasetRegistryRequest.datasetRegistryIds);
		} catch (StorageException e) {
			try {
				StorageExceptionResponse body = bodyMapper
					.parseBody(e.getHttpResponse(), StorageExceptionResponse.class);
				throw new AppException(body.getCode(), "Storage Service: " + body.getReason(), body.getMessage());
			} catch (HttpResponseBodyParsingException e1) {
				throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR,
					"Internal Server Error",
					"Failed to parse error from Storage Service");
			}
		}
		return getRecordsResponse;
	}

	private List<DatasetRetrievalDeliveryItem> getFileDelivery(List<Record> datasetRegistryRecords) {
		List<DatasetRetrievalDeliveryItem> delivery = new ArrayList<>();
		for (Record datasetRegistryRecord : datasetRegistryRecords) {
			String preLoadFilePath = getPreLoadFilePath(datasetRegistryRecord);

			//reject paths that are not files
			if (preLoadFilePath.trim().endsWith("/")) {
				throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Invalid File Path",
					"Invalid File Path - Filename cannot contain trailing '/'");
			}

			FileInstructionsItem fileInstructionsItem = fileStorageService.createFileDeliveryItem(preLoadFilePath);

			GcpDatasetRetrievalDeliveryItem resp = new GcpDatasetRetrievalDeliveryItem(datasetRegistryRecord.getId(),
				fileInstructionsItem, objectMapper, providerKeyResolver.getProviderKey());

			delivery.add(resp);
		}
		return delivery;
	}

	private List<DatasetRetrievalDeliveryItem> getFileCollectionDelivery(List<Record> datasetRegistryRecords) {
		List<DatasetRetrievalDeliveryItem> delivery = new ArrayList<>();
		for (Record datasetRegistryRecord : datasetRegistryRecords) {
			String fileCollectionPath = getFileCollectionPath(datasetRegistryRecord);

			FileCollectionInstructionsItem collectionInstructionsItem = collectionStorageService
				.createCollectionDeliveryItem(fileCollectionPath);

			GcpDatasetRetrievalDeliveryItem resp = new GcpDatasetRetrievalDeliveryItem(datasetRegistryRecord.getId(),
				collectionInstructionsItem, objectMapper, providerKeyResolver.getProviderKey());

			delivery.add(resp);
		}
		return delivery;
	}

	private String getPreLoadFilePath(Record datasetRegistryRecord) {
		try {
			return getLocationFromMap(datasetRegistryRecord.getData(), propertiesConfig.getFileLocationSequence());
		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR,
				"Error finding unsigned path on record for signing",
				e.getMessage(), e);
		}
	}

	private String getFileCollectionPath(Record datasetRegistryRecord) {
		try {
			return getLocationFromMap(datasetRegistryRecord.getData(),
				propertiesConfig.getFileCollectionLocationSequence());
		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Error finding file collection path on record",
				e.getMessage(), e);
		}
	}

	private String getLocationFromMap(Map<String, Object> datasetProperties, List<String> locationSequence) {
		Object node = datasetProperties.get(locationSequence.get(0));
		if (locationSequence.size() == 1) {
			return (String) node;
		} else {
			return getLocationFromMap((Map) node, locationSequence.subList(1, locationSequence.size()));
		}
	}

}
