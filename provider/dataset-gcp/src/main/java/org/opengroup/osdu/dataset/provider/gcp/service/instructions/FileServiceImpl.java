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

package org.opengroup.osdu.dataset.provider.gcp.service.instructions;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
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
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileCollectionStorageService;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileService;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileStorageService;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.FileInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpDatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpGetDatasetStorageInstructionsResponse;
import org.springframework.stereotype.Service;

@Service
public class FileServiceImpl implements IFileService {

	@Inject
	private IFileStorageService fileStorageService;

	@Inject
	private IFileCollectionStorageService collectionStorageService;

	@Inject
	private IStorageFactory storageFactory;

	@Inject
	private DpsHeaders headers;

	private final ObjectMapper objectMapper = new ObjectMapper();
	private final HttpResponseBodyMapper bodyMapper = new HttpResponseBodyMapper(objectMapper);

	@Override
	public GetDatasetStorageInstructionsResponse getFileUploadInstructions() {
		return new GcpGetDatasetStorageInstructionsResponse(fileStorageService.getUploadLocation(),
			fileStorageService.getProviderKey());
	}

	@Override
	public GetDatasetStorageInstructionsResponse getCollectionUploadInstructions() {
		return new GcpGetDatasetStorageInstructionsResponse(collectionStorageService.getUploadLocation(),
			collectionStorageService.getProviderKey());
	}

	@Override
	public GetDatasetRetrievalInstructionsResponse getFileRetrievalInstructions(
		GetDatasetRegistryRequest getDatasetRegistryRequest) {

		MultiRecordInfo getRecordsResponse = getMultiRecordInfo(getDatasetRegistryRequest);
		List<DatasetRetrievalDeliveryItem> delivery = getFileDelivery(getRecordsResponse.getRecords());
		GetDatasetRetrievalInstructionsResponse response = new GetDatasetRetrievalInstructionsResponse(delivery);

		return response;
	}

	@Override
	public GetDatasetRetrievalInstructionsResponse getCollectionRetrievalInstructions(
		GetDatasetRegistryRequest getDatasetRegistryRequest) {

		MultiRecordInfo getRecordsResponse = getMultiRecordInfo(getDatasetRegistryRequest);
		List<DatasetRetrievalDeliveryItem> delivery = getFileCollectionDelivery(getRecordsResponse.getRecords());
		GetDatasetRetrievalInstructionsResponse response = new GetDatasetRetrievalInstructionsResponse(delivery);

		return response;
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

			String fileName = getFileName(datasetRegistryRecord);
			FileInstructionsItem fileInstructionsItem = fileStorageService.createDeliveryItem(preLoadFilePath, fileName);
			String providerKey = fileStorageService.getProviderKey();

			GcpDatasetRetrievalDeliveryItem resp = new GcpDatasetRetrievalDeliveryItem(datasetRegistryRecord.getId(),
				fileInstructionsItem, providerKey);

			delivery.add(resp);
		}
		return delivery;
	}

	private List<DatasetRetrievalDeliveryItem> getFileCollectionDelivery(List<Record> datasetRegistryRecords) {
		List<DatasetRetrievalDeliveryItem> delivery = new ArrayList<>();
		for (Record datasetRegistryRecord : datasetRegistryRecords) {
			String fileCollectionPath = getFileCollectionPath(datasetRegistryRecord);

			FileCollectionInstructionsItem collectionInstructionsItem = collectionStorageService
				.createDeliveryItem(fileCollectionPath);

			String providerKey = collectionStorageService.getProviderKey();

			GcpDatasetRetrievalDeliveryItem resp = new GcpDatasetRetrievalDeliveryItem(datasetRegistryRecord.getId(),
				collectionInstructionsItem, providerKey);

			delivery.add(resp);
		}
		return delivery;
	}

	private String getFileName(Record datasetRegistryRecord) {
		String fileName;
		try {
			fileName = (String) datasetRegistryRecord.getData().get("ResourceName");
		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Error finding file's name on record",
				e.getMessage(), e);
		}
		return fileName;
	}

	private String getPreLoadFilePath(Record datasetRegistryRecord) {
		String preLoadFilePath;
		try {
			Map<String, Object> datasetProperties = (Map) datasetRegistryRecord.getData().get("DatasetProperties");
			Map<String, String> fileSourceInfo = (Map) datasetProperties.get("FileSourceInfo");
			preLoadFilePath = (String) datasetRegistryRecord.getData().get("PreLoadFilePath");
		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR,
				"Error finding unsigned path on record for signing",
				e.getMessage(), e);
		}
		return preLoadFilePath;
	}

	private String getFileCollectionPath(Record datasetRegistryRecord) {
		String fileCollectionPath;
		try {
			Map<String, Object> datasetProperties = (Map) datasetRegistryRecord.getData().get("DatasetProperties");
			Map<String, String> fileSourceInfo = (Map) datasetProperties.get("FileCollectionSourceInfo");
			fileCollectionPath = fileSourceInfo.get("FileCollectionPath");
		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Error finding file collection path on record",
				e.getMessage(), e);
		}
		return fileCollectionPath;
	}

}
