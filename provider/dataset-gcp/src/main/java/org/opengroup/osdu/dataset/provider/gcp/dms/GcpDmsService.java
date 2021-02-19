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

package org.opengroup.osdu.dataset.provider.gcp.dms;

import lombok.RequiredArgsConstructor;
import org.apache.http.HttpStatus;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.dataset.dms.DmsException;
import org.opengroup.osdu.dataset.dms.IDmsProvider;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.response.GetDatasetStorageInstructionsResponse;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpDmsServiceProperties;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileService;

@RequiredArgsConstructor
public class GcpDmsService implements IDmsProvider {

	private final IFileService fileDmsService;

	private final GcpDmsServiceProperties dmsServiceProperties;

	@Override
	public GetDatasetStorageInstructionsResponse getStorageInstructions() throws DmsException {
		switch (dmsServiceProperties.getDataSetType()) {
			case FILE:
				return fileDmsService.getFileUploadInstructions();
			case FILE_COLLECTION:
				return fileDmsService.getCollectionUploadInstructions();
			default:
				throw new AppException(HttpStatus.SC_BAD_REQUEST, "Bad request", "Invalid dataset provided");
		}
	}

	@Override
	public GetDatasetRetrievalInstructionsResponse getDatasetRetrievalInstructions(GetDatasetRegistryRequest request)
		throws DmsException {
		switch (dmsServiceProperties.getDataSetType()) {
			case FILE:
				return fileDmsService.getFileRetrievalInstructions(request);
			case FILE_COLLECTION:
				return fileDmsService.getCollectionRetrievalInstructions(request);
			default:
				throw new AppException(HttpStatus.SC_BAD_REQUEST, "Bad request", "Invalid dataset provided");
		}
	}
}
