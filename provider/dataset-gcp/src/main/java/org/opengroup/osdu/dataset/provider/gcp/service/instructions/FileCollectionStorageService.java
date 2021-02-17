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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.core.gcp.multitenancy.TenantFactory;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.AccessBoundaryRule;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.AvailabilityCondition;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedCredentials;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedCredentialsService;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedOptions;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileCollectionStorageService;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem.FileCollectionInstructionsItemBuilder;
import org.opengroup.osdu.dataset.provider.gcp.properties.GcpPropertiesConfig;
import org.opengroup.osdu.dataset.provider.gcp.util.InstantHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FileCollectionStorageService implements IFileCollectionStorageService {

	public static final String AVAILABILITY_CONDITION_EXPRESSION =
		"resource.name.startsWith('projects/_/buckets/<<bucket>>/objects/<<folder>>/') " +
			"|| api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('<<folder>>')";

	public static final String MALFORMED_URL = "Malformed URL";
	private static final String URI_EXCEPTION_REASON = "Exception creating signed url";
	private static final String INVALID_GS_PATH_REASON = "Unsigned url invalid, needs to be full GS path";
	private static final HttpMethod signedUrlMethod = HttpMethod.GET;

	@Autowired
	private Storage storage;

	@Autowired
	private InstantHelper instantHelper;

	@Autowired
	private DpsHeaders headers;

	@Autowired
	private GcpPropertiesConfig config;

	@Autowired
	TenantFactory tenantFactory;

	@Autowired
	private DownScopedCredentialsService downscopedCredentialsService;

	@Override
	public FileCollectionInstructionsItem createDeliveryItem(String unsignedUrl) {
		Instant now = instantHelper.getCurrentInstant();

		String[] gsPathParts = unsignedUrl.split("gs://");

		if (gsPathParts.length < 2) {
			throw new AppException(HttpStatus.BAD_REQUEST.value(), MALFORMED_URL,
				INVALID_GS_PATH_REASON);
		}

		String[] gsObjectKeyParts = gsPathParts[1].split("/");
		if (gsObjectKeyParts.length < 1) {
			throw new AppException(HttpStatus.BAD_REQUEST.value(), MALFORMED_URL,
				INVALID_GS_PATH_REASON);
		}

		String bucketName = gsObjectKeyParts[0];
		String filePath = String
			.join("/", Arrays.copyOfRange(gsObjectKeyParts, 1, gsObjectKeyParts.length));
		FileCollectionInstructionsItemBuilder instructionsItemBuilder = FileCollectionInstructionsItem
			.builder().createdAt(now).unsignedUrl(filePath);

		BlobId blobId = BlobId.of(bucketName, filePath);
		Blob blob = storage.get(blobId);

		if (Objects.nonNull(blob)) {
			log.error("Resource is a blob, cannot proceed with token generation.");
			throw new AppException(org.apache.http.HttpStatus.SC_BAD_REQUEST, MALFORMED_URL,
				URI_EXCEPTION_REASON);
		} else {
			DownScopedCredentials downScopedCredentials = getDownScopedCredentials(bucketName, filePath,
				"storage.objectViewer");
			try {
				instructionsItemBuilder
					.connectionString(downScopedCredentials.refreshAccessToken().getTokenValue());
			} catch (IOException e) {
				log.error("There was an error getting the DownScoped token.", e);
				throw new AppException(org.apache.http.HttpStatus.SC_BAD_REQUEST, MALFORMED_URL,
					URI_EXCEPTION_REASON, e);
			}
		}
		return instructionsItemBuilder.build();
	}


	@Override
	public FileCollectionInstructionsItem getUploadLocation() {
		TenantInfo tenantInfo = tenantFactory.getTenantInfo(headers.getPartitionId());
		Instant now = Instant.now(Clock.systemUTC());

		String filePath = getRelativePath();
		String bucketName = config.getUploadBucket();

		log.debug("Creating the signed blob for fileName : {}. PartitionID : {}",
			filePath, headers.getPartitionId());

		BlobId blobId = BlobId.of(bucketName, filePath + "/");

		BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
			.build();

		storage.create(blobInfo, ArrayUtils.EMPTY_BYTE_ARRAY);

		String fileSource = "gs://" + config.getUploadBucket() + "/" + filePath + "/";

		FileCollectionInstructionsItemBuilder instructionsItemBuilder = FileCollectionInstructionsItem
			.builder().createdAt(now)
			.unsignedUrl(fileSource);

		DownScopedCredentials downScopedCredentials = getDownScopedCredentials(bucketName, filePath,
			"storage.objectCreator");
		try {
			instructionsItemBuilder
				.connectionString(downScopedCredentials.refreshAccessToken().getTokenValue());
		} catch (IOException e) {
			log.error("There was an error getting the DownScoped token.", e);
			throw new AppException(org.apache.http.HttpStatus.SC_BAD_REQUEST, MALFORMED_URL,
				URI_EXCEPTION_REASON, e);
		}

		return instructionsItemBuilder.build();
	}

	@Override
	public String getProviderKey() {
		return null;
	}


	private DownScopedCredentials getDownScopedCredentials(String bucketName, String filePath, String storageRole) {
		log.debug("resource is not a blob. assume it is a folder. get DownScoped token");

		String availabilityConditionExpression = AVAILABILITY_CONDITION_EXPRESSION
			.replace("<<bucket>>", bucketName)
			.replace("<<folder>>", filePath);

		AvailabilityCondition ap = new AvailabilityCondition("obj", availabilityConditionExpression);

		AccessBoundaryRule abr = new AccessBoundaryRule(
			"//storage.googleapis.com/projects/_/buckets/" + bucketName,
			Collections.singletonList("inRole:roles/" + storageRole),
			ap);

		DownScopedOptions downScopedOptions = new DownScopedOptions(Collections.singletonList(abr));
		return downscopedCredentialsService
			.getDownScopedCredentials(downScopedOptions);
	}

	private String getRelativePath() {
		return UUID.randomUUID().toString();
	}
}
