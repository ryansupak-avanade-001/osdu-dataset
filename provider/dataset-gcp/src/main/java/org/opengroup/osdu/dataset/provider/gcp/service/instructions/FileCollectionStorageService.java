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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.core.common.provider.interfaces.ITenantFactory;
import org.opengroup.osdu.core.gcp.multitenancy.GcsMultiTenantAccess;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem.FileCollectionInstructionsItemBuilder;
import org.opengroup.osdu.dataset.provider.gcp.model.GcsRole;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.AccessBoundaryRule;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.AvailabilityCondition;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedCredentials;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedCredentialsService;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedOptions;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileCollectionStorageService;
import org.opengroup.osdu.dataset.provider.gcp.util.GoogleStorageBucketUtil;
import org.opengroup.osdu.dataset.provider.gcp.util.InstantHelper;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.context.annotation.RequestScope;

@Slf4j
@Service
@RequestScope
@RequiredArgsConstructor
public class FileCollectionStorageService implements IFileCollectionStorageService {

	private static final String AVAILABILITY_CONDITION_EXPRESSION =
		"resource.name.startsWith('projects/_/buckets/<<bucket>>/objects/<<folder>>/') " +
			"|| api.getAttribute('storage.googleapis.com/objectListPrefix', '').startsWith('<<folder>>')";

	private static final String MALFORMED_URL = "Malformed URL";
	private static final String URI_EXCEPTION_REASON = "Exception creating signed url";
	private static final String INVALID_GS_PATH_REASON = "Unsigned url invalid, needs to be full GS path";

	private final GcsMultiTenantAccess gcsMultiTenantAccess;

	private final InstantHelper instantHelper;

	private final DpsHeaders headers;

	private final ITenantFactory tenantFactory;

	private final GoogleStorageBucketUtil bucketUtil;

	private final DownScopedCredentialsService downscopedCredentialsService;

	@Override
	public FileCollectionInstructionsItem createCollectionDeliveryItem(String unsignedUrl) {
		Instant now = instantHelper.getCurrentInstant();
		TenantInfo tenantInfo = tenantFactory.getTenantInfo(headers.getPartitionId());
		Storage storage = getStorage(tenantInfo);

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
			.builder().createdAt(now).unsignedUrl(unsignedUrl);

		BlobId blobId = BlobId.of(bucketName, filePath);
		Blob blob = storage.get(blobId);

		if (Objects.nonNull(blob)) {
			log.error("Resource is a blob, cannot proceed with token generation.");
			throw new AppException(org.apache.http.HttpStatus.SC_BAD_REQUEST, MALFORMED_URL,
				URI_EXCEPTION_REASON);
		} else {
			DownScopedCredentials downScopedCredentials = getDownScopedCredentials(bucketName, filePath,
				GcsRole.STORAGE_OBJECT_VIEWER, storage.getOptions().getCredentials());
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
	public FileCollectionInstructionsItem getCollectionUploadItem() {
		TenantInfo tenantInfo = tenantFactory.getTenantInfo(headers.getPartitionId());
		Storage storage = getStorage(tenantInfo);
		Instant now = instantHelper.getCurrentInstant();

		String filePath = getRelativePath();
		String bucketName = bucketUtil.getBucketPath(tenantInfo);

		log.debug("Creating the folder for name : {}. PartitionID : {}",
			filePath, headers.getPartitionId());

		BlobId blobId = BlobId.of(bucketName, filePath + "/");

		BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
			.build();

		storage.create(blobInfo, ArrayUtils.EMPTY_BYTE_ARRAY);

		String fileSource = "gs://" + bucketName + "/" + filePath + "/";

		FileCollectionInstructionsItemBuilder instructionsItemBuilder = FileCollectionInstructionsItem
			.builder().createdAt(now)
			.unsignedUrl(fileSource);

		DownScopedCredentials downScopedCredentials = getDownScopedCredentials(bucketName, filePath,
			GcsRole.STORAGE_OBJECT_ADMIN, storage.getOptions().getCredentials());
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

	private Storage getStorage(TenantInfo tenantInfo) {
		return gcsMultiTenantAccess.get(tenantInfo);
	}

	private DownScopedCredentials getDownScopedCredentials(String bucketName, String filePath, String storageRole,
		Credentials credentials) {
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
			.getDownScopedCredentials((GoogleCredentials) credentials, downScopedOptions);
	}

	private String getRelativePath() {
		return UUID.randomUUID().toString();
	}
}
