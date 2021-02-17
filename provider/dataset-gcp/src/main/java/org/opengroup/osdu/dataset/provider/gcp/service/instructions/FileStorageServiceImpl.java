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

import static java.lang.String.format;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.SignUrlOption;
import java.net.URL;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.gcp.multitenancy.TenantFactory;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileStorageService;
import org.opengroup.osdu.dataset.provider.gcp.model.FileInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.FileInstructionsItem.FileInstructionsItemBuilder;
import org.opengroup.osdu.dataset.provider.gcp.properties.GcpPropertiesConfig;
import org.opengroup.osdu.dataset.provider.gcp.util.InstantHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FileStorageServiceImpl implements IFileStorageService {

	public static final String MALFORMED_URL = "Malformed URL";
	private static final String URI_EXCEPTION_REASON = "Exception creating signed url";
	private static final String INVALID_GS_PATH_REASON = "Unsigned url invalid, needs to be full GS path";
	private static final HttpMethod signedUrlMethod = HttpMethod.GET;

	@Autowired
	private Storage storage;

	@Autowired
	private DpsHeaders headers;

	@Autowired
	private InstantHelper instantHelper;

	@Autowired
	private GcpPropertiesConfig config;


	@Autowired
	TenantFactory tenantFactory;

	@Override
	public FileInstructionsItem createDeliveryItem(String unsignedUrl, String fileName) {
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

		FileInstructionsItemBuilder instructionsItemBuilder = FileInstructionsItem.builder().createdAt(now);

		BlobId blobId = BlobId.of(bucketName, filePath);
		Blob blob = storage.get(blobId);

		if (Objects.nonNull(blob)) {
			log.debug("resource is a blob. get SignedUrl");
			URL url = generateSignedGcURL(blobId);
			instructionsItemBuilder.url(url).unsignedUrl(unsignedUrl).createdAt(now);
		}
		return instructionsItemBuilder.build();
	}

	@Override
	public FileInstructionsItem getUploadLocation() {
		Instant now = Instant.now(Clock.systemUTC());

		String filepath = getRelativePath();
		String bucketName = config.getUploadBucket();

		log.debug("Creating the signed blob for fileName : {}. PartitionID : {}",
			filepath, headers.getPartitionId());

		BlobId blobId = BlobId.of(bucketName, filepath);

		BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
			.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE)
			.build();

		Blob blob = storage.create(blobInfo, ArrayUtils.EMPTY_BYTE_ARRAY);

		URL signedUrl = storage.signUrl(blobInfo, 7L, TimeUnit.DAYS,
			SignUrlOption.httpMethod(HttpMethod.PUT),
			SignUrlOption.withV4Signature());

		log.debug("Signed URL for created storage object. Object ID : {} , Signed URL : {}",
			blob.getGeneratedId(), signedUrl);

		String fileSource = "gs://" + config.getUploadBucket() + "/" + filepath;

		return FileInstructionsItem.builder().url(signedUrl).unsignedUrl(fileSource).createdAt(now).build();
	}

	@Override
	public String getProviderKey() {
		return null;
	}

	private URL generateSignedGcURL(BlobId blobId) {

		BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
			.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE)
			.build();

		return storage.signUrl(blobInfo,
			config.getExpirationDays(), TimeUnit.DAYS,
			SignUrlOption.httpMethod(signedUrlMethod),
			SignUrlOption.withV4Signature());
	}

	private String getRelativePath() {
		String folderName = UUID.randomUUID().toString();
		String fileName = UUID.randomUUID().toString();

		return format("%s/%s", folderName, fileName);
	}
}
