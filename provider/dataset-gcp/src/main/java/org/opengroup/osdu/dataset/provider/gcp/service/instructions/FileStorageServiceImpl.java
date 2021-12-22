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

import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.core.common.provider.interfaces.ITenantFactory;
import org.opengroup.osdu.core.gcp.obm.driver.Driver;
import org.opengroup.osdu.core.gcp.obm.model.Blob;
import org.opengroup.osdu.core.gcp.obm.persistence.ObmDestination;
import org.opengroup.osdu.dataset.provider.gcp.config.GcpConfigProperties;
import org.opengroup.osdu.dataset.provider.gcp.di.EnvironmentResolver;
import org.opengroup.osdu.dataset.provider.gcp.mappers.osm.config.IDestinationProvider;
import org.opengroup.osdu.dataset.provider.gcp.model.FileInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.interfaces.IFileStorageService;
import org.opengroup.osdu.dataset.provider.gcp.util.PathUtil;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.context.annotation.RequestScope;

@Slf4j
@RequiredArgsConstructor
@Service
@RequestScope
public class FileStorageServiceImpl implements IFileStorageService {

	private static final String MALFORMED_URL = "Malformed URL";
	private static final String URI_EXCEPTION_REASON = "Exception creating signed url";

	private final Driver obmDriver;
	private final DpsHeaders headers;
	private final GcpConfigProperties config;
	private final PathUtil pathUtil;
	private final ITenantFactory tenantFactory;
	private final EnvironmentResolver environmentResolver;

	@Override
	public FileInstructionsItem createFileDeliveryItem(String unsignedUrl) {
		String[] gsObjectKeyParts = pathUtil.getObjectKeyParts(unsignedUrl);
		String bucketName = gsObjectKeyParts[0];
		String filePath = String
			.join("/", Arrays.copyOfRange(gsObjectKeyParts, 1, gsObjectKeyParts.length));

		Blob blob = obmDriver.getBlob(bucketName, filePath,getDestination());

		if (Objects.isNull(blob)) {
			log.error("Resource is not a blob, cannot proceed with signed url generation.");
			throw new AppException(HttpStatus.BAD_REQUEST.value(), MALFORMED_URL,
				URI_EXCEPTION_REASON);
		}
		log.debug("Resource is a blob. Generating SignedUrl");
		URL signedUrl = obmDriver.getSignedUrlForDownload(bucketName, getDestination(), filePath,
				config.getExpirationDays(), TimeUnit.DAYS);

		return FileInstructionsItem.builder()
				.unsignedUrl(unsignedUrl)
				.signedUrl(signedUrl)
				.createdAt(Instant.now())
				.build();
	}

	@Override
	public FileInstructionsItem getFileUploadItem() {
		TenantInfo tenantInfo = tenantFactory.getTenantInfo(headers.getPartitionId());

		String filePath = pathUtil.buildRelativePath();
		String bucketName = pathUtil.getBucketPath(tenantInfo);

		log.debug("Creating the signed blob for fileName : {}. PartitionID : {}",
			filePath, headers.getPartitionId());
		URL signedUrl = obmDriver.getSignedUrlForUpload(bucketName, getDestination(), filePath,
				config.getExpirationDays(), TimeUnit.DAYS);
		log.debug("Signed URL for created storage object. Signed URL : {}", signedUrl);
		String fileSource = environmentResolver.getTransferProtocol() + bucketName + "/" + filePath;

		return FileInstructionsItem.builder()
				.unsignedUrl(fileSource)
				.signedUrl(signedUrl)
				.createdAt(Instant.now())
				.build();
	}

	private ObmDestination getDestination() {
		TenantInfo tenantInfo = tenantFactory.getTenantInfo(headers.getPartitionId());
		return getDestination(tenantInfo.getDataPartitionId());
	}

	private ObmDestination getDestination(String dataPartitionId) {
		return ObmDestination.builder().partitionId(dataPartitionId).build();
	}
}
