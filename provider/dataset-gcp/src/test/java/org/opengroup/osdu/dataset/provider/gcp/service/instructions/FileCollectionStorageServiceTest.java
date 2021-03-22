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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.time.Instant;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.core.gcp.multitenancy.GcsMultiTenantAccess;
import org.opengroup.osdu.core.gcp.multitenancy.TenantFactory;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedCredentials;
import org.opengroup.osdu.dataset.provider.gcp.service.instructions.downscoped.DownScopedCredentialsService;
import org.opengroup.osdu.dataset.provider.gcp.util.GoogleStorageBucketUtil;
import org.opengroup.osdu.dataset.provider.gcp.util.InstantHelper;

@RunWith(MockitoJUnitRunner.class)
public class FileCollectionStorageServiceTest {

	@Mock
	private GcsMultiTenantAccess gcsMultiTenantAccess;

	@Mock
	private Storage storage;

	@Mock
	private StorageOptions storageOptions;

	@Mock
	private InstantHelper instantHelper;

	@Mock
	private DpsHeaders headers;

	@Mock
	private TenantFactory tenantFactory;

	@Mock
	private TenantInfo tenantInfo;

	@Mock
	private GoogleStorageBucketUtil bucketUtil;

	@Mock
	private GoogleCredentials googleCredentials;

	@Mock
	private DownScopedCredentialsService downScopedCredentialsService;

	@Mock
	private DownScopedCredentials downScopedCredentials;

	@Mock
	private AccessToken downScopedToken;

	private String downScopedTokenValue = "connectionString";

	@InjectMocks
	private FileCollectionStorageService collectionStorageService;

	private String bucketName = "osdu-sample-osdu-file";
	private String key = "1590050272122-2020-05-21-08-37-52-122/cebdd5780fc74f24b518c9676160136f/";
	private String unsignedUrl = "gs://" + bucketName + "/" + key;

	@Before
	public void before() throws IOException {
		when(tenantFactory.getTenantInfo(any())).thenReturn(tenantInfo);
		when(storage.getOptions()).thenReturn(storageOptions);
		when(storageOptions.getCredentials()).thenReturn(googleCredentials);
		when(gcsMultiTenantAccess.get(any())).thenReturn(storage);
		when(downScopedCredentialsService
			.getDownScopedCredentials(any(), any()))
			.thenReturn(downScopedCredentials);
		when(downScopedCredentials.refreshAccessToken()).thenReturn(downScopedToken);
		when(downScopedToken.getTokenValue()).thenReturn(downScopedTokenValue);
		when(bucketUtil.getBucketPath(tenantInfo)).thenReturn(bucketName);
	}

	@Test
	public void givenFolderResource_whenCreateDeliveryInstruction_thenCreatedProperly() throws IOException {
		when(storage.get(any(BlobId.class))).thenReturn(null);

		Instant instant = Instant.now();
		when(instantHelper.getCurrentInstant()).thenReturn(instant);

		FileCollectionInstructionsItem expected = FileCollectionInstructionsItem.builder()
			.unsignedUrl(unsignedUrl)
			.createdAt(instant)
			.connectionString(downScopedTokenValue)
			.build();

		FileCollectionInstructionsItem actual = collectionStorageService
			.createCollectionDeliveryItem(unsignedUrl);

		assertEquals(expected, actual);
	}

	@Test
	public void shouldGetUploadInstruction() {
		Instant instant = Instant.now();
		when(instantHelper.getCurrentInstant()).thenReturn(instant);
		FileCollectionInstructionsItem actual = collectionStorageService.getCollectionUploadItem();
		assertEquals(instant, actual.getCreatedAt());
		assertEquals(downScopedTokenValue, actual.getConnectionString());
		assertTrue(actual.getUnsignedUrl().endsWith("/"));
	}

	@Test
	public void createSignedUrl_malformedUnsignedUrl_throwsAppException() {
		try {
			String unsignedUrl = "malformedUrlString";

			FileCollectionInstructionsItem collectionDeliveryItem = collectionStorageService
				.createCollectionDeliveryItem(unsignedUrl);

			fail("Should not succeed!");
		} catch (AppException e) {
			assertEquals(HttpStatus.SC_BAD_REQUEST, e.getError().getCode());
			assertEquals("Malformed URL", e.getError().getReason());
			assertEquals("Unsigned url invalid, needs to be full GS path", e.getError().getMessage());
		} catch (Exception e) {
			fail("Should not get different exception");
		}
	}
}