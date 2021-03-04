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

package org.opengroup.osdu.dataset.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Level;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import lombok.extern.java.Log;
import org.jetbrains.annotations.NotNull;
import org.opengroup.osdu.dataset.CloudStorageUtil;
import org.opengroup.osdu.dataset.configuration.GcpConfig;
import org.opengroup.osdu.dataset.configuration.MapperConfig;
import org.opengroup.osdu.dataset.credentials.StorageServiceAccountCredentialsProvider;
import org.opengroup.osdu.dataset.provider.gcp.model.FileCollectionInstructionsItem;
import org.opengroup.osdu.dataset.provider.gcp.model.FileInstructionsItem;

@Log
public class CloudStorageUtilGcp extends CloudStorageUtil {

	private final ObjectMapper objectMapper;

	private final Storage storage;

	public CloudStorageUtilGcp() {
		objectMapper = MapperConfig.getObjectMapper();
		storage = StorageOptions.newBuilder()
			.setCredentials(StorageServiceAccountCredentialsProvider.getCredentials())
			.setProjectId(GcpConfig.getProjectID()).build()
			.getService();
	}

	public String uploadCloudFileUsingProvidedCredentials(String fileName, Object storageLocationProperties,
		String fileContents) {
		FileInstructionsItem fileInstructionsItem = objectMapper
			.convertValue(storageLocationProperties, FileInstructionsItem.class);

		Client client = GcpTestUtils.getClient();

		try {
			WebResource resource = client.resource(fileInstructionsItem.getSignedUrl().toURI());
			Builder builder = resource.accept(MediaType.APPLICATION_JSON).type(MediaType.TEXT_PLAIN);
			ClientResponse put = builder.method(HttpMethod.PUT, ClientResponse.class, fileContents);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Upload file by signed URL FAIL", e);
		}
		return fileInstructionsItem.getUnsignedUrl();
	}

	public String uploadCollectionUsingProvidedCredentials(String fileName, Object storageLocationProperties,
		String fileContents) {
		FileCollectionInstructionsItem instructionsItem = objectMapper
			.convertValue(storageLocationProperties, FileCollectionInstructionsItem.class);

		Storage instructionsBasedService = getStorageServiceFromInstruction(instructionsItem);

		BlobId blobId = getBlobId(fileName, instructionsItem.getUnsignedUrl());
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
		Blob blob = instructionsBasedService.create(blobInfo);

		try (WriteChannel writer = blob.writer()) {
			writer.write(ByteBuffer.wrap(fileContents.getBytes(UTF_8), 0, fileContents.length()));
		} catch (IOException e) {
			log.log(Level.SEVERE, "Upload collection by instructions FAIL", e);
		}
		return instructionsItem.getUnsignedUrl();
	}

	public String downloadCloudFileUsingDeliveryItem(Object deliveryItem) {
		FileInstructionsItem fileInstructionsItem = objectMapper
			.convertValue(deliveryItem, FileInstructionsItem.class);
		try {
			return FileUtils.readFileFromUrl(fileInstructionsItem.getSignedUrl());
		} catch (IOException e) {
			log.log(Level.SEVERE, "Download file by signed URL FAIL", e);
		}
		return null;
	}

	public String downloadCollectionFileUsingDeliveryItem(Object deliveryItem, String fileName) {
		FileCollectionInstructionsItem instructionsItem = objectMapper
			.convertValue(deliveryItem, FileCollectionInstructionsItem.class);

		Storage instructionsBasedService = getStorageServiceFromInstruction(instructionsItem);

		BlobId blobId = getBlobId(fileName, instructionsItem.getUnsignedUrl());
		Blob blob = instructionsBasedService.get(blobId);
		byte[] content = blob.getContent();
		return new String(content, UTF_8);
	}

	@NotNull
	private BlobId getBlobId(String fileName, String unsignedUrl) {
		String[] gsPathParts = unsignedUrl.split("gs://");
		String[] gsObjectKeyParts = gsPathParts[1].split("/");
		String bucketName = gsObjectKeyParts[0];
		String filePath = String.join("/", Arrays.copyOfRange(gsObjectKeyParts, 1, gsObjectKeyParts.length));
		return BlobId.of(bucketName, filePath + "/" + fileName);
	}

	private Storage getStorageServiceFromInstruction(FileCollectionInstructionsItem instructionsItem) {
		String token = instructionsItem.getConnectionString();
		Credentials credentials = GoogleCredentials.create(new AccessToken(token, null));
		return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
	}

	public void deleteCloudFile(String unsignedUrl) {
		String[] gsPathParts = unsignedUrl.split("gs://");
		String[] gsObjectKeyParts = gsPathParts[1].split("/");
		String bucketName = gsObjectKeyParts[0];

		if (unsignedUrl.endsWith("/")) {
			Page<Bucket> buckets = storage.list();
			for (Bucket bucket : buckets.getValues()) {
				if (bucket.getName().contains(bucketName)) {
					Page<Blob> blobs = bucket.list(BlobListOption.prefix(gsObjectKeyParts[1] + "/"));
					for (Blob collectionBlobs : blobs.getValues()) {
						log.info("Post test cleanup, deleting blob:" + collectionBlobs.getName());
						storage.delete(collectionBlobs.getBlobId());
					}
				}
			}
		} else {
			BlobId fileBlobId = BlobId.of(bucketName, gsObjectKeyParts[1] + "/" + gsObjectKeyParts[2]);
			log.info("Post test cleanup, deleting blob:" + fileBlobId.getName());
			storage.delete(fileBlobId);
		}
	}
}
