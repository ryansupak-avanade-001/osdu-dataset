/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.dataset.ibm.util;
import java.util.HashMap;
import java.util.Map;

import org.opengroup.osdu.dataset.CloudStorageUtil;
import org.opengroup.osdu.dataset.TestUtils;

import com.sun.jersey.api.client.ClientResponse;

public class CloudStorageUtilIbm extends CloudStorageUtil {

    @Override
    public String uploadCloudFileUsingProvidedCredentials(String fileName, Object storageLocationProperties,
                                                          String fileContents) throws Exception {
        // Upload File to Signed URL
        Map<String, String> fileUploadHeaders = new HashMap<>();
        fileUploadHeaders.put("x-ms-blob-type", "BlockBlob");

        Map<String, Object> storageLocation = (Map<String, Object>) storageLocationProperties;
        ClientResponse fileUploadResponse = TestUtils.send(
                storageLocation.get("signedUrl").toString(), "",
                "PUT", fileUploadHeaders, fileContents, null);

        return storageLocation.get("fileSource").toString();
    }

    @Override
    public String downloadCloudFileUsingDeliveryItem(Object retrievalLocationProperties) throws Exception {

        Map<String, Object> retrievalProperties = (Map<String, Object>) retrievalLocationProperties;
        ClientResponse fileUploadResponse = TestUtils.send(
                retrievalProperties.get("signedUrl").toString(), "",
                "GET", new HashMap<>(), "", null);
        String downloadedFileResp = null;
        downloadedFileResp = fileUploadResponse.getEntity(String.class);
        return downloadedFileResp;
    }

    @Override
    public void deleteCloudFile(String unsignedUrl) {
    	
    }
}