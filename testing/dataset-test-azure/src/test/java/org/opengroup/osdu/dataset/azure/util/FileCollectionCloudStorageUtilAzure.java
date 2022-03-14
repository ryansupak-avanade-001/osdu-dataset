package org.opengroup.osdu.dataset.azure.util;

import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import org.opengroup.osdu.dataset.FileCollectionCloudStorageUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class FileCollectionCloudStorageUtilAzure extends FileCollectionCloudStorageUtil {

    @Override
    public void uploadFile(Map<String, Object> storageLocation, String fileName, String content) throws Exception {

        String signedUrl = storageLocation.get("signedUrl").toString();
        DataLakeDirectoryClient directoryClient = new DataLakePathClientBuilder()
                .endpoint(signedUrl)
                .buildDirectoryClient();
        DataLakeFileClient fileClient = directoryClient.createFile(fileName);
        InputStream targetStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        long fileSize = content.length();
        fileClient.append(targetStream, 0, fileSize);
        fileClient.flush(fileSize);
        targetStream.close();
    }

    @Override
    public ByteArrayOutputStream downloadFile(Map<String, Object> retrievalProperties, String fileName) throws Exception {

        String signedUrl = retrievalProperties.get("signedUrl").toString();
        DataLakeDirectoryClient directoryClient = new DataLakePathClientBuilder()
                .endpoint(signedUrl)
                .buildDirectoryClient();

        DataLakeFileClient fileClient =
                directoryClient.getFileClient(fileName);

        ByteArrayOutputStream targetStream = new ByteArrayOutputStream();

        fileClient.read(targetStream);
        targetStream.close();
        return targetStream;
    }
}
