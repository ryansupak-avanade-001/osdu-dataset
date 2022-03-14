package org.opengroup.osdu.dataset;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public abstract class FileCollectionCloudStorageUtil {

    public abstract void uploadFile(Map<String, Object> storageLocation, String fileName, String content) throws Exception;

    public abstract ByteArrayOutputStream downloadFile(Map<String, Object> retrievalProperties, String fileName) throws Exception;
}
