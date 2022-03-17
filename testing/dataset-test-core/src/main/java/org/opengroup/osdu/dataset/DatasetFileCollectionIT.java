package org.opengroup.osdu.dataset;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.opengroup.osdu.core.common.dms.model.DatasetRetrievalProperties;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsRequest;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.core.common.dms.model.StorageInstructionsResponse;
import org.opengroup.osdu.core.common.model.entitlements.Acl;
import org.opengroup.osdu.core.common.model.legal.Legal;
import org.opengroup.osdu.core.common.model.legal.LegalCompliance;
import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.dataset.model.shared.TestGetCreateUpdateDatasetRegistryRequest;
import org.opengroup.osdu.dataset.util.LegalTagUtils;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public abstract class DatasetFileCollectionIT extends TestBase {

    protected static FileCollectionCloudStorageUtil fileCollectionCloudStorageUtil;
    private static String FIRST_FILE_NAME = "upload_first.txt";
    private static String SECOND_FILE_NAME = "upload_second.txt";
    private static String THIRD_FILE_NAME = "upload_third.txt";
    private static final String FIRST_FILE_CONTENT = "foo-bar1";
    private static final String SECOND_FILE_CONTENT = "foo-bar2";
    private static final String THIRD_FILE_CONTENT = "foo-bar3";

    protected static boolean runTests = false;

    protected ObjectMapper jsonMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

    public abstract void validateRetrievalInstructions(RetrievalInstructionsResponse retrievalInstructionsResponse,
                                                       int expectedDatasets);

    public abstract void validateStorageInstructions(StorageInstructionsResponse storageInstructionsResponse);

    protected static String LEGAL_TAG = LegalTagUtils.createRandomName();

    public static void classSetup(String token) throws Exception {
        ClientResponse legalTagCreateResponse = LegalTagUtils.create(
                HeaderUtils.getHeaders(TenantUtils.getTenantName(), token),
                LEGAL_TAG);
    }

    public static void classTearDown(String token) throws Exception {
        LegalTagUtils.delete(HeaderUtils.getHeaders(TenantUtils.getTenantName(), token), LEGAL_TAG);
    }

    @Test
    public void upload_file_register_it_and_retrieve_it() throws Exception {
        Assume.assumeTrue(runTests);
        String kindSubType = "dataset--FileCollection.Generic";

        //Step 1: Get Storage Instructions for File
        StorageInstructionsResponse storageInstResponse = storageInstructions(kindSubType);
        String fileCollectionPath = storageInstResponse.getStorageLocation().get("fileCollectionSource").toString();

        //Step 2: Upload File
        fileCollectionCloudStorageUtil.uploadFile(storageInstResponse.getStorageLocation(), FIRST_FILE_NAME, FIRST_FILE_CONTENT);
        fileCollectionCloudStorageUtil.uploadFile(storageInstResponse.getStorageLocation(), SECOND_FILE_NAME, SECOND_FILE_CONTENT);
        fileCollectionCloudStorageUtil.uploadFile(storageInstResponse.getStorageLocation(), THIRD_FILE_NAME, THIRD_FILE_CONTENT);

        //Step 3: Register File
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String datasetRegistryId = String.format("%s:%s:%s", TenantUtils.getTenantName(),kindSubType, uuid);

        Record datasetRegistry = createDatasetRegistry(datasetRegistryId, fileCollectionPath);

        ClientResponse datasetRegistryResponse = testRegisterDatasetRequest(Collections.singletonList(datasetRegistry));
        System.out.println("Registry response " + datasetRegistryResponse.getEntity(String.class));
        assertEquals(201, datasetRegistryResponse.getStatus());


        //Step 4: Retrieve File and validate contents
        ClientResponse retrievalClientResponse = getTestRetrievalInstructions(Collections.singletonList(datasetRegistryId));
        assertEquals(200, retrievalClientResponse.getStatus());

        String getRetrievalRespStr = retrievalClientResponse.getEntity(String.class);
        RetrievalInstructionsResponse retrievalInstResponse = jsonMapper.readValue(getRetrievalRespStr,
                RetrievalInstructionsResponse.class);
        validateRetrievalInstructions(retrievalInstResponse, 1);

        // Download file
        ByteArrayOutputStream firstDownloadedContent = fileCollectionCloudStorageUtil.downloadFile(
                retrievalInstResponse.getDatasets().get(0).getRetrievalProperties(), FIRST_FILE_NAME);
        ByteArrayOutputStream secondDownloadedContent = fileCollectionCloudStorageUtil.downloadFile(
                retrievalInstResponse.getDatasets().get(0).getRetrievalProperties(), SECOND_FILE_NAME);
        ByteArrayOutputStream thirdDownloadedContent = fileCollectionCloudStorageUtil.downloadFile(
                retrievalInstResponse.getDatasets().get(0).getRetrievalProperties(), THIRD_FILE_NAME);

        assertEquals(FIRST_FILE_CONTENT, firstDownloadedContent.toString());
        assertEquals(SECOND_FILE_CONTENT, secondDownloadedContent.toString());
        assertEquals(THIRD_FILE_CONTENT, thirdDownloadedContent.toString());
    }


    private StorageInstructionsResponse storageInstructions(String kindSubType) throws Exception {
        StorageInstructionsResponse getStorageInstResponse = getTestStorageInstructions(kindSubType, 200);
        validateStorageInstructions(getStorageInstResponse);
        return getStorageInstResponse;
    }

    private ClientResponse getTestRetrievalInstructions(List<String> datasetRegistryIds) throws Exception {
        RetrievalInstructionsRequest getDatasetRequest = new RetrievalInstructionsRequest();
        getDatasetRequest.setDatasetRegistryIds(datasetRegistryIds);

        return TestUtils.send("retrievalInstructions", "POST",
                HeaderUtils.getHeaders(TenantUtils.getTenantName(), testUtils.getToken()),
                jsonMapper.writeValueAsString(getDatasetRequest),
                "");
    }

    private ClientResponse testRegisterDatasetRequest(List<Record> datasetRegistries) throws Exception {
        TestGetCreateUpdateDatasetRegistryRequest datasetRegistryRequest = new TestGetCreateUpdateDatasetRegistryRequest(new ArrayList<>());
        datasetRegistryRequest.getDatasetRegistries().addAll(datasetRegistries);

        return TestUtils.send(TestUtils.datasetBaseUrl, "registerDataset", "PUT",
                HeaderUtils.getHeaders(TenantUtils.getTenantName(), testUtils.getToken()),
                jsonMapper.writeValueAsString(datasetRegistryRequest), "");
    }

    private StorageInstructionsResponse getTestStorageInstructions(String kindSubType, int expectedStatusCode) throws Exception {
        ClientResponse getStorageInstClientResp = TestUtils.send("storageInstructions", "POST",
                HeaderUtils.getHeaders(TenantUtils.getTenantName(), testUtils.getToken()),
                "", String.format("?kindSubType=%s", kindSubType));

        int actualStatusCode = getStorageInstClientResp.getStatus();
        assertEquals(expectedStatusCode, actualStatusCode);

        if (actualStatusCode != 200) {
            return null;
        }

        String getStorageRespStr = getStorageInstClientResp.getEntity(String.class);
        return jsonMapper.readValue(getStorageRespStr, StorageInstructionsResponse.class);
    }

    private static Record createDatasetRegistry(String id, String fileCollectionSource) {
        TestGetCreateUpdateDatasetRegistryRequest request = new TestGetCreateUpdateDatasetRegistryRequest();

        Record datasetRegistry = new Record();

        datasetRegistry.setId(id);
        datasetRegistry.setKind(String.format("%s:wks:dataset--FileCollection.Generic:1.0.0", TestUtils.getSchemaAuthority()));

        //set legal
        Legal legal = new Legal();
        HashSet<String> legalTags = new HashSet<>();
        legalTags.add(LEGAL_TAG);
        legal.setLegaltags(legalTags);
        HashSet<String> otherRelevantDataCountries = new HashSet<>();
        otherRelevantDataCountries.add("US");
        legal.setOtherRelevantDataCountries(otherRelevantDataCountries);
        legal.setStatus(LegalCompliance.compliant);
        datasetRegistry.setLegal(legal);

        //set acl
        Acl acl = new Acl();
        String[] viewers = new String[] { String.format("data.default.viewers@%s", getAclSuffix()) };
        acl.setViewers(viewers);
        String[] owners = new String[] { String.format("data.default.owners@%s",getAclSuffix()) };
        acl.setOwners(owners);
        datasetRegistry.setAcl(acl);

        // Data
        HashMap<String, Object> data = new HashMap<>();
        HashMap<String, Object> datasetProperties = new HashMap<>();

        datasetProperties.put("FileCollectionPath", fileCollectionSource);
        data.put("DatasetProperties", datasetProperties);
        datasetRegistry.setData(data);

        return datasetRegistry;
    }

    private static String getAclSuffix() {
        String environment = TestUtils.getEnvironment();
        if (environment.equalsIgnoreCase("empty")) {
            environment = "";
        }

        if (!environment.isEmpty()) {
            environment = "." + environment;
        }

        return String.format("%s%s.%s", TestUtils.getTenantName(), environment, TestUtils.getDomain());
    }
}
