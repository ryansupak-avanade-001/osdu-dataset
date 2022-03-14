package org.opengroup.osdu.dataset.azure;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.core.common.dms.model.StorageInstructionsResponse;
import org.opengroup.osdu.dataset.DatasetFileCollectionIT;
import org.opengroup.osdu.dataset.DatasetIT;
import org.opengroup.osdu.dataset.azure.util.AzureTestUtils;
import org.opengroup.osdu.dataset.azure.util.FileCollectionCloudStorageUtilAzure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDatasetFileCollectionAzure extends DatasetFileCollectionIT {

    private static final AzureTestUtils azureTestUtils = new AzureTestUtils();

    public TestDatasetFileCollectionAzure() {
        fileCollectionCloudStorageUtil = new FileCollectionCloudStorageUtilAzure();
    }

    @BeforeClass
    public static void classSetup() throws Exception {
        DatasetFileCollectionIT.classSetup(azureTestUtils.getToken());
    }

    @AfterClass
    public static void classTearDown() throws Exception {
        DatasetFileCollectionIT.classTearDown(azureTestUtils.getToken());
    }

    @Before
    @Override
    public void setup() throws Exception {
        runTests = true;
        testUtils = azureTestUtils;
    }

    @Override
    public void tearDown() throws Exception {

    }

    @Override
    public void validateStorageInstructions(StorageInstructionsResponse storageInstructions) {
        assertEquals("AZURE", storageInstructions.getProviderKey());
        assertTrue(storageInstructions.getStorageLocation().containsKey("signedUrl"));
        assertTrue(storageInstructions.getStorageLocation().containsKey("fileCollectionSource"));
    }

    @Override
    public void validateRetrievalInstructions(RetrievalInstructionsResponse retrievalInstructions,
                                              int expectedDatasets) {
        assertEquals("AZURE", retrievalInstructions.getProviderKey());
        assertEquals(expectedDatasets, retrievalInstructions.getDatasets().size());
    }
}
