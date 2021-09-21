/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.dataset.ibm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.core.common.dms.model.StorageInstructionsResponse;
import org.opengroup.osdu.dataset.DatasetIT;
import org.opengroup.osdu.dataset.ibm.util.CloudStorageUtilIbm;
import org.opengroup.osdu.dataset.ibm.util.IBMTestUtils;

public class TestDatasetIbm extends DatasetIT {

    private static final IBMTestUtils ibmTestUtils = new IBMTestUtils();

    public TestDatasetIbm() {
        cloudStorageUtil = new CloudStorageUtilIbm();
    }

    @BeforeClass
    public static void classSetup() throws Exception {
        DatasetIT.classSetup(ibmTestUtils.getToken());
    }

    @AfterClass
    public static void classTearDown() throws Exception {
        DatasetIT.classTearDown(ibmTestUtils.getToken());
    }

    @Before
    @Override
    public void setup() throws Exception {
        runTests = true;
        testUtils = ibmTestUtils;
    }

    @Override
    public void tearDown() throws Exception {

    }

    @Override
    public void validateStorageInstructions(StorageInstructionsResponse storageInstructions) {
        assertEquals("IBM", storageInstructions.getProviderKey());
        assertTrue(storageInstructions.getStorageLocation().containsKey("signedUrl"));
        assertTrue(storageInstructions.getStorageLocation().containsKey("fileSource"));
    }

    @Override
    public void validateRetrievalInstructions(RetrievalInstructionsResponse retrievalInstructions,
                                              int expectedDatasets) {
        assertEquals("IBM", retrievalInstructions.getProviderKey());
        assertEquals(expectedDatasets, retrievalInstructions.getDatasets().size());
    }
}
