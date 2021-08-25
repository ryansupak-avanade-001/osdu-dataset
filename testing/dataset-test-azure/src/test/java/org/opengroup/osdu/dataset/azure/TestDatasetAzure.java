/*
 * Copyright 2021 Microsoft Corporation
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

package org.opengroup.osdu.dataset.azure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.core.common.dms.model.StorageInstructionsResponse;
import org.opengroup.osdu.dataset.DatasetIT;
import org.opengroup.osdu.dataset.azure.util.AzureTestUtils;
import org.junit.Before;
import org.opengroup.osdu.dataset.azure.util.CloudStorageUtilAzure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDatasetAzure extends DatasetIT {

    private static final AzureTestUtils azureTestUtils = new AzureTestUtils();

    public TestDatasetAzure() {
        cloudStorageUtil = new CloudStorageUtilAzure();
    }

    @BeforeClass
    public static void classSetup() throws Exception {
        DatasetIT.classSetup(azureTestUtils.getToken());
    }

    @AfterClass
    public static void classTearDown() throws Exception {
        DatasetIT.classTearDown(azureTestUtils.getToken());
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
        assertTrue(storageInstructions.getStorageLocation().containsKey("fileSource"));
    }

    @Override
    public void validateRetrievalInstructions(RetrievalInstructionsResponse retrievalInstructions,
                                              int expectedDatasets) {
        assertEquals("AZURE", retrievalInstructions.getProviderKey());
        assertEquals(expectedDatasets, retrievalInstructions.getDatasets().size());
    }
}
