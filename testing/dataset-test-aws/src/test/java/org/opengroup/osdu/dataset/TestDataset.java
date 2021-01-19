// Copyright © 2021 Amazon Web Services
// Copyright 2017-2019, Schlumberger
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.opengroup.osdu.dataset;

import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opengroup.osdu.dataset.model.IntTestFileDeliveryItemAWSImpl;
import org.opengroup.osdu.dataset.model.IntTestFileUploadLocationAWSImpl;
import org.opengroup.osdu.dataset.model.response.IntTestDatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.util.CloudStorageUtilAws;

public class TestDataset extends Dataset {

    private static final AWSTestUtils awsTestUtils = new AWSTestUtils();

    public TestDataset() {
        cloudStorageUtil = new CloudStorageUtilAws();
    }

    @BeforeClass
    public static void classSetup() throws Exception {
        Dataset.classSetup(awsTestUtils.getToken());
    }

    @AfterClass
    public static void classTearDown() throws Exception {
        Dataset.classTearDown(awsTestUtils.getToken());
    }

    @Before
    @Override
    public void setup() throws Exception {
        this.testUtils = new AWSTestUtils();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        this.testUtils = null;
    }

    @Override
    public void validate_storageLocation(Object storageLocation) {

        IntTestFileUploadLocationAWSImpl awsStorageLocation = jsonMapper.convertValue(storageLocation,
                IntTestFileUploadLocationAWSImpl.class);

        Assert.assertNotNull(awsStorageLocation.getConnectionString());
        Assert.assertNotNull(awsStorageLocation.getRegion());
        Assert.assertNotNull(awsStorageLocation.getUnsignedUrl());
        Assert.assertNotNull(awsStorageLocation.getCreatedAt());
        Assert.assertNotNull(awsStorageLocation.getCredentials());
    }

    @Override
    public void validate_dataset_retrieval_delivery_item(IntTestDatasetRetrievalDeliveryItem deliveryItem) {
        
        Assert.assertEquals(deliveryItem.getProviderKey(), TestUtils.providerKey);  
        
        IntTestFileDeliveryItemAWSImpl awsDeliveryItem = jsonMapper.convertValue(deliveryItem.getRetrievalProperties(),
        IntTestFileDeliveryItemAWSImpl.class);
        
        Assert.assertNotNull(awsDeliveryItem.getConnectionString());
        Assert.assertNotNull(awsDeliveryItem.getRegion());
        Assert.assertNotNull(awsDeliveryItem.getUnsignedUrl());
        Assert.assertNotNull(awsDeliveryItem.getCreatedAt());
        Assert.assertNotNull(awsDeliveryItem.getCredentials());
        Assert.assertNotNull(awsDeliveryItem.getFileName());
        Assert.assertNotNull(awsDeliveryItem.getSignedUrl());
        Assert.assertNotNull(awsDeliveryItem.getSignedUrlExpiration());

    }

}