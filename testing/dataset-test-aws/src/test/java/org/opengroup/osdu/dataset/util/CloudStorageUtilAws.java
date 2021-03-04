// Copyright © 2021 Amazon Web Services
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

package org.opengroup.osdu.dataset.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.core.aws.s3.S3Config;
import org.opengroup.osdu.dataset.CloudStorageUtil;
import org.opengroup.osdu.dataset.model.IntTestCredentials;
import org.opengroup.osdu.dataset.model.IntTestCredentialsProvider;
import org.opengroup.osdu.dataset.model.IntTestFileDeliveryItem;
import org.opengroup.osdu.dataset.model.IntTestFileDeliveryItemAWSImpl;
import org.opengroup.osdu.dataset.model.IntTestFileUploadLocation;
import org.opengroup.osdu.dataset.model.IntTestFileUploadLocationAWSImpl;
import org.opengroup.osdu.dataset.model.IntTestS3Location;
import org.springframework.util.StreamUtils;

public class CloudStorageUtilAws extends CloudStorageUtil {

    private ObjectMapper jsonMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
    
    private AmazonS3 s3Master;

    public CloudStorageUtilAws() {
        String region = AwsConfig.getCloudStorageRegion();
        String storageEndpoint = String.format("s3.%s.amazonaws.com", region);
        S3Config config = new S3Config(storageEndpoint, region);
        s3Master = config.amazonS3();
    }

    private AmazonS3 generateS3ClientWithCredentials(IntTestCredentials credentials) {
        return AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(AwsConfig.getCloudStorageRegion()))
                .withCredentials(new IntTestCredentialsProvider(credentials)).build();
    }

    @Override
    public String uploadCloudFileUsingProvidedCredentials(String fileName, Object storageLocation, String fileContents) {
                
        IntTestFileUploadLocationAWSImpl awsStorageLocation = jsonMapper.convertValue(storageLocation, IntTestFileUploadLocationAWSImpl.class);

        AmazonS3 client = generateS3ClientWithCredentials(awsStorageLocation.getCredentials());

        String unsignedUrl = StringUtils.join(awsStorageLocation.getUnsignedUrl(), fileName);

        IntTestS3Location s3Location = new IntTestS3Location(unsignedUrl);

        client.putObject(s3Location.bucket, s3Location.key, fileContents);

        return unsignedUrl;

    }

    @Override
    public String downloadCloudFileUsingDeliveryItem(Object deliveryItem) {


        IntTestFileDeliveryItemAWSImpl awsDeliveryItem = jsonMapper.convertValue(deliveryItem,
        IntTestFileDeliveryItemAWSImpl.class);
        
        AmazonS3 client = generateS3ClientWithCredentials(awsDeliveryItem.getCredentials());

        IntTestS3Location s3Location = new IntTestS3Location(awsDeliveryItem.getUnsignedUrl());

        S3Object downloadedFile = client.getObject(s3Location.bucket, s3Location.key);
        
        try {
            InputStream is = downloadedFile.getObjectContent();
            return StreamUtils.copyToString(is, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            System.out.println(e);
            System.out.println("Failed to read S3 File");
        }

        return null;
        

    }

    @Override
    public void deleteCloudFile(String unsignedUrl) {        
        IntTestS3Location s3Location = new IntTestS3Location(unsignedUrl);

        s3Master.deleteObject(s3Location.bucket, s3Location.key);
    }

    // @Override
    // public String createCloudFile(String fileName){
    //     // s3.putObject(testBucketName, fileName, "");
    //     // return String.format("s3://%s/%s", testBucketName, fileName);
    // }

    // @Override
    // public void deleteCloudFile(String bucketName,String fileName) {
    //     // s3.deleteObject(bucketName, fileName);
    // }
    
}
