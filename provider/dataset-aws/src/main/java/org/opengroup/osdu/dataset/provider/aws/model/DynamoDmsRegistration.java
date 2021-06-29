package org.opengroup.osdu.dataset.provider.aws.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@DynamoDBTable(tableName = "Dataset.DmsRegistration") // DynamoDB table name (without environment prefix)
public class DynamoDmsRegistration {

    @DynamoDBHashKey(attributeName = "datasetKind")
    private String datasetKind;

    @DynamoDBAttribute(attributeName = "apiBase")
    private String apiBase;

    @DynamoDBAttribute(attributeName = "route")
    private String route;

    @DynamoDBAttribute(attributeName = "isStorageAllowed")
    private Boolean isStorageAllowed;
    
}
