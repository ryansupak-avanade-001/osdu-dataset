package org.opengroup.osdu.datasetregistry.model;

import lombok.Data;

@Data
public class StorageExceptionResponse {

    private Integer code;
    private String reason;
    private String message;
    
}
