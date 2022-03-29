/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.dataset.provider.ibm;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

	private Map<String,DmsServiceProperties> resourceTypeToDmsServiceMap = new HashMap<>();

    @Value("${FILE_API}")
    private String fileApi;
    
    @Value("${FILE_COLLECTION_API}")
    private String fileCollectionApi;
    
    @PostConstruct
	public void init() {
    	
    	DmsServiceProperties fileDmsProperties = new DmsServiceProperties(fileApi);
		fileDmsProperties.setStagingLocationSupported(true);

		resourceTypeToDmsServiceMap.put("dataset--File.*", fileDmsProperties);
		resourceTypeToDmsServiceMap.put("dataset--FileCollection.*", getDmsServicePropertyForFileCollection());
	}

    @Override
    public Map<String, DmsServiceProperties> getResourceTypeToDmsServiceMap() {
        return resourceTypeToDmsServiceMap;
    }
    
    private DmsServiceProperties getDmsServicePropertyForFileCollection() {
        DmsServiceProperties fileCollectionDmsProperties = new DmsServiceProperties(fileCollectionApi);
        fileCollectionDmsProperties.setStagingLocationSupported(true);
        return fileCollectionDmsProperties;
    }
    
}
