/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.dataset.provider.ibm;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

    @Value("${DMS_API_BASE}")
	private String DMS_API_BASE;

    private Map<String,DmsServiceProperties> resourceTypeToDmsServiceMap = new HashMap<>();

    @PostConstruct
    public void init() {
       
        //todo: replace this with service discovery / registered db entries
        resourceTypeToDmsServiceMap.put(
            "dataset--File.*", 
            new DmsServiceProperties(StringUtils.join(DMS_API_BASE, "/api/dms/file/v1/file"))
        );
        
        resourceTypeToDmsServiceMap.put(
            "dataset--FileCollection.*", 
            new DmsServiceProperties(StringUtils.join(DMS_API_BASE, "/api/dms/file/v1/file-collection"))
        );
    }

    @Override
    public Map<String, DmsServiceProperties> getResourceTypeToDmsServiceMap() {
        return resourceTypeToDmsServiceMap;
    }
    
}
