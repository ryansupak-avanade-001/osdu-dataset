// Copyright © 2020 Amazon Web Services
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

package org.opengroup.osdu.dataset.provider.aws;

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
