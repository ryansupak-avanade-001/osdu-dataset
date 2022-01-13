/*
 * Copyright 2021  Microsoft Corporation
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

package org.opengroup.osdu.dataset.provider.azure.service;

import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Service
@PropertySource("classpath:osdu-dataset.properties")
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

    private final Map<String, DmsServiceProperties> resourceTypeToDmsServiceMap = new HashMap<>();

    @Value("${osdu.kind.file}")
    private String fileKind;

    @Value("${osdu.kind.file.collection}")
    private String fileCollectionKind;

    @Value("${osdu.api.endpoints.file}")
    private String fileApi;

    @Value("${osdu.api.endpoints.file.collection}")
    private String fileCollectionApi;

    @PostConstruct
    public void init() {
        DmsServiceProperties fileDmsProperties = new DmsServiceProperties(fileApi);
        fileDmsProperties.setStagingLocationSupported(true);

        //TODO: replace this with static or dynamic registration of DMS
        resourceTypeToDmsServiceMap.put(fileKind, fileDmsProperties);
        resourceTypeToDmsServiceMap.put(fileCollectionKind, getDmsServicePropertyForFileCollection());
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
