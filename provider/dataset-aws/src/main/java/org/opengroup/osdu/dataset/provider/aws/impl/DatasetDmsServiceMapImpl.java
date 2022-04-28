// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opengroup.osdu.dataset.provider.aws.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.core.aws.dynamodb.DynamoDBQueryHelperV2;
import org.opengroup.osdu.core.aws.dynamodb.IDynamoDBQueryHelperFactory;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.aws.cache.DmsRegistrationCache;
import org.opengroup.osdu.dataset.provider.aws.config.ProviderConfigurationBag;
import org.opengroup.osdu.dataset.provider.aws.model.DmsRegistrations;
import org.opengroup.osdu.dataset.provider.aws.model.DynamoDmsRegistration;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

    private final ProviderConfigurationBag providerConfigurationBag;
    private final DmsRegistrationCache cache;
    private final DpsHeaders headers;
    private final JaxRsDpsLog logger;
    private final DynamoDBQueryHelperV2 queryHelper;

    @Autowired
    public DatasetDmsServiceMapImpl(DpsHeaders headers,
                                    ProviderConfigurationBag providerConfigurationBag,
                                    @Qualifier("DmsRegistrationCache") DmsRegistrationCache cache,
                                    IDynamoDBQueryHelperFactory queryHelperFactory,
                                    JaxRsDpsLog logger) {
        this.providerConfigurationBag = providerConfigurationBag;
        this.cache = cache;
        this.headers = headers;
        this.logger = logger;
        this.queryHelper = queryHelperFactory.getQueryHelperUsingSSM(providerConfigurationBag.ssmParameterPrefix,
                                                                     providerConfigurationBag.dmsRegistrationTableRelativePath);
    }

    @Override
    public Map<String, DmsServiceProperties> getResourceTypeToDmsServiceMap() {
        DmsRegistrations dmsRegistrations = getServicesInfoFromCacheOrDynamoDb(headers);

        return dmsRegistrations.getDynamoDmsRegistrations();
    }

    protected DmsRegistrations getServicesInfoFromCacheOrDynamoDb(DpsHeaders headers) {
        String cacheKey = DmsRegistrationCache.getCacheKey(headers);
        DmsRegistrations dmsRegistrations = this.cache.get(cacheKey);

        if (dmsRegistrations == null) {
            try {
                ArrayList<DynamoDmsRegistration> dynamoDmsRegistrations = queryHelper.scanTable(DynamoDmsRegistration.class);
                HashMap<String, DmsServiceProperties> resourceTypeToDmsServiceMap = new HashMap<>();

                for (DynamoDmsRegistration dmsRegistration : dynamoDmsRegistrations) {
                    String apiBase = StringUtils.isNotEmpty(providerConfigurationBag.dmsApiBase) ? providerConfigurationBag.dmsApiBase
                                                                                                 : dmsRegistration.getApiBase();
                    DmsServiceProperties dmsServiceProperties = new DmsServiceProperties(StringUtils.join(apiBase, dmsRegistration.getRoute()),
                                                                                         dmsRegistration.getIsStorageAllowed());
                    resourceTypeToDmsServiceMap.put(dmsRegistration.getDatasetKind(), dmsServiceProperties);
                }

                dmsRegistrations = new DmsRegistrations(resourceTypeToDmsServiceMap);
                this.cache.put(cacheKey, dmsRegistrations);

                this.logger.info("DMS Registration cache miss");
            } catch (Exception e) {
                log.error("Error occurred.", e);
                throw new AppException(HttpStatus.INTERNAL_SERVER_ERROR.value(),
                                       HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(),
                                       "Failed to get DMS Service Registrations");
            }
        }

        return dmsRegistrations;
    }
}
