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

package org.opengroup.osdu.dataset.provider.aws;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.core.aws.dynamodb.DynamoDBQueryHelperV2;
import org.opengroup.osdu.core.aws.dynamodb.IDynamoDBQueryHelperFactory;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.aws.cache.DmsRegistrationCache;
import org.opengroup.osdu.dataset.provider.aws.model.DynamoDmsRegistration;
import org.opengroup.osdu.dataset.provider.aws.model.DmsRegistrations;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

    @Value("${DMS_API_BASE}")
    private String DMS_API_BASE;

    @Inject
    @Qualifier("DmsRegistrationCache")
    DmsRegistrationCache cache;

    @Inject
    DpsHeaders headers;

    @Inject
	private JaxRsDpsLog logger;

    @Value("${aws.parameter.prefix}")
    private String ssmParameterPrefix;

    @Value("${aws.dynamodb.dmsRegistrationTable.ssm.relativePath:/common/dataset/DmsRegistrationTable}")
    private String dmsRegistrationTableRelativePath;

    private DynamoDBQueryHelperV2 queryHelper;

    @Inject    
    private IDynamoDBQueryHelperFactory queryHelperFactory;

    @PostConstruct
    public void init() {

        queryHelper = queryHelperFactory.getQueryHelperUsingSSM(ssmParameterPrefix, dmsRegistrationTableRelativePath);
    }

    @Override
    public Map<String, DmsServiceProperties> getResourceTypeToDmsServiceMap() {

        DmsRegistrations dmsRegistrations = getServicesInfoFromCacheOrDynamo(headers);       

        return dmsRegistrations.getDynamoDmsRegistrations();
    }

    protected DmsRegistrations getServicesInfoFromCacheOrDynamo(DpsHeaders headers) {
		String cacheKey = DmsRegistrationCache.getCacheKey(headers);
		DmsRegistrations dmsRegistrations = (DmsRegistrations) this.cache.get(cacheKey);

		if (dmsRegistrations == null) {			
			try {
                ArrayList<DynamoDmsRegistration> test = queryHelper.scanTable(DynamoDmsRegistration.class);
        
                HashMap<String, DmsServiceProperties> resourceTypeToDmsServiceMap = new HashMap<>();                
        
                for (DynamoDmsRegistration reg : test) {

                    String apiBase = "";
                    if (StringUtils.isNotEmpty(DMS_API_BASE)) {
                        apiBase = DMS_API_BASE;
                    }
                    else {
                        apiBase = reg.getApiBase();
                    }

                    resourceTypeToDmsServiceMap.put(reg.getDatasetKind(), new DmsServiceProperties(
                        StringUtils.join(apiBase, reg.getRoute()),
                        reg.getIsStorageAllowed()
                    ));
                }

                dmsRegistrations = new DmsRegistrations(resourceTypeToDmsServiceMap);

				this.cache.put(cacheKey, dmsRegistrations);
				this.logger.info("DMS Registration cache miss");

			} catch (Exception e) {
				e.printStackTrace();				
				throw new AppException(HttpStatus.INTERNAL_SERVER_ERROR.value(), HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(), "Failed to get DMS Service Registrations");
			}
		}

		return dmsRegistrations;
	}
    
}
