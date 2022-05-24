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

package org.opengroup.osdu.dataset.provider.aws.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.opengroup.osdu.core.aws.cache.DummyCache;
import org.opengroup.osdu.core.aws.ssm.K8sLocalParameterProvider;
import org.opengroup.osdu.core.aws.ssm.K8sParameterNotFoundException;
import org.opengroup.osdu.core.common.cache.ICache;
import org.opengroup.osdu.core.common.cache.RedisCache;
import org.opengroup.osdu.core.common.cache.VmCache;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.opengroup.osdu.dataset.provider.aws.config.ProviderConfigurationBag;
import org.opengroup.osdu.dataset.provider.aws.model.DmsRegistrations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("DmsRegistrationCache")
public class DmsRegistrationCache implements ICache<String, DmsRegistrations> {

    private ICache cache;

    @Autowired
    public DmsRegistrationCache(ProviderConfigurationBag providerConfigurationBag) throws K8sParameterNotFoundException, JsonProcessingException {
        K8sLocalParameterProvider provider = new K8sLocalParameterProvider();
        if (provider.getLocalMode()) {
            if (Boolean.parseBoolean(System.getenv("DISABLE_CACHE"))) {
                this.cache = new DummyCache();
            }
            this.cache = new VmCache<>(60, 10);
        } else {
            String host = provider.getParameterAsStringOrDefault("CACHE_CLUSTER_ENDPOINT", providerConfigurationBag.redisSearchHost);
            int port = Integer.parseInt(provider.getParameterAsStringOrDefault("CACHE_CLUSTER_PORT", providerConfigurationBag.redisSearchPort));
            Map<String, String> credential = provider.getCredentialsAsMap("CACHE_CLUSTER_KEY");

            String password = credential != null ? credential.get("token") : providerConfigurationBag.redisSearchKey;
            this.cache = new RedisCache(host, port, password, 60, String.class, DmsRegistrations.class);
        }
    }

    public static String getCacheKey(DpsHeaders headers) {
        String key = String.format("dms-registration:%s:%s", headers.getPartitionIdWithFallbackToAccountId(),
                                   headers.getAuthorization());
        return Crc32c.hashToBase64EncodedString(key);
    }

    @Override
    public void put(String k, DmsRegistrations o) {
        this.cache.put(k, o);
    }

    @Override
    public DmsRegistrations get(String k) {
        return (DmsRegistrations) this.cache.get(k);
    }

    @Override
    public void delete(String k) {
        this.cache.delete(k);
    }

    @Override
    public void clearAll() {
        this.cache.clearAll();
    }
}
