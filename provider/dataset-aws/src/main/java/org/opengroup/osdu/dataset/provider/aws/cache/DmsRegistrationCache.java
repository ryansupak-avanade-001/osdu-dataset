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

package org.opengroup.osdu.dataset.provider.aws.cache;

import org.opengroup.osdu.core.common.model.http.DpsHeaders;

import org.opengroup.osdu.core.common.cache.RedisCache;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.opengroup.osdu.dataset.provider.aws.model.DmsRegistrations;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DmsRegistrationCache extends RedisCache<String, DmsRegistrations> {
    public DmsRegistrationCache(@Value("${aws.elasticache.cluster.endpoint}") final String REDIS_HOST, @Value("${aws.elasticache.cluster.port}") final String REDIS_PORT, @Value("${aws.elasticache.cluster.key}") final String REDIS_KEY) {
        super(REDIS_HOST, Integer.parseInt(REDIS_PORT), REDIS_KEY, 300, String.class, DmsRegistrations.class);
    }

    public static String getCacheKey(DpsHeaders headers) {
        String key = String.format("dms-registration:%s:%s", headers.getPartitionIdWithFallbackToAccountId(),
                headers.getAuthorization());
        return Crc32c.hashToBase64EncodedString(key);
    }

}
