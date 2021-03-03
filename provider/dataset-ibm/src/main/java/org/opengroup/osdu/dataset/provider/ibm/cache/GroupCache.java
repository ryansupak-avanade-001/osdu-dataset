/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.dataset.provider.ibm.cache;

import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.entitlements.Groups;
import org.opengroup.osdu.core.common.cache.RedisCache;
import org.opengroup.osdu.core.common.cache.VmCache;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class GroupCache extends VmCache<String, Groups> {
    public GroupCache(@Value("${VM_CACHE_EXPIRATION}") final String VM_CACHE_EXPIRATION,
            @Value("${MAX_CACHE_VALUE_SIZE}") final String MAX_CACHE_VALUE_SIZE) {
        super(Integer.parseInt(VM_CACHE_EXPIRATION),Integer.parseInt(MAX_CACHE_VALUE_SIZE));
    }

    public static String getGroupCacheKey(DpsHeaders headers) {
        String key = String.format("entitlement-groups:%s:%s", headers.getPartitionIdWithFallbackToAccountId(),
                headers.getAuthorization());
        return Crc32c.hashToBase64EncodedString(key);
    }

    public static String getPartitionGroupsCacheKey(String dataPartitionId) {
        String key = String.format("entitlement-groups:data-partition:%s", dataPartitionId);
        return Crc32c.hashToBase64EncodedString(key);
    }
}
