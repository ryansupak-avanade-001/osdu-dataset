/*
 * Copyright 2021 Google LLC
 * Copyright 2021 EPAM Systems, Inc
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

package org.opengroup.osdu.dataset.provider.gcp.mappers.osm.repository;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.core.gcp.osm.model.query.GetQuery;
import org.opengroup.osdu.core.gcp.osm.service.Context;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.DmsServicePropertiesEntity;
import org.opengroup.osdu.dataset.provider.gcp.mappers.osm.config.IDestinationProvider;
import org.springframework.stereotype.Repository;

@Repository
@Slf4j
@Setter
@RequiredArgsConstructor
public class DmsServicePropertiesRepository {

  private static final String KIND_NAME = "DmsServiceProperties";

  private final IDestinationProvider destinationProvider;
  private final Context context;
  private final TenantInfo tenantInfo;

  public Iterable<DmsServicePropertiesEntity> findAll() {
    GetQuery<DmsServicePropertiesEntity> entities = new GetQuery<>(
        DmsServicePropertiesEntity.class,
        destinationProvider.getDestination(tenantInfo.getName(), KIND_NAME)
    );
    return context.getResultsAsList(entities);
  }
}
