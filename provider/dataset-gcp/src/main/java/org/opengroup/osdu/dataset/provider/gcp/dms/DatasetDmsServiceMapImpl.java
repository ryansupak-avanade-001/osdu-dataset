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

package org.opengroup.osdu.dataset.provider.gcp.dms;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.provider.gcp.config.GcpConfigProperties;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.DataSetType;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.DmsServicePropertiesEntity;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.GcpDmsServiceProperties;
import org.opengroup.osdu.dataset.provider.gcp.mappers.osm.repository.DmsServicePropertiesRepository;
import org.opengroup.osdu.dataset.provider.interfaces.IDatasetDmsServiceMap;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DatasetDmsServiceMapImpl implements IDatasetDmsServiceMap {

  private final DmsServicePropertiesRepository dmsServicePropertiesRepository;
  private final GcpConfigProperties gcpConfigProperties;

  @Override
  public Map<String, DmsServiceProperties> getResourceTypeToDmsServiceMap() {
    Iterable<DmsServicePropertiesEntity> properties = this.dmsServicePropertiesRepository.findAll();
    String dmsApiBase = this.gcpConfigProperties.getDmsApiBase();
    return StreamSupport.stream(properties.spliterator(), false)
        .collect(Collectors.toMap(DmsServicePropertiesEntity::getDatasetKind,
            entity -> GcpDmsServiceProperties.builder()
                .dmsServiceBaseUrl(StringUtils.isEmpty(dmsApiBase) ? entity.getDmsServiceBaseUrl() : dmsApiBase)
                .allowStorage(entity.isStorageAllowed())
                .apiKey(entity.getApiKey())
                .stagingLocationSupported(entity.isStagingLocationSupported())
                .dataSetType(StringUtils.containsIgnoreCase(entity.getDatasetKind(),
                    "collection") ? DataSetType.FILE_COLLECTION : DataSetType.FILE)
                .build()));
  }
}
