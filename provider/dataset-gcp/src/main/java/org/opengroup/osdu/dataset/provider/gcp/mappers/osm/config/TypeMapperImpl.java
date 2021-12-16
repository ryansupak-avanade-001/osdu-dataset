/*
 *  Copyright 2020-2021 Google LLC
 *  Copyright 2020-2021 EPAM Systems, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.opengroup.osdu.dataset.provider.gcp.mappers.osm.config;

import com.google.cloud.datastore.Key;
import java.util.Collections;
import org.opengroup.osdu.core.gcp.osm.persistence.IdentityTranslator;
import org.opengroup.osdu.core.gcp.osm.translate.Instrumentation;
import org.opengroup.osdu.core.gcp.osm.translate.TypeMapper;
import org.opengroup.osdu.dataset.provider.gcp.model.dataset.DmsServicePropertiesEntity;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "osmDriver")
public class TypeMapperImpl extends TypeMapper {

  public TypeMapperImpl() {
    super(Collections.singletonList(
        new Instrumentation<>(DmsServicePropertiesEntity.class,
            Collections.emptyMap(),
            Collections.emptyMap(),
            new IdentityTranslator<>(
                DmsServicePropertiesEntity::getDatasetKind,
                (r, o) -> r.setDatasetKind(((Key) o).getName())
            ),
            Collections.singletonList("datasetKind")
        )
    ));
  }
}
