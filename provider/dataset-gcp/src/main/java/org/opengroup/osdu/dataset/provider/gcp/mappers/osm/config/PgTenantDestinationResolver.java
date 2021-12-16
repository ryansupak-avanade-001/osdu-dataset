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

import com.zaxxer.hikari.HikariDataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.partition.IPartitionFactory;
import org.opengroup.osdu.core.common.partition.IPartitionProvider;
import org.opengroup.osdu.core.common.partition.PartitionException;
import org.opengroup.osdu.core.common.partition.PartitionInfo;
import org.opengroup.osdu.core.common.partition.Property;
import org.opengroup.osdu.core.gcp.osm.model.Destination;
import org.opengroup.osdu.core.gcp.osm.translate.TranslatorRuntimeException;
import org.opengroup.osdu.core.gcp.osm.translate.postgresql.PgDestinationResolution;
import org.opengroup.osdu.core.gcp.osm.translate.postgresql.PgDestinationResolver;
import org.opengroup.osdu.dataset.provider.gcp.config.GcpConfigProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "osmDriver", havingValue = "postgres")
@RequiredArgsConstructor
@Slf4j
public class PgTenantDestinationResolver implements PgDestinationResolver {

  private static final String DATASOURCE = "datasource";
  private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

  private final DpsHeaders dpsHeaders;
  private final IPartitionFactory partitionFactory;
  private final PgOsmConfigurationProperties pgOsmConfigurationProperties;
  private final GcpConfigProperties gcpConfigProperties;
  private final Map<String, DataSource> dataSourceCache = new HashMap<>();

  @Override
  public PgDestinationResolution resolve(Destination destination) {
    String partitionId = destination.getPartitionId();

    DataSource dataSource = dataSourceCache.get(partitionId);
    if (dataSource == null || (dataSource instanceof HikariDataSource
        && ((HikariDataSource) dataSource).isClosed())) {
      synchronized (dataSourceCache) {
        dataSource = dataSourceCache.get(partitionId);
        if (dataSource == null || (dataSource instanceof HikariDataSource
            && ((HikariDataSource) dataSource).isClosed())) {

          IPartitionProvider partitionProvider = partitionFactory.create(dpsHeaders);
          PartitionInfo partitionInfo;
          try {
            partitionInfo = partitionProvider.get(destination.getPartitionId());
          } catch (PartitionException e) {
            throw new TranslatorRuntimeException(e, "Partition '{}' destination resolution issue",
                destination.getPartitionId());
          }
          Map<String, Property> partitionProperties = partitionInfo.getProperties();

          String prefix = pgOsmConfigurationProperties.getPartitionPropertiesPrefix();
          String delimiter = gcpConfigProperties.getPartitionPropertiesDelimiter();
          String url = getPartitionProperty(partitionId, partitionProperties,
              prefix + delimiter + DATASOURCE + delimiter + "url");
          String username = getPartitionProperty(partitionId, partitionProperties,
              prefix + delimiter + DATASOURCE + delimiter + "username");
          String password = getPartitionProperty(partitionId, partitionProperties,
              prefix + delimiter + DATASOURCE + delimiter + "password");

          dataSource = DataSourceBuilder.create()
              .driverClassName(DRIVER_CLASS_NAME)
              .url(url)
              .username(username)
              .password(password)
              .build();

          HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
          hikariDataSource.setMaximumPoolSize(pgOsmConfigurationProperties.getMaximumPoolSize());
          hikariDataSource.setMinimumIdle(pgOsmConfigurationProperties.getMinimumIdle());
          hikariDataSource.setIdleTimeout(pgOsmConfigurationProperties.getIdleTimeout());
          hikariDataSource.setMaxLifetime(pgOsmConfigurationProperties.getMaxLifetime());
          hikariDataSource.setConnectionTimeout(
              pgOsmConfigurationProperties.getConnectionTimeout());

          dataSourceCache.put(partitionId, dataSource);
        }
      }
    }

    return PgDestinationResolution.builder()
        .datasource(dataSource)
        .build();
  }

  @PreDestroy
  @Override
  public void close() {
    log.info("On pre-destroy. {} DataSources to shutdown", dataSourceCache.size());
    for (DataSource dataSource : dataSourceCache.values()) {
      if (dataSource instanceof HikariDataSource && !((HikariDataSource) dataSource).isClosed()) {
        ((HikariDataSource) dataSource).close();
      }
    }
  }

  private String getPartitionProperty(String partitionId, Map<String, Property> partitionProperties, String fullName) {
    return Optional.ofNullable(partitionProperties.get(fullName))
        .map(Property::getValue)
        .map(Object::toString)
        .orElseThrow(() ->
            new TranslatorRuntimeException(null,
                "Partition %s Postgres OSM destination resolution configuration issue. Property "
                    + "%s' is not provided in PartitionInfo.",
                partitionId, fullName));
  }
}
