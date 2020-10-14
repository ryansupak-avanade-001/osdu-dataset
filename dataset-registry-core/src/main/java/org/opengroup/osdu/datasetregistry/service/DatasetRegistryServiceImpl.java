// Copyright © 2020 Amazon Web Services
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

package org.opengroup.osdu.datasetregistry.service;

import java.util.*;

import java.util.ArrayList;
import java.util.List;

import org.opengroup.osdu.core.common.entitlements.IEntitlementsAndCacheService;
import org.opengroup.osdu.core.common.model.indexer.OperationType;
import org.apache.http.HttpStatus;

import org.opengroup.osdu.core.common.storage.IPersistenceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import org.opengroup.osdu.core.common.legal.ILegalService;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;

import org.opengroup.osdu.core.common.model.tenant.TenantInfo;

import static java.util.Collections.singletonList;

@Service
public class DatasetRegistryServiceImpl implements DatasetRegistryService {

    @Autowired
    private IEntitlementsAndCacheService entitlementsAndCacheService;

    // @Autowired
    // private ILegalService legalService;

    @Autowired
    private TenantInfo tenant;

    @Autowired
    private DpsHeaders headers;

    @Override
    public void deleteDatasetRegistry(String datasetRegistryId) {
        //todo: implement
    }

    @Override
    public void createOrUpdateDatasetRegistry(Object datasetRegistry) {

    }

}