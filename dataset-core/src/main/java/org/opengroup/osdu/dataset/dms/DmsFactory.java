// Copyright © 2021 Amazon Web Services
// Copyright 2017-2019, Schlumberger
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

//TODO: move to os-core-common

package org.opengroup.osdu.dataset.dms;

import org.opengroup.osdu.core.common.http.HttpClient;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.di.DatasetConfig;

public class DmsFactory implements IDmsFactory {

    private DatasetConfig datasetConfig;

    private IHttpClient httpClient = new HttpClient();

    public DmsFactory(DatasetConfig datasetConfig) {
        this.datasetConfig = datasetConfig;
    }

    @Override
    public IDmsProvider create(DpsHeaders headers, DmsServiceProperties dmsServiceProperties) {
        if (headers == null) {
            throw new NullPointerException("headers cannot be null");
        }
        if (datasetConfig.isUseRestDms()) {
            return new DmsRestService(dmsServiceProperties, httpClient, headers);
        } else {
            return new DmsService(dmsServiceProperties, httpClient, headers);
        }
    }
}
