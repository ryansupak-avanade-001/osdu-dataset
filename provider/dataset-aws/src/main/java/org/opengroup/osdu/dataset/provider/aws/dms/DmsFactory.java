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

package org.opengroup.osdu.dataset.provider.aws.dms;

import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.core.common.http.HttpClient;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.dms.IDmsFactory;
import org.opengroup.osdu.dataset.dms.IDmsProvider;

@RequiredArgsConstructor
public class DmsFactory implements IDmsFactory {
    private final IHttpClient httpClient = new HttpClient();

    @Override
    public IDmsProvider create(DpsHeaders headers, DmsServiceProperties dmsServiceRoute) {
        return new AwsDmsRestService(dmsServiceRoute, httpClient, headers);
    }
}
