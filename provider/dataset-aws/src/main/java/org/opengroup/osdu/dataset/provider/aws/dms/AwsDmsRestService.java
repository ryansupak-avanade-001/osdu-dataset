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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.opengroup.osdu.core.common.dms.model.DatasetRetrievalProperties;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.dms.DmsRestService;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.DatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;


public class AwsDmsRestService extends DmsRestService {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final DmsServiceProperties dmsServiceProperties;
    private final IHttpClient httpClient;
    private final DpsHeaders headers;

    public AwsDmsRestService(DmsServiceProperties dmsServiceProperties,
                             IHttpClient httpClient,
                             DpsHeaders headers) {
        super(dmsServiceProperties, httpClient, headers);
        this.dmsServiceProperties = dmsServiceProperties;
        this.httpClient = httpClient;
        this.headers = headers;
    }

    @Override
    public GetDatasetRetrievalInstructionsResponse getDatasetRetrievalInstructions(GetDatasetRegistryRequest request) {
        RetrievalInstructionsResponse retrievalInstructions = super.getRetrievalInstructions(request);
        String providerKey = retrievalInstructions.getProviderKey();

        List<DatasetRetrievalDeliveryItem> datasetRetrievalDeliveryItemList =
            retrievalInstructions.getDatasets()
                .stream()
                .map(dataset -> new DatasetRetrievalDeliveryItem(dataset.getDatasetRegistryId(), dataset.getRetrievalProperties(), providerKey))
                .collect(Collectors.toList());

        return new GetDatasetRetrievalInstructionsResponse(datasetRetrievalDeliveryItemList);
    }

    private String createUrl(String path) {
        try {
            URIBuilder uriBuilder = new URIBuilder(dmsServiceProperties.getDmsServiceBaseUrl());
            uriBuilder.setPath(uriBuilder.getPath() + path);
            return uriBuilder.build().normalize().toString();
        } catch (URISyntaxException e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Invalid URL", e.getMessage(), e);
        }
    }
}
