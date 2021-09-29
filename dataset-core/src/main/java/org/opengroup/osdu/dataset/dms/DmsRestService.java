/*
 * Copyright 2021 Microsoft Corporation
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

package org.opengroup.osdu.dataset.dms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URISyntaxException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.opengroup.osdu.core.common.dms.model.CopyDmsRequest;
import org.opengroup.osdu.core.common.dms.model.CopyDmsResponse;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.core.common.http.HttpRequest;
import org.opengroup.osdu.core.common.http.HttpResponse;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.response.GetDatasetStorageInstructionsResponse;

@RequiredArgsConstructor
public class DmsRestService implements IDmsProvider {

    private final DmsServiceProperties dmsServiceProperties;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final IHttpClient httpClient;
    private final DpsHeaders headers;

    // TODO: Use StorageInstructionsResponse from OS-Core-Lib once existing APIs are removed.
    @Override
    public GetDatasetStorageInstructionsResponse getStorageInstructions() {
        String url = this.createUrl("/storageInstructions");
        HttpResponse result = this.httpClient
                .send(HttpRequest.post().url(url).headers(this.headers.getHeaders()).build());
        try {
            return OBJECT_MAPPER.readValue(result.getBody(), GetDatasetStorageInstructionsResponse.class);
        } catch (JsonProcessingException e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Internal Server Error", e.getMessage(), e);
        }
    }

    @Override
    public GetDatasetRetrievalInstructionsResponse getDatasetRetrievalInstructions(GetDatasetRegistryRequest request) {
        // The REST APIs of Dataset Service have a new response format.
        // TODO: This method will be deleted from the interface once the old Non-REST compliant APIs are deprecated and deleted.
        return null;
    }

    @Override
    public RetrievalInstructionsResponse getRetrievalInstructions(GetDatasetRegistryRequest request) {
        String url = this.createUrl("/retrievalInstructions");
        HttpResponse result = this.httpClient
                .send(HttpRequest.post(request).url(url).headers(this.headers.getHeaders()).build());

        try {
            return OBJECT_MAPPER.readValue(result.getBody(), RetrievalInstructionsResponse.class);
        } catch (JsonProcessingException e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Internal Server Error", e.getMessage(), e);
        }
    }

    @Override
    public List<CopyDmsResponse> copyDmsToPersistentStorage(CopyDmsRequest copyDmsRequest) {
        String url = this.createUrl("/copy");
        HttpResponse result = this.httpClient
                .send(HttpRequest.post(copyDmsRequest).url(url).headers(this.headers.getHeaders()).build());
        try {
            return OBJECT_MAPPER.readValue(result.getBody(), new TypeReference<List<CopyDmsResponse>>(){});
        } catch (JsonProcessingException e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Internal Server Error", e.getMessage(), e);
        }
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
