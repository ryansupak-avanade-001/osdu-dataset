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

//TODO: Move to os-core-common

package org.opengroup.osdu.dataset.schema;

import com.google.gson.JsonSyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.core.common.http.HttpRequest;
import org.opengroup.osdu.core.common.http.HttpResponse;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.http.json.HttpResponseBodyMapper;
import org.opengroup.osdu.core.common.http.json.HttpResponseBodyParsingException;
import org.opengroup.osdu.core.common.model.http.DpsException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;

public class SchemaService implements ISchemaService {
    private final String rootUrl;
    private final IHttpClient httpClient;
    private final DpsHeaders headers;
    private final HttpResponseBodyMapper bodyMapper;

    public SchemaService(SchemaAPIConfig config, IHttpClient httpClient, DpsHeaders headers, HttpResponseBodyMapper bodyMapper) {

        this.rootUrl = config.getRootUrl();
        this.httpClient = httpClient;
        this.headers = headers;
        this.bodyMapper = bodyMapper;

        if (config.apiKey != null) {
            headers.put("AppKey", config.apiKey);
        }

    }    

    @Override
    public Object getSchema(String kind) throws DpsException {
        String url = this.createUrl("/schema/" + kind);
        HttpResponse result = this.httpClient
                .send(HttpRequest.get().url(url).headers(this.headers.getHeaders()).build());
        return this.getResult(result, Object.class);
    }

    private String createUrl(String pathAndQuery) {
        return StringUtils.join(this.rootUrl, pathAndQuery);
    }

    private <T> T getResult(HttpResponse result, Class<T> type) throws DpsException {
        if (result.isSuccessCode()) {
            try {
                return bodyMapper.parseBody(result, type);
            } catch (HttpResponseBodyParsingException e) {
                throw new DpsException("Error parsing response. Check the inner HttpResponse for more info.",
                        result);
            }
        } else {
            throw this.generateException(result);
        }
    }

    private DpsException generateException(HttpResponse result) {
        return new DpsException(
                "Error making request to Schema service. Check the inner HttpResponse for more info.", result);
    }

}
