// Copyright © 2020 Amazon Web Services
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

package org.opengroup.osdu.dataset.storage;

import java.util.List;

import com.google.gson.JsonSyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.opengroup.osdu.core.common.http.HttpRequest;
import org.opengroup.osdu.core.common.http.HttpResponse;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.MultiRecordIds;
import org.opengroup.osdu.core.common.model.storage.MultiRecordInfo;
import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.core.common.model.storage.Schema;

public class StorageService implements IStorageProvider {
    private final String rootUrl;
    private final IHttpClient httpClient;
    private final DpsHeaders headers;

    public StorageService(StorageAPIConfig config, IHttpClient httpClient, DpsHeaders headers) {

        this.rootUrl = config.getRootUrl();
        this.httpClient = httpClient;
        this.headers = headers;

        if (config.apiKey != null) {
            headers.put("AppKey", config.apiKey);
        }

    }

    @Override
    public CreateUpdateRecordsResponse createOrUpdateRecords(List<Record> records) throws StorageException {
        String url = this.createUrl("/records");
        HttpResponse result = this.httpClient
                .send(HttpRequest.put(records).url(url).headers(this.headers.getHeaders()).build());
        return this.getResult(result, CreateUpdateRecordsResponse.class);
    }

    @Override
    public GetRecordsResponse getRecords(MultiRecordIds ids) throws StorageException {
        String url = this.createUrl("/query/records");
        HttpResponse result = this.httpClient
                .send(HttpRequest.post(ids).url(url).headers(this.headers.getHeaders()).build());
        return this.getResult(result, GetRecordsResponse.class);
    }

    @Override
    public Schema getSchema(String kind) throws StorageException {
        String url = this.createUrl("/schemas/" + kind);
        HttpResponse result = this.httpClient
                .send(HttpRequest.get().url(url).headers(this.headers.getHeaders()).build());
        return this.getResult(result, Schema.class);
    }

    private String createUrl(String pathAndQuery) {
        return StringUtils.join(this.rootUrl, pathAndQuery);
    }

    private <T> T getResult(HttpResponse result, Class<T> type) throws StorageException {
        if (result.isSuccessCode()) {
            try {
                return result.parseBody(type);
            } catch (JsonSyntaxException e) {
                throw new StorageException("Error parsing response. Check the inner HttpResponse for more info.",
                        result);
            }
        } else {
            throw this.generateException(result);
        }
    }

    private StorageException generateException(HttpResponse result) {
        return new StorageException(
                "Error making request to Storage service. Check the inner HttpResponse for more info.", result);
    }

}
