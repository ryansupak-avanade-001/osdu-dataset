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

package org.opengroup.osdu.dataset.schema;

import org.opengroup.osdu.core.common.http.HttpClient;
import org.opengroup.osdu.core.common.http.json.HttpResponseBodyMapper;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;

public class SchemaFactory implements ISchemaFactory {
    
    private final SchemaAPIConfig config;
    private final HttpResponseBodyMapper bodyMapper;

    public SchemaFactory(SchemaAPIConfig config, HttpResponseBodyMapper bodyMapper) {
        if (config == null) {
            throw new IllegalArgumentException("SchemaAPIConfig cannot be empty");
        }

        this.config = config;
        this.bodyMapper = bodyMapper;
    }

    @Override
    public ISchemaService create(DpsHeaders headers) {
        if (headers == null) {
            throw new NullPointerException("headers cannot be null");
        }
        return new SchemaService(this.config, new HttpClient(), headers, bodyMapper);
    }
}
