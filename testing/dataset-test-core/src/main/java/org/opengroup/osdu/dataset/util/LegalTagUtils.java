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

package org.opengroup.osdu.dataset.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.jersey.api.client.ClientResponse;
import org.opengroup.osdu.dataset.TestUtils;

import java.util.Map;

import static org.opengroup.osdu.dataset.TestUtils.getTenantName;

public class LegalTagUtils {
    public static String createRandomName() {
        return getTenantName() + "-dataset-" + System.currentTimeMillis();
    }

    public static ClientResponse create(Map<String, String> headers,
                                        String legalTagName) throws Exception {
        String body = getBody("US", legalTagName, "2099-01-25", "Public Domain Data");
        return TestUtils.send(getLegalUrl(), "legaltags", "POST", headers, body, "");
    }

    public static ClientResponse delete(Map<String, String> headers,
                                        String legalTagName) throws Exception {
        return TestUtils.send(getLegalUrl(), "legaltags", "DELETE", headers, null, "");
    }

    protected static String getLegalUrl() {
        String legalUrl = System.getProperty("LEGAL_HOST", System.getenv("LEGAL_HOST"));
        if (legalUrl == null || legalUrl.contains("-null")) {
            throw new RuntimeException("Legal URL not configured");
        }
        return legalUrl;
    }

    protected static String getBody(String countryOfOrigin, String name, String expDate, String dataType) {
        JsonArray coo = new JsonArray();
        coo.add(countryOfOrigin);

        JsonObject properties = new JsonObject();
        properties.add("countryOfOrigin", coo);
        properties.addProperty("contractId", "A1234");
        properties.addProperty("expirationDate", expDate);
        properties.addProperty("dataType", dataType);
        properties.addProperty("originator", "MyCompany");
        properties.addProperty("securityClassification", "Public");
        properties.addProperty("exportClassification", "EAR99");
        properties.addProperty("personalData", "No Personal Data");

        JsonObject tag = new JsonObject();
        tag.addProperty("name", name);
        tag.addProperty("description", "test for " + name);
        tag.add("properties", properties);

        return tag.toString();
    }
}
