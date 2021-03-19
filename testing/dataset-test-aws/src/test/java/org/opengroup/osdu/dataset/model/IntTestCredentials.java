// Copyright © 2021 Amazon Web Services
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

package org.opengroup.osdu.dataset.model;

import com.amazonaws.auth.AWSSessionCredentials;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class IntTestCredentials implements AWSSessionCredentials {

    @JsonProperty("accessKeyId")
    private String accessKeyId;

    @JsonProperty("secretAccessKey")
    private String secretAccessKey;

    @JsonProperty("sessionToken")
    private String sessionToken;

    @JsonProperty("expiration")
    private Date expiration;

    @Override
    @JsonIgnore
    public String getAWSAccessKeyId() {
        return accessKeyId;
    }

    @Override
    @JsonIgnore
    public String getAWSSecretKey() {
        return secretAccessKey;
    }

    @Override
    @JsonIgnore
    public String getSessionToken() {
        return sessionToken;
    }

}