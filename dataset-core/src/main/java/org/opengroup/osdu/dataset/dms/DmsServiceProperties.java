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

package org.opengroup.osdu.dataset.dms;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DmsServiceProperties {
    
    //url to dms api base endpoint. ex: https://some-url/api/dms/file/v1/file-collection
    private String dmsServiceBaseUrl;

    //mark this is as false if the DMS service can only retrieve datasets, but cant store new ones via OSDU
    private boolean allowStorage = true;

    //optional API key if required for connecting to service
    private String apiKey;

    //optional copy dms support if staging containers are supported by DMS implementers
    private boolean stagingLocationSupported = false;

    //additional constructors
    public DmsServiceProperties(String dmsServiceBaseUrl) {
        this.dmsServiceBaseUrl = dmsServiceBaseUrl;
    }

    public DmsServiceProperties(String dmsServiceBaseUrl, boolean allowStorage) {
        this(dmsServiceBaseUrl);

        this.allowStorage = allowStorage;
    }

}
