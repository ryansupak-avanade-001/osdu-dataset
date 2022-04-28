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

package org.opengroup.osdu.dataset.provider.aws.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ProviderConfigurationBag {

    @Value("${DMS_API_BASE}")
    public String dmsApiBase;

    @Value("${aws.parameter.prefix}")
    public String ssmParameterPrefix;

    @Value("${aws.dynamodb.dmsRegistrationTable.ssm.relativePath:/common/dataset/DmsRegistrationTable}")
    public String dmsRegistrationTableRelativePath;

    @Value("${aws.elasticache.cluster.endpoint:null}")
    public String redisSearchHost;

    @Value("${aws.elasticache.cluster.port:null}")
    public String redisSearchPort;

    @Value("${aws.elasticache.cluster.key:null}")
    public String redisSearchKey;
}
