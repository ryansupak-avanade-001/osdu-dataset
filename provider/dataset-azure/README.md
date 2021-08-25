# Dataset Service Azure implementation

## Running Locally

### Requirements

In order to run this service locally, you will need the following:

- [Maven 3.6.0+](https://maven.apache.org/download.cgi)
- [AdoptOpenJDK8](https://adoptopenjdk.net/)
- Infrastructure dependencies, deployable through the relevant [infrastructure template](https://dev.azure.com/slb-des-ext-collaboration/open-data-ecosystem/_git/infrastructure-templates?path=%2Finfra&version=GBmaster&_a=contents)
- While not a strict dependency, example commands in this document use [bash](https://www.gnu.org/software/bash/)

### General Tips

**Environment Variable Management**
The following tools make environment variable configuration simpler
 - [direnv](https://direnv.net/) - for a shell/terminal environment
 - [EnvFile](https://plugins.jetbrains.com/plugin/7861-envfile) - for [Intellij IDEA](https://www.jetbrains.com/idea/)

**Lombok**
This project uses [Lombok](https://projectlombok.org/) for code generation. You may need to configure your IDE to take advantage of this tool.
 - [Intellij configuration](https://projectlombok.org/setup/intellij)
 - [VSCode configuration](https://projectlombok.org/setup/vscode)

### Environment Variables

In order to run the service locally, you will need to have the following environment variables defined.

**Note** The following command can be useful to pull secrets from keyvault:
```bash
az keyvault secret show --vault-name $KEY_VAULT_NAME --name $KEY_VAULT_SECRET_NAME --query value -otsv
```

**Required to run service**

| name | value | description | sensitive? | source |
| ---  | ---   | ---         | ---        | ---    |
| `entitlements_app_key` | `********` | The API key clients will need to use when calling the entitlements | yes | -- |
| `storage_service_endpoint` | ex `https://foo-legal.azurewebsites.net/api/storage/v1` | Storage API endpoint | no | output of infrastructure deployment |
| `entitlements_service_endpoint` | ex `https://foo-entitlements.azurewebsites.net/api/entitlements/v2` | Entitlements API endpoint | no | output of infrastructure deployment |
| `file_service_endpoint` | ex `https//foo-partition.azurewebsites.net/api/file/v2/files` | File DMS API endpoint | no | output of infrastructure deployment |
| `partition_service_endpoint` | ex `https//foo-partition.azurewebsites.net/api/partition/v1` | Partition API endpoint | no | output of infrastructure deployment |
| `schema_service_endpoint` | ex `https//foo-partition.azurewebsites.net/api/schema-service/v1` | Schema API Endpoint | no | output of infrastructure deployment |
| `appinsights_key` | `********` | API Key for App Insights | yes | output of infrastructure deployment |
| `KEYVAULT_URI` | ex `https://foo-keyvault.vault.azure.net/` | URI of KeyVault that holds application secrets | no | output of infrastructure deployment |
| `AZURE_CLIENT_ID` | `********` | Identity to run the service locally. This enables access to Azure resources. You only need this if running locally | yes | keyvault secret: `$KEYVAULT_URI/secrets/app-dev-sp-username` |
| `AZURE_TENANT_ID` | `********` | AD tenant to authenticate users from | yes | keyvault secret: `$KEYVAULT_URI/secrets/app-dev-sp-tenant-id` |
| `AZURE_CLIENT_SECRET` | `********` | Secret for `$AZURE_CLIENT_ID` | yes | keyvault secret: `$KEYVAULT_URI/secrets/app-dev-sp-password` |
| `aad_client_id` | `********` | AAD client application ID | yes | output of infrastructure deployment |
| `azure_istioauth_enabled` | `true` | Flag to Disable AAD auth | no | -- |
| `server_port` | `8089` | The port on which server should be started | no | |

**Run the service in intellij**

Add VM option `-Dspring.profiles.active=local` in the Edit Configurations Section to activate `application-local.properties` that will avoid unnecessary changes to 
`application.properties`. 

**Required to run integration tests**

| name | value | description | sensitive? | source |
| ---  | ---   | ---         | ---        | ---    |
| `USER_ID` | ex `osdu-user` | Id of the user who triggers load test | no | -- |
| `STORAGE_HOST` | ex `https://foo-legal.azurewebsites.net/api/storage/v1` | Storage API endpoint | no | output of infrastructure deployment |
| `DATASET_BASE_URL` | ex `https://foo-entitlements.azurewebsites.net/api/dataset/v1` | Dataset API endpoint | no | output of infrastructure deployment |
| `LEGAL_HOST` | ex `https://foo-legal.azurewebsites.net/api/legal/v1` | Legal API Endpoint | yes | output of infrastructure deployment |
| `DATA_PARTITION_ID` | ex `opendes` | Data partition against which integration tests will be triggered | no | output of infrastructure deployment |
| `INTEGRATION_TESTER` | `********` | Identity to run the integration tests | yes | keyvault secret: `$KEYVAULT_URI/secrets/app-dev-sp-username` |
| `NO_DATA_ACCESS_TESTER` | `********` | Service principal ID of a service principal without entitlements | yes | `aad-no-data-access-tester-client-id` secret from keyvault |
| `NO_DATA_ACCESS_TESTER_SERVICEPRINCIPAL_SECRET` | `********` | Secret for `$NO_DATA_ACCESS_TESTER` | yes | `aad-no-data-access-tester-secret` secret from keyvault |
| `AZURE_AD_APP_RESOURCE_ID` | `********` | AAD client application ID | yes | output of infrastructure deployment |
| `AZURE_AD_TENANT_ID` | `********` | AD tenant to authenticate users from | yes | -- |
| `DEPLOY_ENV` | `empty` | Required but not used | no | - |
| `DOMAIN` | `contoso.com` | OSDU R2 to run tests under | no | - |
| `INTEGRATION_TESTER` | `********` | System identity to assume for API calls. Note: this user must have entitlements configured already | no | -- |
| `TESTER_SERVICEPRINCIPAL_SECRET` | `********` | Secret for `$INTEGRATION_TESTER` | yes | -- |
| `TENANT_NAME` | ex `opendes` | OSDU tenant used for testing | no | -- |

### Configure Maven

Check that maven is installed:
```bash
$ mvn --version
Apache Maven 3.6.0
Maven home: /usr/share/maven
Java version: 1.8.0_212, vendor: AdoptOpenJDK, runtime: /usr/lib/jvm/jdk8u212-b04/jre
...
```

## License
Copyright © Microsoft Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


