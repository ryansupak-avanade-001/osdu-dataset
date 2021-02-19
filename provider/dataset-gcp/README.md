# Dataset registry service

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites
Pre-requisites

* GCloud SDK with java (latest version)
* JDK 8
* Lombok 1.16 or later
* Maven

### Installation
In order to run the service locally or remotely, you will need to have the following environment variables defined.

| name | value | description | sensitive? | source |
| ---  | ---   | ---         | ---        | ---    |
| `GCP_SCHEMA_API` | ex `https://os-schema-jvmvia5dea-uc.a.run.app/api/schema-service/v1` | Schema API endpoint | no | output of infrastructure deployment |
| `GCP_STORAGE_API` | ex `https://os-storage-jvmvia5dea-uc.a.run.app/api/storage/v2` | Storage API endpoint | no | output of infrastructure deployment |
| `AUTHORIZE_API` | ex `https://os-entitlements-gcp-jvmvia5dea-uc.a.run.app/entitlements/v1` | Entitlements API endpoint | no | output of infrastructure deployment |
| `GCP_FILE_DMS_BUCKET` | ex `file-dms-bucket` | File bucket name postfix (full name represent by project-id + partition-id + GCP_FILE_DMS_BUCKET ex `osdu-cicd-epam-opendes-file-dms-bucket`) | no | output of infrastructure deployment |
| `GCP_EXPIRATION_DAYS` | ex `1` | expiration for signed urls & connection strings | no |  |
| `REDIS_GROUP_HOST` |  ex `127.0.0.1` | Redis host for groups | no | https://console.cloud.google.com/memorystore/redis/instances |
| `REDIS_STORAGE_HOST` |  ex `127.0.0.1` | Redis host for storage | no | https://console.cloud.google.com/memorystore/redis/instances |

### Run Locally
Check that maven is installed:

```bash
$ mvn --version
Apache Maven 3.6.0
Maven home: /usr/share/maven
Java version: 1.8.0_212, vendor: AdoptOpenJDK, runtime: /usr/lib/jvm/jdk8u212-b04/jre
...
```

You may need to configure access to the remote maven repository that holds the OSDU dependencies. This file should live within `~/.mvn/community-maven.settings.xml`:

```bash
$ cat ~/.m2/settings.xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
    <servers>
        <server>
            <id>community-maven-via-private-token</id>
            <!-- Treat this auth token like a password. Do not share it with anyone, including Microsoft support. -->
            <!-- The generated token expires on or before 11/14/2019 -->
             <configuration>
              <httpHeaders>
                  <property>
                      <name>Private-Token</name>
                      <value>${env.COMMUNITY_MAVEN_TOKEN}</value>
                  </property>
              </httpHeaders>
             </configuration>
        </server>
    </servers>
</settings>
```
* Update the Google cloud SDK to the latest version:

```bash
gcloud components update
```
* Set Google Project Id:

```bash
gcloud config set project <YOUR-PROJECT-ID>
```

* Perform a basic authentication in the selected project:

```bash
gcloud auth application-default login
```

* Navigate to Dataset service's root folder and run:

```bash
mvn clean install   
```

* If you wish to build the project without running tests

```bash
mvn clean install -DskipTests
```

After configuring your environment as specified above, you can follow these steps to build and run the application. These steps should be invoked from the *repository root.*

```bash
cd provider/dataset-gcp && mvn spring-boot:run
```
## Testing
 
 ### Running E2E Tests 
 This section describes how to run cloud OSDU E2E tests (testing/dataset-test-gcp).
 
 You will need to have the following environment variables defined.
 
 **Entitlements configuration for integration accounts**
 
 Execute following command to build code and run all the integration tests:
 
 ```bash
 # Note: this assumes that the environment variables for integration tests as outlined
 #       above are already exported in your environment.
 # build + install integration test core
 $ (cd testing/dataset-test-core/ && mvn clean install)
 ```
 ```bash
 # build + run GCP integration tests.
 $ (cd testing/dataset-test-gcp/ && mvn clean test)
 ```

## Deployment

* To deploy into Cloud run, please, use this documentation:
https://cloud.google.com/run/docs/quickstarts/build-and-deploy

* To deploy into App Engine, please, use this documentation:
https://cloud.google.com/appengine/docs/flexible/java/quickstart

## License

Copyright 2021 Google LLC

Copyright 2021 EPAM Systems, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.