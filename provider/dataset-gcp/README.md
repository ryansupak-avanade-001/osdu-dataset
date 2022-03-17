# Dataset registry service

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* GCloud SDK with java (latest version)
* JDK 8
* Lombok 1.16 or later
* Maven

# Features of implementation
This is a universal solution created using EPAM OSM and OBM mappers technology.
It allows you to work with various implementations of KV stores and Blob stores.

## Limitations of the current version

In the current version, the mappers are equipped with several drivers to the stores:

- OSM (mapper for KV-data): Google Datastore; Postgres
- OBM (mapper to Blob stores): Google Cloud Storage (GCS); MinIO

## Extensibility

To use any other store or message broker, implement a driver for it. With an extensible set of drivers, the solution is unrestrictedly universal and portable without modification to the main code.

Mappers support "multitenancy" with flexibility in how it is implemented.
They switch between datasources of different tenants due to the work of a bunch of classes that implement the following interfaces:

- Destination - takes a description of the current context, e.g., "data-partition-id = opendes"
- DestinationResolver – accepts Destination, finds the resource, connects, and returns Resolution
- DestinationResolution – contains a ready-made connection, the mapper uses it to get to data

## Mapper tuning mechanisms

This service uses specific implementations of DestinationResolvers based on the tenant information provided by the OSDU Partition service.
- for Google Datastore: osm/config/DsTenantDestinationResolver.java
- for Postgres: osm/config/PgTenantDestinationResolver.java

#### Their algorithms are as follows:
- incoming Destination carries data-partition-id
- resolver accesses the Partition service and gets PartitionInfo
- from PartitionInfo resolver retrieves properties for the connection: URL, username, password etc.
- resolver creates a data source, connects to the resource, remembers the datasource
- resolver gives the datasource to the mapper in the Resolution object
- Google Cloud resolvers do not receive special properties from the Partition service for connection, 
because the location of the resources is unambiguously known - they are in the GCP project. 
And credentials are also not needed - access to data is made on behalf of the Google Identity SA
under which the service itself is launched. Therefore, resolver takes only 
the value of the **projectId** property from PartitionInfo and uses it to connect to a resource 
in the corresponding GCP project.

# Configuration

## Service Configuration

Define the following environment variables.
Most of them are common to all hosting environments, but there are properties that are only necessary when running in Google Cloud.

### Anthos Service Configuration:
[Anthos service configuration ](docs/anthos/README.md)
### GCP Service Configuration:
[Gcp service configuration ](docs/gcp/README.md)

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

* Navigate to Dataset service root folder and run:

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
 
 | name | value | description | sensitive? | source |
 | ---  | ---   | ---         | ---        | ---    |
 | `DOMAIN` | ex `osdu-gcp.go3-nrg.projects.epam.com` | - | no | - |
 | `STORAGE_BASE_URL` | ex `https://os-storage-jvmvia5dea-uc.a.run.app/api/storage/v2/` | Storage API endpoint | no | output of infrastructure deployment |
 | `LEGAL_BASE_URL` | ex `https://os-legal-jvmvia5dea-uc.a.run.app/api/legal/v1/` | Legal API endpoint | no | output of infrastructure deployment |
 | `DATASET_BASE_URL` | ex `http://localhost:8080/api/dataset/v1/` | Dataset API endpoint | no | output of infrastructure deployment |
 | `SCHEMA_API` | ex `https://os-schema-jvmvia5dea-uc.a.run.app/api/schema-service/v1` | Schema API endpoint | no | output of infrastructure deployment |
 | `PROVIDER_KEY` | `GCP` | required for response verification | no | - |
 | `INTEGRATION_TEST_AUDIENCE` | ex `****.apps.googleusercontent.com;` | Client application ID | yes | https://console.cloud.google.com/apis/credentials |
 | `INTEGRATION_TESTER` | `********` | Service account for API calls, passed as a filename or JSON content, plain or Base64 encoded.  Note: this user must have entitlements configured already | yes | https://console.cloud.google.com/iam-admin/serviceaccounts |
 | `GCP_DEPLOY_FILE` | `********` | Service account for test data tear down, passed as a filename or JSON content, plain or Base64 encoded. Must have cloud storage role configured | yes | https://console.cloud.google.com/iam-admin/serviceaccounts |
 | `TENANT_NAME` | `opendes` | Tenant name | no | - |
 | `KIND_SUBTYPE` | `DatasetTest` | Kind subtype that will be used in int tests, schema creation automated , result kind will be `TENANT_NAME::wks-test:dataset--FileCollection.KIND_SUBTYPE:1.0.0`| no | - |
 | `LEGAL_TAG` | `public-usa-dataset-1` | Legal tag name, if tag with that name doesn't exist then it will be created during preparing step | no | - |
 | `GCLOUD_PROJECT` | `osdu-cicd-epam` | Project id | no | - |
 | `GCP_STORAGE_PERSISTENT_AREA` | ex `persistent-area` | persistent area bucket(will be concatenated with project id ex `osdu-cicd-epam-persistent-area` | no | output of infrastructure deployment |
 | `LEGAL_HOST` | ex `https://os-legal-jvmvia5dea-uc.a.run.app/api/legal/v1/` | Legal API endpoint | no | output of infrastructure deployment |

 **Entitlements configuration for integration accounts**
 
 | INTEGRATION_TESTER | 
 | ---  | 
 | users<br/>service.entitlements.user<br/>service.storage.admin<br/>service.legal.user<br/>service.search.user<br/>service.delivery.viewer<br/>service.dataset.viewers<br/>service.dataset.editors | 
 
 **Cloud roles configuration for integration accounts**
 
 | GCP_DEPLOY_FILE|
 | ---  |
 | storage.admin access to the Google Cloud Storage |
 
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