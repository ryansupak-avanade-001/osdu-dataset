# Dataset Service

dataset-aws is a [Spring Boot](https://spring.io/projects/spring-boot) service that provides a set of APIs to allow an application or an user to fetch storage/retrieval instructions for
various types of datasets.

## Running Locally

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Pre-requisites

* JDK 8 (https://docs.aws.amazon.com/corretto/latest/corretto-8-ug/downloads-list.html)
* Maven 3.8.3 or later
* Lombok 1.16 or later
* OSDU Instance deployed on AWS

### Service Configuration

In order to run the service locally or remotely, you will need to have the following environment variables defined.

| name                     | example value                                        | required | description                                                                                   | sensitive? |
|--------------------------|------------------------------------------------------|----------|-----------------------------------------------------------------------------------------------|------------|
| `LOCAL_MODE`             | `true`                                               | yes      | Set to 'true' to use env vars in place of the k8s variable resolver                           | no         |
| `APPLICATION_PORT`       | `8080`                                               | yes      | The port the service will be hosted on.                                                       | no         |
| `AWS_REGION`             | `us-east-1`                                          | yes      | The region where resources needed by the service are deployed                                 | no         |
| `AWS_ACCESS_KEY_ID`      | `ASIAXXXXXXXXXXXXXX`                                 | yes      | The AWS Access Key for a user with access to Backend Resources required by the service        | yes        |
| `AWS_SECRET_ACCESS_KEY`  | `super-secret-key==`                                 | yes      | The AWS Secret Key for a user with access to Backend Resources required by the service        | yes        |
| `AWS_SESSION_TOKEN`      | `session-token-xxxxxxxxxx`                           | no       | AWS Session token needed if using an SSO user session to authenticate                         | yes        |
| `ENVIRONMENT`            | `osdu-prefix`                                        | yes      | The Resource Prefix defined during deployment                                                 | no         |
| `LOG_LEVEL`              | `DEBUG`                                              | yes      | The Log Level severity to use (https://www.tutorialspoint.com/log4j/log4j_logging_levels.htm) | no         |
| `SSL_ENABLED`            | `false`                                              | no       | Set to 'false' to disable SSL for local development                                           | no         |
| `ENTITLEMENTS_BASE_URL`  | `http://localhost:8081` or `https://some-hosted-url` | yes      | Specify the base url for an entitlements service instance. Can be run locally or remotely     | no         |
| `SCHEMA_BASE_URL`        | `http://localhost:8082` or `https://some-hosted-url` | yes      | Specify the base url for a schema service instance. Can be run locally or remotely            | no         | 
| `STORAGE_BASE_URL`       | `http://localhost:8083` or `https://some-hosted-url` | yes      | Specify the base url for a storage service instance. Can be run locally or remotely           | no         |
| `DISABLE_CACHE`          | `true`                                               | no       | Set to true to disable caching to redis. Either set this or configure cache config env vars   | no         | 
| `CACHE_CLUSTER_ENDPOINT` | `127.0.0.1`                                          | no       | Redis endpoint uri. Either set this or DISABLE_CACHE                                          | no         | 
| `CACHE_CLUSTER_PORT`     | `6379`                                               | no       | Redis port. Either set this or DISABLE_CACHE                                                  | no         | 
| `CACHE_CLUSTER_KEY`      | `xxxxxx`                                             | no       | Redis auth key. Either set this or DISABLE_CACHE                                              | no         | 

### Run Locally

Check that maven is installed:

example:

```bash
$ mvn --version
Apache Maven 3.8.3 (ff8e977a158738155dc465c6a97ffaf31982d739)
Maven home: /usr/local/Cellar/maven/3.8.3/libexec
Java version: 1.8.0_312, vendor: Amazon.com Inc., runtime: /Library/Java/JavaVirtualMachines/amazon-corretto-8.jdk/Contents/Home/jre
...
```

You may need to configure access to the remote maven repository that holds the OSDU dependencies. Copy one of the below files' content to your .m2 folder

* For development against the OSDU GitLab environment, leverage: `<REPO_ROOT>~/.mvn/community-maven.settings.xml`
* For development in an AWS Environment, leverage: `<REPO_ROOT>/provider/dataset-aws/maven/settings.xml`

* Navigate to the service's root folder and run:

```bash
mvn clean package -pl dataset-core,provider/dataset-aws
```

* If you wish to build the project without running tests

```bash
mvn clean package -pl dataset-core,provider/dataset-aws -DskipTests
```

After configuring your environment as specified above, you can follow these steps to run the application. These steps should be invoked from the *repository root.*
<br/>
<br/>
NOTE: If not on osx/linux: Replace `*` with version numbers as defined in the provider/dataset-aws/pom.xml file

```bash
java -jar provider/dataset-aws/target/dataset-aws-*.*.*-SNAPSHOT-spring-boot.jar
```

## Testing

### Running Integration Tests

This section describes how to run OSDU Integration tests (testing/dataset-test-aws).

You will need to have the following environment variables defined.

| name                                     | example value                                                                        | description                                                                            | sensitive?                                                                      |
|------------------------------------------|--------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `AWS_ACCESS_KEY_ID`                      | `ASIAXXXXXXXXXXXXXX`                                                                 | The AWS Access Key for a user with access to Backend Resources required by the service | yes                                                                             |
| `AWS_SECRET_ACCESS_KEY`                  | `super-secret-key==`                                                                 | The AWS Secret Key for a user with access to Backend Resources required by the service | yes                                                                             |
| `AWS_SESSION_TOKEN`                      | `session-token-xxxxxxxxx`                                                            | AWS Session token needed if using an SSO user session to authenticate                  | yes                                                                             |
| `AWS_COGNITO_USER_POOL_ID`               | `us-east-1_xxxxxxxx`                                                                 | User Pool Id for the reference cognito                                                 | no                                                                              |
| `AWS_COGNITO_CLIENT_ID`                  | `xxxxxxxxxxxx`                                                                       | Client ID for the Auth Flow integrated with the Cognito User Pool                      | no                                                                              |
| `AWS_COGNITO_AUTH_FLOW`                  | `USER_PASSWORD_AUTH`                                                                 | Auth flow used by reference cognito deployment                                         | no                                                                              |
| `AWS_COGNITO_AUTH_PARAMS_USER`           | `int-test-user@testing.com`                                                          | Int Test Username                                                                      | no                                                                              |
| `AWS_COGNITO_AUTH_PARAMS_USER_NO_ACCESS` | `no-access-user@testing.com`                                                         | Int Test No Access Username                                                            | no                                                                              |
| `AWS_COGNITO_AUTH_PARAMS_PASSWORD`       | `some-secure-password`                                                               | Int Test User/NoAccessUser Password                                                    | yes                                                                             |
| `DATASET_URL`                            | `http://localhost:8080/api/dataset/v1/`                                              | The url where the Dataset API is hosted                                                | no                                                                              |
| `STORAGE_URL`                            | `http://localhost:8081/api/storage/v2/` or `https://some-hosted-url/api/storage/v2/` | The url where the Storage API is hosted. Can be run locally or remotely                | no                                                                              |
| `LEGAL_URL`                              | `http://localhost:8082/api/legal/v1/` or `https://some-hosted-url/api/legal/v1/`     | The url where the Legal API is hosted. Can be run locally or remotely                  | no                                                                              |
| `DMS_BASE_URL`                           | `http://localhost:8083` or `https://some-hosted-url`                                 | yes                                                                                    | Specify the base url for a DMS service instance. Can be run locally or remotely | no |
| `DOMAIN`                                 | `example.com`                                                                        | Domain used by ACL                                                                     | no                                                                              |
| `DEPLOY_ENV`                             | `empty`                                                                              | Unused env var, set to 'empty'.                                                        | no                                                                              |
| `TENANT_NAME`                            | `int-test-dataset`                                                                   | Data Partition Id used by int tests                                                    | no                                                                              |
| `SSM_ENABLED`                            | `true`                                                                               | yes                                                                                    | Set to 'true' to use SSM to resolve config properties, otherwise use env vars   | no |

**Creating a new user to use for integration tests**

 ```bash
 aws cognito-idp admin-create-user --user-pool-id ${AWS_COGNITO_USER_POOL_ID} --username ${AWS_COGNITO_AUTH_PARAMS_USER} --user-attributes Name=email,Value=${AWS_COGNITO_AUTH_PARAMS_USER} Name=email_verified,Value=True --message-action SUPPRESS
 aws cognito-idp initiate-auth --auth-flow ${AWS_COGNITO_AUTH_FLOW} --client-id ${AWS_COGNITO_CLIENT_ID} --auth-parameters USERNAME=${AWS_COGNITO_AUTH_PARAMS_USER},PASSWORD=${AWS_COGNITO_AUTH_PARAMS_PASSWORD}
 ```

**Entitlements group configuration for integration accounts**
<br/>
In order to add user entitlements, run entitlements bootstrap scripts in the entitlements project

| AWS_COGNITO_AUTH_PARAMS_USER | AWS_COGNITO_AUTH_PARAMS_USER_NO_ACCESS | 
|------------------------------|----------------------------------------|
| service.entitlements.user    | service.entitlements.user              |
| service.dataset.admin        | service.dataset.admin                  |
| service.dataset.editor       |                                        |
| service.dataset.viewer       |                                        |
| service.storage.admin        |                                        |
| service.storage.creater      |                                        |
| service.storage.viewer       |                                        |
| service.legal.user           |                                        |
| service.delivery.viewer      |                                        |

Execute following command to build code and run all the integration tests:

### Run Tests Simulating Pipeline

* Prior to running tests, scripts must be executed locally to generate pipeline env vars

```bash
testing/dataset-test-aws/build-aws/prepare-dist.sh

#Set Neccessary ENV Vars here as defined in run-tests.sh

testing/dataset-test-aws/build-aws/run-tests.sh```
```

### Run Tests Using mvn

Set required env vars and execute the following:

```bash
mvn clean package -f testing/pom.xml -pl dataset-test-core,dataset-test-aws -DskipTests
mvn test -f testing/dataset-test-aws/pom.xml
```

## License

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
