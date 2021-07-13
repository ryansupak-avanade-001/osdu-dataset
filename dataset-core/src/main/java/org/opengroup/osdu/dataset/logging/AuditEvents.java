/*
 * Copyright 2021 Google LLC
 * Copyright 2021 EPAM Systems, Inc
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

package org.opengroup.osdu.dataset.logging;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import org.opengroup.osdu.core.common.logging.audit.AuditAction;
import org.opengroup.osdu.core.common.logging.audit.AuditPayload;
import org.opengroup.osdu.core.common.logging.audit.AuditStatus;

public class AuditEvents {

  private static final String READ_STORAGE_INSTRUCTIONS_ACTION_ID = "DS001";
  private static final String READ_STORAGE_INSTRUCTIONS_MESSAGE = "Read storage instructions";

  private static final String READ_RETRIEVAL_INSTRUCTIONS_ACTION_ID = "DS002";
  private static final String READ_RETRIEVAL_INSTRUCTIONS_MESSAGE = "Read retrieval instructions";

  private static final String REGISTER_DATASET_ACTION_ID = "DS003";
  private static final String REGISTER_DATASET_MESSAGE = "Registered dataset";

  private static final String READ_DATASET_REGISTRIES_ACTION_ID = "DS004";
  private static final String READ_DATASET_REGISTRIES_MESSAGE = "Read dataset registries";

  private final String user;

  public AuditEvents(String user) {
    if (Strings.isNullOrEmpty(user)) {
      throw new IllegalArgumentException("User not provided for audit events.");
    }
    this.user = user;
  }

  public AuditPayload getReadStorageInstructionsEvent(AuditStatus status, List<String> resources) {
    //not including resources to avoid logging sensitive access information
    return AuditPayload.builder()
        .action(AuditAction.READ)
        .status(status)
        .user(this.user)
        .actionId(READ_STORAGE_INSTRUCTIONS_ACTION_ID)
        .message(getStatusMessage(status, READ_STORAGE_INSTRUCTIONS_MESSAGE))
        .resources(new ArrayList<>())
        .build();
  }

  public AuditPayload getReadRetrievalInstructionsEvent(AuditStatus status,
      List<String> resources) {
    //not including resources to avoid logging sensitive access information
    return AuditPayload.builder()
        .action(AuditAction.READ)
        .status(status)
        .user(this.user)
        .actionId(READ_RETRIEVAL_INSTRUCTIONS_ACTION_ID)
        .message(getStatusMessage(status, READ_RETRIEVAL_INSTRUCTIONS_MESSAGE))
        .resources(new ArrayList<>())
        .build();
  }

  public AuditPayload getRegisterDatasetEvent(AuditStatus status, List<String> resources) {
    return AuditPayload.builder()
        .action(AuditAction.CREATE)
        .status(status)
        .user(this.user)
        .actionId(REGISTER_DATASET_ACTION_ID)
        .message(getStatusMessage(status, REGISTER_DATASET_MESSAGE))
        .resources(resources)
        .build();
  }

  public AuditPayload getReadDatasetRegistriesEvent(AuditStatus status, List<String> resources) {
    return AuditPayload.builder()
        .action(AuditAction.READ)
        .status(status)
        .user(this.user)
        .actionId(READ_DATASET_REGISTRIES_ACTION_ID)
        .message(getStatusMessage(status, READ_DATASET_REGISTRIES_MESSAGE))
        .resources(resources)
        .build();
  }

  private String getStatusMessage(AuditStatus status, String message) {
    return String.format("%s - %s", message, status.name().toLowerCase());
  }
}