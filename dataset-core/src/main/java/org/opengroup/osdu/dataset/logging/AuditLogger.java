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

import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.logging.audit.AuditPayload;
import org.opengroup.osdu.core.common.logging.audit.AuditStatus;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

@Component
@RequestScope
@RequiredArgsConstructor
public class AuditLogger {

  private final JaxRsDpsLog logger;
  private final DpsHeaders dpsHeaders;
  private AuditEvents auditEvents;

  private AuditEvents getAuditEvents() {
    if (Objects.isNull(this.auditEvents)) {
      this.auditEvents = new AuditEvents(this.dpsHeaders.getUserEmail());
    }
    return this.auditEvents;
  }

  public void readStorageInstructionsSuccess(List<String> resources) {
    writeLog(getAuditEvents().getReadStorageInstructionsEvent(AuditStatus.SUCCESS, resources));
  }

  public void readStorageInstructionsFailure(List<String> resources) {
    writeLog(getAuditEvents().getReadStorageInstructionsEvent(AuditStatus.FAILURE, resources));
  }

  public void readRetrievalInstructionsSuccess(List<String> resources) {
    writeLog(getAuditEvents().getReadRetrievalInstructionsEvent(AuditStatus.SUCCESS, resources));
  }

  public void readRetrievalInstructionsFailure(List<String> resources) {
    writeLog(getAuditEvents().getReadRetrievalInstructionsEvent(AuditStatus.FAILURE, resources));
  }

  public void registerDatasetSuccess(List<String> resources) {
    writeLog(getAuditEvents().getRegisterDatasetEvent(AuditStatus.SUCCESS, resources));
  }

  public void registerDatasetFailure(List<String> resources) {
    writeLog(getAuditEvents().getRegisterDatasetEvent(AuditStatus.FAILURE, resources));
  }

  public void readDatasetRegistriesSuccess(List<String> resources) {
    writeLog(getAuditEvents().getReadDatasetRegistriesEvent(AuditStatus.SUCCESS, resources));
  }

  public void readDatasetRegistriesFailure(List<String> resources) {
    writeLog(getAuditEvents().getReadDatasetRegistriesEvent(AuditStatus.FAILURE, resources));
  }

  private void writeLog(AuditPayload log) {
    this.logger.audit(log);
  }
}