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

//TODO: move to os-core-common

package org.opengroup.osdu.dataset.dms;

import org.opengroup.osdu.core.common.dms.model.CopyDmsRequest;
import org.opengroup.osdu.core.common.dms.model.CopyDmsResponse;
import org.opengroup.osdu.core.common.dms.model.RetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.response.GetDatasetStorageInstructionsResponse;

import java.util.List;

public interface IDmsProvider {

    GetDatasetStorageInstructionsResponse getStorageInstructions() throws DmsException;

    GetDatasetRetrievalInstructionsResponse getDatasetRetrievalInstructions(GetDatasetRegistryRequest request) throws DmsException;

    // new retrieval
    default RetrievalInstructionsResponse getRetrievalInstructions(GetDatasetRegistryRequest request)
            throws DmsException {

        return null;
    }

    // copy dms
    default List<CopyDmsResponse> copyDmsToPersistentStorage(CopyDmsRequest copyDmsRequest) throws DmsException {
        return null;
    }
}