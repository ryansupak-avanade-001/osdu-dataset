package org.opengroup.osdu.datasetregistry.model;

import java.util.List;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;

import org.opengroup.osdu.core.common.model.storage.Record;
import org.opengroup.osdu.core.common.model.storage.validation.ValidNotNullCollection;

public class CreateDatasetRegistryRequest {
    
    @ValidNotNullCollection
	@NotEmpty(message = DatasetRegistryValidationDoc.MISSING_DATASET_REGISTRIES_ARRAY)
	@Size(min = 1, max = 20, message = DatasetRegistryValidationDoc.MAX_DATASET_REGISTRIES_EXCEEDED) //TODO: need to support pagination of storage record get and then extend this back to 500
    public List<Record> datasetRegistries;

}
