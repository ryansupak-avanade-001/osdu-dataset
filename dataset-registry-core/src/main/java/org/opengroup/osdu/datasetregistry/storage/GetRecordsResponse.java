package org.opengroup.osdu.datasetregistry.storage;

import java.util.List;

import org.opengroup.osdu.core.common.model.storage.Record;

import lombok.Data;

@Data
public class GetRecordsResponse {
    private List<Record> records;
    private List<Record> invalidRecords;
    private List<Record> retryRecords;
}
