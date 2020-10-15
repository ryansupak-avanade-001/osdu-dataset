package org.opengroup.osdu.datasetregistry.model;

public class DatasetRegistryValidationDoc {
    
  private DatasetRegistryValidationDoc() {
    // private constructor
  }

  public static final String MISSING_DATASET_PROPERTIES_VALIDATION = "DatasetProperties cannot be null";
  public static final String MISSING_DATASET_REGISTRIES_ARRAY = "datasetRegistries cannot be empty";
  public static final String MAX_DATASET_REGISTRIES_EXCEEDED = "Only 20 Dataset Registries can be ingested at a time";
  public static final String MISSING_DATASET_REGISTRY_SCHEMA_ERROR_FORMAT = "No schema for Dataset Registry was found: Expecting '%s'. It must be registered first.";
  public static final String DATASET_REGISTRY_MISSING_PROPERTY_VALIDATION_FORMAT = "Dataset Registry Schema Validation Failed: Expected property '%s' is missing";
    

}
