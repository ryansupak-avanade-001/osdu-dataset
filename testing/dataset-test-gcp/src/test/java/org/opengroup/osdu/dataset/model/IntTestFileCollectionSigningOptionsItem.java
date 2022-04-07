package org.opengroup.osdu.dataset.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IntTestFileCollectionSigningOptionsItem {

  @JsonProperty("bucket")
  private String bucket;

  @JsonProperty("connectionString")
  private String connectionString;

  @JsonProperty("filepath")
  private String filepath;

}
