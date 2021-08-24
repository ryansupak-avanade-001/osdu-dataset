package org.opengroup.osdu.dataset.di;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "osdu.dataset.config")
@Data
public class DatasetConfig {
    private boolean useRestDms = false;
}
