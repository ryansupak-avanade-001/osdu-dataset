package org.opengroup.osdu.dataset.provider.aws.model;

import java.util.ArrayList;
import java.util.HashMap;

import org.opengroup.osdu.dataset.dms.DmsServiceProperties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DmsRegistrations {

    HashMap<String, DmsServiceProperties> dynamoDmsRegistrations = new HashMap<>();
    
}
