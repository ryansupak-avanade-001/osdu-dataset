package org.opengroup.osdu.dataset.provider.azure.dms;

import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.dms.IDmsFactory;
import org.opengroup.osdu.dataset.dms.IDmsProvider;

@RequiredArgsConstructor
public class AzureDmsFactory implements IDmsFactory {

    private final IHttpClient client;

    @Override
    public IDmsProvider create(DpsHeaders headers, DmsServiceProperties dmsServiceRoute) {
        return new AzureDmsService(dmsServiceRoute, client, headers);
    }
}
