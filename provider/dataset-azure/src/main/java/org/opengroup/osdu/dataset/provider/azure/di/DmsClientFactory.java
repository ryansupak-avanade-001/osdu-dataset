package org.opengroup.osdu.dataset.provider.azure.di;

import lombok.RequiredArgsConstructor;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.dataset.dms.IDmsFactory;
import org.opengroup.osdu.dataset.provider.azure.dms.AzureDmsFactory;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Primary
@Component(value = "azureDmsClientFactory")
@RequiredArgsConstructor
public class DmsClientFactory extends AbstractFactoryBean<IDmsFactory> {

    private final IHttpClient client;

    @Override
    public Class<?> getObjectType() {
        return IDmsFactory.class;
    }

    @Override
    protected IDmsFactory createInstance() throws Exception {
        return new AzureDmsFactory(client);
    }
}
