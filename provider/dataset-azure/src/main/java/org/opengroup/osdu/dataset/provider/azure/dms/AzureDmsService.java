package org.opengroup.osdu.dataset.provider.azure.dms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URIBuilder;
import org.opengroup.osdu.core.common.http.HttpRequest;
import org.opengroup.osdu.core.common.http.HttpResponse;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.dataset.dms.DmsServiceProperties;
import org.opengroup.osdu.dataset.dms.IDmsProvider;
import org.opengroup.osdu.dataset.model.request.GetDatasetRegistryRequest;
import org.opengroup.osdu.dataset.model.response.DatasetRetrievalDeliveryItem;
import org.opengroup.osdu.dataset.model.response.GetDatasetRetrievalInstructionsResponse;
import org.opengroup.osdu.dataset.model.response.GetDatasetStorageInstructionsResponse;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class AzureDmsService implements IDmsProvider {

    private final DmsServiceProperties dmsServiceProperties;

    private final IHttpClient httpClient;
    private final DpsHeaders headers;

    private static final String AZURE_PROVIDER_KEY = "AZURE";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Map<String, Object> FILE_LOCATION_REQUEST = new HashMap<>();

    @Override
    public GetDatasetStorageInstructionsResponse getStorageInstructions() {
        String url = this.createUrl("/getLocation");

        HttpResponse result = this.httpClient
                .send(HttpRequest.post(FILE_LOCATION_REQUEST).url(url).headers(this.headers.getHeaders()).build());

        Map<String, Object> storageLocation = null;
        try {
            storageLocation = OBJECT_MAPPER.readValue(result.getBody(), new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new AppException(500, "Internal Server Error", e.getMessage(), e);
        }

        return new GetDatasetStorageInstructionsResponse(storageLocation, AZURE_PROVIDER_KEY);
    }

    @Override
    public GetDatasetRetrievalInstructionsResponse getDatasetRetrievalInstructions(GetDatasetRegistryRequest request) {
        List<DatasetRetrievalDeliveryItem> getRetrievalInstructionsResponse = new ArrayList<>(request.datasetRegistryIds.size());

        for (String datasetId: request.datasetRegistryIds) {
            String url = this.createUrl(String.format("/files/%s/downloadURL", datasetId));

            HttpResponse result = this.httpClient
                    .send(HttpRequest.get().url(url).headers(this.headers.getHeaders()).build());

            Map<String, Object> retrievalProperties = null;
            try {
                retrievalProperties = OBJECT_MAPPER.readValue(result.getBody(), new TypeReference<Map<String, Object>>() {});
            } catch (JsonProcessingException e) {
                throw new AppException(500, "Internal Server Error", e.getMessage(), e);
            }
            DatasetRetrievalDeliveryItem retrievalInstruction = new DatasetRetrievalDeliveryItem(datasetId,
                    retrievalProperties, AZURE_PROVIDER_KEY);

            getRetrievalInstructionsResponse.add(retrievalInstruction);
        }

        return new GetDatasetRetrievalInstructionsResponse(getRetrievalInstructionsResponse);
    }

    private String createUrl(String path) {
        try {
            URIBuilder  uriBuilder = new URIBuilder(dmsServiceProperties.getDmsServiceBaseUrl());
            uriBuilder.setPath(uriBuilder.getPath() + path);
            return uriBuilder.build().normalize().toString();
        } catch (URISyntaxException e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Invalid URL", e.getMessage(), e);
        }
    }
}
