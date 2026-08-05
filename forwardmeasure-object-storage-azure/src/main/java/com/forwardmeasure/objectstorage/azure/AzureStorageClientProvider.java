package com.forwardmeasure.objectstorage.azure;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientProvider;

public class AzureStorageClientProvider implements StorageClientProvider {

    @Override
    public String backend() {
        return "azure";
    }

    @Override
    public StorageClient create(String clientName, NamedStorageClientConfig config) {
        return new AzureStorageClient(config);
    }
}
