package com.forwardmeasure.objectstorage.gcs;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientProvider;

public class GcsStorageClientProvider implements StorageClientProvider {

    @Override
    public String backend() {
        return "gcs";
    }

    @Override
    public StorageClient create(String clientName, NamedStorageClientConfig config) {
        return new GcsStorageClient(config);
    }
}
