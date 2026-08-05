package com.forwardmeasure.objectstorage.s3;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientProvider;

public class S3StorageClientProvider implements StorageClientProvider {

    @Override
    public String backend() {
        return "s3";
    }

    @Override
    public StorageClient create(String clientName, NamedStorageClientConfig config) {
        return new S3StorageClient(config);
    }
}
