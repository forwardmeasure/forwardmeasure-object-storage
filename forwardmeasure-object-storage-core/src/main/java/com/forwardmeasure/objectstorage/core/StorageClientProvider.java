package com.forwardmeasure.objectstorage.core;

import com.forwardmeasure.objectstorage.StorageClient;

public interface StorageClientProvider {

  /** Stable backend identifier used by named client configuration. */
  String backend();

  StorageClient create(String clientName, NamedStorageClientConfig config);
}
