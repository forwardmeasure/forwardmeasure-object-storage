package com.forwardmeasure.objectstorage.core;

import java.util.Map;
import java.util.Optional;

public interface NamedStorageClientConfig {

    String backend();

    Optional<String> bucket();

    Optional<String> endpoint();

    /**
     * Browser-visible endpoint used when generating delegated capabilities.
     * This may differ from {@link #endpoint()} when applications access object
     * storage through an in-cluster service name but return capabilities to an
     * external caller.
     */
    Optional<String> publicEndpoint();

    Optional<String> region();

    Optional<String> accessKey();

    Optional<String> secretKey();

    Optional<Boolean> pathStyleAccess();

    Map<String, String> properties();
}
