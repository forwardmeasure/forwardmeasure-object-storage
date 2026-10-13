/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.objectstorage.core;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A plain {@link NamedStorageClientConfig} value, so code and tests that know their settings up
 * front don't each hand-write an implementation.
 */
public record StorageClientSettings(
    String backend,
    Optional<String> bucket,
    Optional<String> endpoint,
    Optional<String> publicEndpoint,
    Optional<String> region,
    Optional<String> accessKey,
    Optional<String> secretKey,
    Optional<Boolean> pathStyleAccess,
    Map<String, String> properties)
    implements NamedStorageClientConfig {

  public StorageClientSettings {
    Objects.requireNonNull(backend, "backend");
    bucket = Objects.requireNonNullElse(bucket, Optional.empty());
    endpoint = Objects.requireNonNullElse(endpoint, Optional.empty());
    publicEndpoint = Objects.requireNonNullElse(publicEndpoint, Optional.empty());
    region = Objects.requireNonNullElse(region, Optional.empty());
    accessKey = Objects.requireNonNullElse(accessKey, Optional.empty());
    secretKey = Objects.requireNonNullElse(secretKey, Optional.empty());
    pathStyleAccess = Objects.requireNonNullElse(pathStyleAccess, Optional.empty());
    properties = Map.copyOf(Objects.requireNonNullElse(properties, Map.of()));
  }

  /** An S3-compatible endpoint (MinIO and the like): path-style access, static credentials. */
  public static StorageClientSettings s3Compatible(
      String endpoint, String region, String accessKey, String secretKey) {
    return new StorageClientSettings(
        "s3",
        Optional.empty(),
        Optional.of(endpoint),
        Optional.empty(),
        Optional.of(region),
        Optional.of(accessKey),
        Optional.of(secretKey),
        Optional.of(true),
        Map.of());
  }
}
