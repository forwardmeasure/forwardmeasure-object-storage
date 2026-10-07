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

import com.forwardmeasure.objectstorage.ObjectMetadata;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.StorageObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory provider boundary for registry/resource contracts; SDK integration tests use real
 * servers.
 */
final class StorageTestProvider implements StorageClientProvider {
  final AtomicInteger created = new AtomicInteger();
  final AtomicInteger closedClients = new AtomicInteger();
  final AtomicInteger closedObjects = new AtomicInteger();
  StorageClient.GetObjectRequest lastRequest;
  boolean readFailure;
  boolean closeFailure;

  public String backend() {
    return "memory";
  }

  public StorageClient create(String clientName, NamedStorageClientConfig config) {
    created.incrementAndGet();
    return (StorageClient)
        Proxy.newProxyInstance(
            StorageClient.class.getClassLoader(),
            new Class<?>[] {StorageClient.class},
            (proxy, method, args) -> {
              if (method.getName().equals("close")) {
                closedClients.incrementAndGet();
                if (closeFailure) throw new IllegalStateException("close failed");
                return null;
              }
              if (!method.getName().equals("getObject"))
                throw new UnsupportedOperationException(method.getName());
              lastRequest = (StorageClient.GetObjectRequest) args[0];
              if (lastRequest.key().equals("missing"))
                throw StorageException.notFound("memory", "absent", null);
              return new StorageObject() {
                public String bucketName() {
                  return lastRequest.bucketName();
                }

                public String key() {
                  return lastRequest.key();
                }

                public ObjectMetadata metadata() {
                  throw new UnsupportedOperationException();
                }

                public InputStream content() {
                  if (readFailure)
                    return new InputStream() {
                      public int read() throws IOException {
                        throw new IOException("broken stream");
                      }
                    };
                  return new ByteArrayInputStream(
                      "café".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }

                public void close() {
                  closedObjects.incrementAndGet();
                }
              };
            });
  }

  record ClientConfig(String backend) implements NamedStorageClientConfig {
    public Optional<String> bucket() {
      return Optional.empty();
    }

    public Optional<String> endpoint() {
      return Optional.empty();
    }

    public Optional<String> publicEndpoint() {
      return Optional.empty();
    }

    public Optional<String> region() {
      return Optional.empty();
    }

    public Optional<String> accessKey() {
      return Optional.empty();
    }

    public Optional<String> secretKey() {
      return Optional.empty();
    }

    public Optional<Boolean> pathStyleAccess() {
      return Optional.empty();
    }

    public Map<String, String> properties() {
      return Map.of();
    }
  }

  record Configuration(
      Optional<String> defaultClient,
      Map<String, NamedStorageClientConfig> clients,
      Map<String, String> schemeClients,
      Map<String, String> uriClients)
      implements StorageConfigGroup {}

  static Configuration configuration() {
    return new Configuration(
        Optional.of("main"),
        Map.of("main", new ClientConfig("memory"), "other", new ClientConfig("memory")),
        Map.of("mem", "main"),
        Map.of("mem://private", "other"));
  }
}
