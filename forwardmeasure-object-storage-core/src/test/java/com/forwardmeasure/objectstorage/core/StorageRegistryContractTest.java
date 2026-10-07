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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageRegistryContractTest {
  @Test
  void routesCacheByClientIdentityAndClosesEveryOwnedClientDespiteFailure() {
    var provider = new StorageTestProvider();
    var registry =
        new StorageClientRegistry(StorageTestProvider.configuration(), List.of(provider));
    var main = registry.get(" main ");
    assertSame(main, registry.getDefault());
    assertSame(main, registry.getForScheme("mem"));
    assertSame(main, registry.getForUri(URI.create("mem://public/key")));
    assertNotSame(main, registry.getForUri(URI.create("mem://private/key")));
    assertEquals(2, provider.created.get());
    assertTrue(registry.has(" main "));
    assertFalse(registry.has("missing"));
    assertFalse(registry.has(null));
    assertFalse(registry.has(" "));
    provider.closeFailure = true;
    registry.close();
    registry.close();
    assertEquals(2, provider.closedClients.get());
  }

  @Test
  void badProviderAndClientConfigurationFailsClosed() {
    var provider = new StorageTestProvider();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageClientRegistry(
                StorageTestProvider.configuration(), List.of(provider, provider)));
    var invalid =
        new StorageClientProvider() {
          public String backend() {
            return " ";
          }

          public StorageClient create(String name, NamedStorageClientConfig config) {
            throw new AssertionError();
          }
        };
    assertThrows(
        IllegalArgumentException.class,
        () -> new StorageClientRegistry(StorageTestProvider.configuration(), List.of(invalid)));
    try (var registry = new StorageClientRegistry(StorageTestProvider.configuration())) {
      assertThrows(StorageException.class, registry::getDefault);
    }
    for (String backend : new String[] {null, " ", "unknown"}) {
      var config =
          new StorageTestProvider.Configuration(
              Optional.empty(),
              Map.of("bad", new StorageTestProvider.ClientConfig(backend)),
              Map.of(),
              Map.of());
      try (var registry = new StorageClientRegistry(config, List.of(provider))) {
        for (String name : new String[] {null, " ", "missing", "bad"})
          assertThrows(StorageException.class, () -> registry.get(name));
        assertThrows(StorageException.class, registry::getDefault);
        assertThrows(StorageException.class, () -> registry.getForScheme("unknown"));
        assertThrows(
            StorageException.class, () -> registry.getForUri(URI.create("mem://bucket/key")));
      }
    }
    assertEquals(0, provider.created.get());
  }
}
