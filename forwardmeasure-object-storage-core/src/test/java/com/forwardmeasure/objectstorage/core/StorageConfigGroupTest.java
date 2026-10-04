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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageConfigGroupTest {

  @Test
  void resolvesTheMostSpecificUriBeforeSchemeAndDefaultMappings() {
    StorageConfigGroup configuration = configuration();

    assertEquals(
        "restricted",
        configuration
            .clientNameForUri(URI.create("s3://evidence/private/case/document.pdf"))
            .orElseThrow());
    assertEquals(
        "bucket",
        configuration
            .clientNameForUri(URI.create("s3://evidence/public/document.pdf"))
            .orElseThrow());
    assertEquals(
        "archive",
        configuration.clientNameForUri(URI.create("gs://archive/document.pdf")).orElseThrow());
    assertEquals(
        "general",
        configuration.clientNameForUri(URI.create("az://documents/document.pdf")).orElseThrow());
  }

  @Test
  void observesPathBoundariesAndDoesNotMatchLookalikeHosts() {
    StorageConfigGroup configuration = configuration();

    assertEquals(
        "bucket",
        configuration
            .clientNameForUri(URI.create("s3://evidence/privateer/document.pdf"))
            .orElseThrow());
    assertEquals(
        "scheme",
        configuration.clientNameForUri(URI.create("s3://not-evidence/document.pdf")).orElseThrow());
  }

  @Test
  void permitsNoDefaultClientAndFailsClosedForUnmappedSchemes() {
    StorageConfigGroup configuration =
        new StorageConfigGroup() {
          @Override
          public Optional<String> defaultClient() {
            return Optional.empty();
          }

          @Override
          public Map<String, NamedStorageClientConfig> clients() {
            return Map.of();
          }

          @Override
          public Map<String, String> schemeClients() {
            return Map.of("s3", "s3-client");
          }

          @Override
          public Map<String, String> uriClients() {
            return Map.of();
          }
        };

    assertEquals("s3-client", configuration.clientNameForScheme("s3").orElseThrow());
    assertTrue(configuration.clientNameForScheme("gs").isEmpty());
  }

  private static StorageConfigGroup configuration() {
    return new StorageConfigGroup() {
      @Override
      public Optional<String> defaultClient() {
        return Optional.of("general");
      }

      @Override
      public Map<String, NamedStorageClientConfig> clients() {
        return Map.of();
      }

      @Override
      public Map<String, String> schemeClients() {
        return Map.of("s3", "scheme", "gs", "archive");
      }

      @Override
      public Map<String, String> uriClients() {
        return Map.of(
            "evidence", "bucket",
            "s3://evidence/private", "restricted");
      }
    };
  }
}
