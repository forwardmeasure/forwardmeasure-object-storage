package com.forwardmeasure.objectstorage.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StorageConfigGroupTest {

    @Test
    void resolvesTheMostSpecificUriBeforeSchemeAndDefaultMappings() {
        StorageConfigGroup configuration = configuration();

        assertEquals("restricted", configuration.clientNameForUri(
                URI.create("s3://evidence/private/case/document.pdf"))
                .orElseThrow());
        assertEquals("bucket", configuration.clientNameForUri(
                URI.create("s3://evidence/public/document.pdf"))
                .orElseThrow());
        assertEquals("archive", configuration.clientNameForUri(
                URI.create("gs://archive/document.pdf"))
                .orElseThrow());
        assertEquals("general", configuration.clientNameForUri(
                URI.create("az://documents/document.pdf"))
                .orElseThrow());
    }

    @Test
    void observesPathBoundariesAndDoesNotMatchLookalikeHosts() {
        StorageConfigGroup configuration = configuration();

        assertEquals("bucket", configuration.clientNameForUri(
                URI.create("s3://evidence/privateer/document.pdf"))
                .orElseThrow());
        assertEquals("scheme", configuration.clientNameForUri(
                URI.create("s3://not-evidence/document.pdf"))
                .orElseThrow());
    }

    private static StorageConfigGroup configuration() {
        return new StorageConfigGroup() {
            @Override
            public String defaultClient() {
                return "general";
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
