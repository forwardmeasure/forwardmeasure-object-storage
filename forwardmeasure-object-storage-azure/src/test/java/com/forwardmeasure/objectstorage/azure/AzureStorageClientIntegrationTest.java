package com.forwardmeasure.objectstorage.azure;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Exercises the Azure adapter against a real Azurite Blob service. */
class AzureStorageClientIntegrationTest {

  private static final int BLOB_PORT = 10000;

  private static final String ACCOUNT = "devstoreaccount1";

  private static final String KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
          + "K1SZFPTOtr/KBHBeksoGMGw==";

  @Test
  void supportsLifecycleStreamingRangesAndSignedCapabilities() throws Exception {
    try (var azurite = azurite()) {
      azurite.start();
      try (var storage = new AzureStorageClient(configuration(azurite))) {
        String container = "object-storage-contract";
        String key = "tenant/evidence/document.txt";
        byte[] content = "provider-neutral evidence".getBytes(StandardCharsets.UTF_8);

        storage.createBucket(new StorageClient.CreateBucketRequest(container, Map.of(), Map.of()));
        assertTrue(storage.bucketExists(container));

        storage.putObject(
            StorageClient.PutObjectRequest.fromBytes(
                container, key, content, "text/plain", Map.of("tenant", "contract-test")));

        var head = storage.headObject(new StorageClient.HeadObjectRequest(container, key));
        assertEquals(content.length, head.size());
        assertEquals("contract-test", head.userMetadata().get("tenant"));

        try (var object = storage.getObject(StorageClient.GetObjectRequest.of(container, key))) {
          assertEquals("text/plain", object.metadata().contentType().orElseThrow());
          assertArrayEquals(content, object.content().readAllBytes());
        }

        try (var range =
            storage.getObject(new StorageClient.GetObjectRequest(container, key, 9L, 15L))) {
          assertEquals(
              "neutral", new String(range.content().readAllBytes(), StandardCharsets.UTF_8));
        }

        String uploadKey = "tenant/evidence/signed.txt";
        var putCapability =
            storage.presignPut(
                new StorageClient.PresignPutRequest(
                    container, uploadKey, Duration.ofMinutes(5), "text/plain"));
        var http = HttpClient.newHttpClient();
        var put =
            http.send(
                request(putCapability)
                    .PUT(HttpRequest.BodyPublishers.ofString("signed upload"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertTrue(put.statusCode() >= 200 && put.statusCode() < 300, put.body());

        var getCapability =
            storage.presignGet(
                new StorageClient.PresignGetRequest(container, uploadKey, Duration.ofMinutes(5)));
        var get =
            http.send(request(getCapability).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, get.statusCode());
        assertEquals("signed upload", get.body());

        storage.deleteObject(new StorageClient.DeleteObjectRequest(container, key));
        storage.deleteObject(new StorageClient.DeleteObjectRequest(container, uploadKey));
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(container, true));
        assertFalse(storage.bucketExists(container));
      }
    }
  }

  private static GenericContainer<?> azurite() {
    return new GenericContainer<>(
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.36.0"))
        .withExposedPorts(BLOB_PORT)
        .withCommand(
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            Integer.toString(BLOB_PORT),
            "--loose",
            "--skipApiVersionCheck")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1)));
  }

  private static NamedStorageClientConfig configuration(GenericContainer<?> azurite) {
    String endpoint = "http://" + azurite.getHost() + ':' + azurite.getMappedPort(BLOB_PORT);
    return new Configuration(endpoint);
  }

  private record Configuration(String endpointValue) implements NamedStorageClientConfig {

    @Override
    public String backend() {
      return "azure";
    }

    @Override
    public Optional<String> bucket() {
      return Optional.empty();
    }

    @Override
    public Optional<String> endpoint() {
      return Optional.of(endpointValue);
    }

    @Override
    public Optional<String> publicEndpoint() {
      return Optional.of(endpointValue);
    }

    @Override
    public Optional<String> region() {
      return Optional.empty();
    }

    @Override
    public Optional<String> accessKey() {
      return Optional.of(ACCOUNT);
    }

    @Override
    public Optional<String> secretKey() {
      return Optional.of(KEY);
    }

    @Override
    public Optional<Boolean> pathStyleAccess() {
      return Optional.of(true);
    }

    @Override
    public Map<String, String> properties() {
      return Map.of();
    }
  }

  private static HttpRequest.Builder request(StorageClient.PresignedRequest capability) {
    var builder = HttpRequest.newBuilder(capability.uri());
    capability.requiredHeaders().forEach(builder::header);
    return builder;
  }
}
