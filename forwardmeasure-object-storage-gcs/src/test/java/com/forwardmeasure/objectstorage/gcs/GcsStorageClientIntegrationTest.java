package com.forwardmeasure.objectstorage.gcs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Exercises the GCS adapter against a real fake-gcs-server process. */
class GcsStorageClientIntegrationTest {

  private static final int GCS_PORT = 4443;

  @Test
  void supportsLifecycleStreamingRangesAndSignedCapabilities() throws Exception {
    int hostPort = availablePort();
    String endpoint = "http://localhost:" + hostPort;
    Path credentials = serviceAccountCredentials();

    try (var gcs = fakeGcs(hostPort, endpoint)) {
      gcs.start();
      try (var storage = new GcsStorageClient(configuration(endpoint, credentials))) {
        String bucket = "object-storage-contract";
        String key = "tenant/evidence/document.txt";
        byte[] content = "provider-neutral evidence".getBytes(StandardCharsets.UTF_8);

        storage.createBucket(
            new StorageClient.CreateBucketRequest(bucket, Map.of(), Map.of("location", "US")));
        assertTrue(storage.bucketExists(bucket));

        storage.putObject(
            StorageClient.PutObjectRequest.fromBytes(
                bucket, key, content, "text/plain", Map.of("tenant", "contract-test")));

        var head = storage.headObject(new StorageClient.HeadObjectRequest(bucket, key));
        assertEquals(content.length, head.size());
        assertEquals("contract-test", head.userMetadata().get("tenant"));

        try (var object = storage.getObject(StorageClient.GetObjectRequest.of(bucket, key))) {
          assertEquals("text/plain", object.metadata().contentType().orElseThrow());
          assertArrayEquals(content, object.content().readAllBytes());
        }

        try (var range =
            storage.getObject(new StorageClient.GetObjectRequest(bucket, key, 9L, 15L))) {
          assertEquals(
              "neutral", new String(range.content().readAllBytes(), StandardCharsets.UTF_8));
        }

        String uploadKey = "tenant/evidence/signed.txt";
        var signedPut =
            storage.presignPut(
                new StorageClient.PresignPutRequest(
                    bucket, uploadKey, Duration.ofMinutes(5), "text/plain"));
        assertTrue(signedPut.uri().getQuery().contains("X-Goog-Signature="));

        var http = HttpClient.newHttpClient();
        var put =
            http.send(
                request(signedPut)
                    .PUT(HttpRequest.BodyPublishers.ofString("signed upload"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertTrue(put.statusCode() >= 200 && put.statusCode() < 300, put.body());

        var signedGet =
            storage.presignGet(
                new StorageClient.PresignGetRequest(bucket, uploadKey, Duration.ofMinutes(5)));
        assertTrue(signedGet.uri().getQuery().contains("X-Goog-Signature="));
        var get = http.send(request(signedGet).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, get.statusCode());
        assertEquals("signed upload", get.body());

        storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, key));
        storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, uploadKey));
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, true));
        assertFalse(storage.bucketExists(bucket));
      }
    } finally {
      Files.deleteIfExists(credentials);
    }
  }

  private static GenericContainer<?> fakeGcs(int hostPort, String externalUrl) {
    return new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:1.54.0"))
        .withExposedPorts(GCS_PORT)
        .withCreateContainerCmdModifier(
            command ->
                command
                    .getHostConfig()
                    .withPortBindings(
                        new PortBinding(
                            Ports.Binding.bindPort(hostPort), new ExposedPort(GCS_PORT))))
        .withCommand(
            "-scheme",
            "http",
            "-port",
            Integer.toString(GCS_PORT),
            "-public-host",
            "localhost:" + hostPort,
            "-external-url",
            externalUrl)
        .waitingFor(
            Wait.forHttp("/storage/v1/b")
                .forPort(GCS_PORT)
                .withStartupTimeout(Duration.ofMinutes(1)));
  }

  private static HttpRequest.Builder request(StorageClient.PresignedRequest capability) {
    var builder = HttpRequest.newBuilder(capability.uri());
    capability.requiredHeaders().forEach(builder::header);
    return builder;
  }

  private static int availablePort() throws Exception {
    try (var socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  private static Path serviceAccountCredentials() throws Exception {
    var generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    String privateKey =
        "-----BEGIN PRIVATE KEY-----\n"
            + Base64.getMimeEncoder(64, new byte[] {'\n'})
                .encodeToString(generator.generateKeyPair().getPrivate().getEncoded())
            + "\n-----END PRIVATE KEY-----\n";
    String json =
        """
        {
          "type": "service_account",
          "project_id": "object-storage-contract",
          "private_key_id": "contract-test-key",
          "private_key": "%s",
          "client_email": "object-storage@object-storage-contract.iam.gserviceaccount.com",
          "client_id": "100000000000000000001",
          "token_uri": "https://oauth2.googleapis.com/token"
        }
        """
            .formatted(privateKey.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\""));
    Path file = Files.createTempFile("object-storage-gcs-", ".json");
    Files.writeString(file, json, StandardCharsets.UTF_8);
    return file;
  }

  private static NamedStorageClientConfig configuration(String endpoint, Path credentials) {
    return new Configuration(endpoint, credentials.toString());
  }

  private record Configuration(String endpointValue, String credentialsPath)
      implements NamedStorageClientConfig {

    @Override
    public String backend() {
      return "gcs";
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
      return Optional.empty();
    }

    @Override
    public Optional<String> secretKey() {
      return Optional.empty();
    }

    @Override
    public Optional<Boolean> pathStyleAccess() {
      return Optional.of(false);
    }

    @Override
    public Map<String, String> properties() {
      return Map.of(
          "gcp.projectId", "object-storage-contract", "gcp.credentialsPath", credentialsPath);
    }
  }
}
