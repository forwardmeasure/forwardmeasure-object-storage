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
package com.forwardmeasure.objectstorage.gcs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
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
import java.util.List;
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

  @Test
  void signedCapabilitiesPreserveObjectNamesAndVerifyAgainstThePublicHost() throws Exception {
    Path credentials = serviceAccountCredentials();
    try {
      var base = configuration("http://127.0.0.1:1", credentials);
      var publicConfig =
          (NamedStorageClientConfig)
              java.lang.reflect.Proxy.newProxyInstance(
                  NamedStorageClientConfig.class.getClassLoader(),
                  new Class<?>[] {NamedStorageClientConfig.class},
                  (proxy, method, args) ->
                      method.getName().equals("publicEndpoint")
                          ? Optional.of("https://public-storage.example.test/")
                          : method.invoke(base, args));
      try (var storage = new GcsStorageClient(publicConfig)) {
        String key = "folder/a b+é.txt";
        var get =
            storage.presignGet(
                new StorageClient.PresignGetRequest("bucket", key, Duration.ofMinutes(1)));
        assertEquals("/bucket/" + key, get.uri().getPath());
        assertEquals("public-storage.example.test", get.uri().getHost());
        verifySignature(get, "GET", credentials);
        var put =
            storage.presignPut(
                new StorageClient.PresignPutRequest(
                    "bucket", key, Duration.ofMinutes(1), "text/plain"));
        verifySignature(put, "PUT", credentials);
      }
    } finally {
      Files.deleteIfExists(credentials);
    }
  }

  @Test
  void filesStreamsPaginationAndInclusiveRangeBoundariesRoundTrip() throws Exception {
    int port = availablePort();
    String endpoint = "http://localhost:" + port;
    Path credentials = serviceAccountCredentials();
    try (var emulator = fakeGcs(port, endpoint)) {
      emulator.start();
      try (var storage = new GcsStorageClient(configuration(endpoint, credentials))) {
        String bucket = "extended-contract";
        storage.createBucket(
            new StorageClient.CreateBucketRequest(bucket, null, Map.of("location", " ")));
        assertTrue(storage.getBucket(bucket).isPresent());
        assertTrue(storage.listBuckets().stream().anyMatch(value -> value.name().equals(bucket)));
        assertTrue(storage.unwrap() instanceof com.google.cloud.storage.Storage);
        var file = Files.createTempFile("gcs-contract", ".txt");
        try {
          Files.writeString(file, "abcdef");
          storage.putObject(
              StorageClient.PutObjectRequest.fromPath(bucket, "a.txt", file, null, null));
          try (var input =
              new java.io.ByteArrayInputStream("stream".getBytes(StandardCharsets.UTF_8))) {
            storage.putObject(
                StorageClient.PutObjectRequest.fromStream(bucket, "b.txt", input, 6, " ", null));
          }
        } finally {
          Files.deleteIfExists(file);
        }
        var first =
            storage.listObjects(new StorageClient.ListObjectsRequest(bucket, "", "", 1, " "));
        assertTrue(first.isTruncated());
        var next =
            storage.listObjects(
                new StorageClient.ListObjectsRequest(
                    bucket, "", "", 1, first.nextContinuationToken()));
        assertEquals(1, next.objects().size());
        assertFalse(first.objects().getFirst().key().equals(next.objects().getFirst().key()));
        assertEquals(
            1,
            storage
                .listObjects(new StorageClient.ListObjectsRequest(bucket, "a", "/", 0, null))
                .objects()
                .size());
        try (var head =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", null, 2L));
            var tail =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", 3L, null));
            var beyond =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", 0L, 20L))) {
          assertEquals(bucket, head.bucketName());
          assertEquals("a.txt", head.key());
          assertEquals('a', head.content().read());
          assertEquals('b', head.content().read());
          assertEquals('c', head.content().read());
          assertEquals(-1, head.content().read());
          assertEquals(0, head.content().read(new byte[0], 0, 0));
          assertThrows(
              IndexOutOfBoundsException.class, () -> head.content().read(new byte[2], -1, 1));
          assertEquals("def", new String(tail.content().readAllBytes(), StandardCharsets.UTF_8));
          for (int character : "abcdef".chars().toArray())
            assertEquals(character, beyond.content().read());
          assertEquals(-1, beyond.content().read());
          assertEquals(-1, beyond.content().read(new byte[4]));
        }
        try (var unbounded =
            storage.getObject(
                new StorageClient.GetObjectRequest(bucket, "a.txt", 0L, Long.MAX_VALUE))) {
          assertEquals(
              "abcdef", new String(unbounded.content().readAllBytes(), StandardCharsets.UTF_8));
        }
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () -> storage.getObject(StorageClient.GetObjectRequest.of(bucket, "missing")))
                .code());
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () ->
                        storage.headObject(new StorageClient.HeadObjectRequest(bucket, "missing")))
                .code());
        assertEquals(
            "CONFLICT",
            assertThrows(
                    StorageException.class,
                    () ->
                        storage.createBucket(
                            new StorageClient.CreateBucketRequest(bucket, null, null)))
                .code());
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, true));
        assertTrue(storage.getBucket(bucket).isEmpty());
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, false));
        storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, "missing"));
      }
    } finally {
      Files.deleteIfExists(credentials);
    }
  }

  @Test
  void invalidPublicOriginsAndNonSigningCredentialsFailClosed() throws Exception {
    Path credentials = serviceAccountCredentials();
    try {
      var base = configuration("http://127.0.0.1:1", credentials);
      for (String invalid :
          List.of(
              "relative",
              "ftp://host",
              "https://user@host",
              "https://host/path",
              "https://host?query",
              "https://host#fragment")) {
        var changed = override(base, "publicEndpoint", Optional.of(invalid));
        try (var storage = new GcsStorageClient(changed)) {
          assertEquals(
              "INVALID_REQUEST",
              assertThrows(
                      StorageException.class,
                      () ->
                          storage.presignGet(
                              new StorageClient.PresignGetRequest(
                                  "bucket", "key", Duration.ofMinutes(1))))
                  .code());
        }
      }
      var nativeConfig =
          override(
              override(base, "endpoint", Optional.empty()), "publicEndpoint", Optional.empty());
      try (var storage = new GcsStorageClient(nativeConfig)) {
        var signed =
            storage.presignPut(
                new StorageClient.PresignPutRequest("bucket", "key", Duration.ofMinutes(1), " "));
        verifySignature(signed, "PUT", credentials);
      }
      var anonymous =
          override(base, "properties", Map.of("gcp.projectId", "test", "gcp.credentialsPath", " "));
      try (var storage = new GcsStorageClient(anonymous)) {
        assertEquals(
            "INVALID_REQUEST",
            assertThrows(
                    StorageException.class,
                    () ->
                        storage.presignGet(
                            new StorageClient.PresignGetRequest(
                                "bucket", "key", Duration.ofMinutes(1))))
                .code());
      }
      var invalidFile =
          override(base, "properties", Map.of("gcp.credentialsPath", credentials + ".absent"));
      assertEquals(
          "INVALID_REQUEST",
          assertThrows(StorageException.class, () -> new GcsStorageClient(invalidFile)).code());
    } finally {
      Files.deleteIfExists(credentials);
    }
  }

  private static NamedStorageClientConfig override(
      NamedStorageClientConfig base, String name, Object value) {
    return (NamedStorageClientConfig)
        java.lang.reflect.Proxy.newProxyInstance(
            NamedStorageClientConfig.class.getClassLoader(),
            new Class<?>[] {NamedStorageClientConfig.class},
            (proxy, method, args) ->
                method.getName().equals(name) ? value : method.invoke(base, args));
  }

  private static void verifySignature(
      StorageClient.PresignedRequest request, String method, Path credentials) throws Exception {
    var raw = new java.util.TreeMap<String, String>();
    for (String part : request.uri().getRawQuery().split("&")) {
      int split = part.indexOf('=');
      raw.put(part.substring(0, split), part.substring(split + 1));
    }
    byte[] signatureBytes = java.util.HexFormat.of().parseHex(raw.remove("X-Goog-Signature"));
    String signedHeaders =
        java.net.URLDecoder.decode(raw.get("X-Goog-SignedHeaders"), StandardCharsets.UTF_8);
    StringBuilder headers = new StringBuilder();
    for (String name : signedHeaders.split(";")) {
      String value =
          name.equals("host")
              ? request.uri().getRawAuthority()
              : request.requiredHeaders().get(name);
      headers.append(name).append(':').append(value).append('\n');
    }
    String query =
        raw.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(java.util.stream.Collectors.joining("&"));
    String canonical =
        method
            + "\n"
            + request.uri().getRawPath()
            + "\n"
            + query
            + "\n"
            + headers
            + "\n"
            + signedHeaders
            + "\nUNSIGNED-PAYLOAD";
    String credential =
        java.net.URLDecoder.decode(raw.get("X-Goog-Credential"), StandardCharsets.UTF_8);
    assertTrue(
        credential.startsWith("object-storage@object-storage-contract.iam.gserviceaccount.com/"));
    String scope = credential.substring(credential.indexOf('/') + 1);
    String hash =
        java.util.HexFormat.of()
            .formatHex(
                java.security.MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
    String toSign = "GOOG4-RSA-SHA256\n" + raw.get("X-Goog-Date") + "\n" + scope + "\n" + hash;
    java.security.interfaces.RSAPrivateCrtKey key;
    try (var input = Files.newInputStream(credentials)) {
      key =
          (java.security.interfaces.RSAPrivateCrtKey)
              com.google.auth.oauth2.ServiceAccountCredentials.fromStream(input).getPrivateKey();
    }
    var publicKey =
        java.security.KeyFactory.getInstance("RSA")
            .generatePublic(
                new java.security.spec.RSAPublicKeySpec(key.getModulus(), key.getPublicExponent()));
    var verifier = java.security.Signature.getInstance("SHA256withRSA");
    verifier.initVerify(publicKey);
    verifier.update(toSign.getBytes(StandardCharsets.UTF_8));
    assertTrue(
        verifier.verify(signatureBytes),
        "Signature must authenticate the URL and headers returned to the caller");
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
