# ForwardMeasure Object Storage

Provider-neutral object-storage clients for S3-compatible storage, Google
Cloud Storage, and Azure Blob Storage.

The library preserves provider-native implementations behind a common API and
resolves named clients by configured URI scheme or URI prefix. Applications can
therefore use multiple providers and credential domains without coupling domain
code to a cloud SDK.

It is a Java library, not a storage proxy. Data continues to move directly
between an application and the selected provider. Provider implementations are
discovered with Java `ServiceLoader`, so the API and registry remain independent
of Quarkus, Spring, Micronaut, and CDI.

## Modules

- `forwardmeasure-object-storage-api` — provider-neutral requests, responses,
  metadata, streams, and failures.
- `forwardmeasure-object-storage-core` — named-client configuration, provider
  discovery, registry, and URI-based resource loading.
- `forwardmeasure-object-storage-s3` — AWS S3 and S3-compatible storage.
- `forwardmeasure-object-storage-gcs` — Google Cloud Storage.
- `forwardmeasure-object-storage-azure` — Azure Blob Storage.
- `forwardmeasure-object-storage-bom` — dependency-management entry point.

This repository is maintained independently of any application.

## Resource Identifiers

Persist canonical provider-neutral resource identifiers rather than SDK-specific
bucket/key fields:

- `s3://evidence-bucket/tenant/document.pdf`
- `gs://evidence-bucket/tenant/document.pdf`
- `az://evidence-container/tenant/document.pdf`

The registry resolves a resource identifier to a named client in this order:

1. The most-specific configured URI prefix.
2. The configured URI-scheme mapping.
3. The configured default client.

URI-prefix routing permits separate credentials for resources that share a
provider scheme, such as two S3 buckets owned by different security domains.

## Named Clients

Applications compose `StorageConfigGroup` into their own configuration root.
For example, a SmallRye Config mapping can expose the library under
`application.evidence.storage` without the library owning that prefix.

```yaml
application:
  evidence:
    storage:
      default-client: evidence
      scheme-clients:
        s3: evidence
        gs: evidence-gcs
        az: evidence-azure
      uri-clients:
        "s3://restricted-evidence/private": restricted-evidence
      clients:
        evidence:
          backend: s3
          endpoint: http://minio.storage.svc:9000
          public-endpoint: https://objects.example.com
          region: us-east-1
          path-style-access: true
          access-key: ${EVIDENCE_ACCESS_KEY}
          secret-key: ${EVIDENCE_SECRET_KEY}
```

`endpoint` is the address used by the application. `public-endpoint` is the
address placed in delegated upload/download capabilities returned to external
callers. They may be the same in a public cloud and different for an in-cluster
MinIO or emulator deployment.

Include the provider artifacts required by the application. The registry finds
them automatically:

```xml
<dependency>
  <groupId>com.forwardmeasure.objectstorage</groupId>
  <artifactId>forwardmeasure-object-storage-core</artifactId>
</dependency>
<dependency>
  <groupId>com.forwardmeasure.objectstorage</groupId>
  <artifactId>forwardmeasure-object-storage-s3</artifactId>
</dependency>
```

Presigned operations return both a URI and the exact HTTP headers that the
caller must send. This avoids the common error of treating a signed URI as the
entire delegated capability when a provider also signs headers such as content
type.

## Verification

Run the complete build with Java 25 and Maven 3.9.9 or newer:

```shell
mvn verify
```

The provider suites exercise actual HTTP storage behavior using MinIO,
fake-gcs-server, and Azurite containers. They cover bucket/container lifecycle,
object upload and download, metadata, ranged reads, delegated uploads and
downloads, deletion, and provider discovery. No cloud SDK or storage client is
mocked.
