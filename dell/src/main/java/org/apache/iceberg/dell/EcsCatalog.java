/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.CopyObjectRequest;
import com.emc.object.s3.request.ListObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ECS catalog implementation
 */
public class EcsCatalog extends BaseMetastoreCatalog implements SupportsNamespaces {

  private static final Logger log = LoggerFactory.getLogger(EcsCatalog.class);

  private String catalogName;
  private S3Client client;
  private FileIO fileIO;
  private String catalogBucket;
  /**
   * Catalog object prefix. Two of following:
   * 1. A prefix with delimiter suffix, such as data/ or result/
   * 2. A blank string, which means use the whole bucket as a catalog.
   */
  private String catalogObjectPrefix;
  /**
   * Warehouse is unified with other catalog that without delimiter.
   */
  private String warehouseLocation;

  private PropertiesSerDes propertiesSerDes;

  /**
   * @param name       a custom name for the catalog
   * @param properties catalog properties
   */
  @Override
  public void initialize(String name, Map<String, String> properties) {
    catalogName = name;
    client = EcsClientFactory.create(properties);
    fileIO = new EcsFileIO();
    fileIO.initialize(properties);
    String inputWarehouse = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    EcsURI uri = LocationUtils.checkAndParseLocation(inputWarehouse);
    warehouseLocation = new EcsURI(uri.getBucket(), LocationUtils.removeLastDelimiter(
        uri.getName(),
        EcsCatalogConstants.DELIMITER)).toString();
    catalogBucket = uri.getBucket();
    catalogObjectPrefix = LocationUtils.addLastDelimiterForPrefix(uri.getName(), EcsCatalogConstants.DELIMITER);
    propertiesSerDes = PropertiesSerDes.current();
  }

  /**
   * Return input name.
   */
  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Iterate all table objects with the namespace prefix.
   */
  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    List<TableIdentifier> tableIdList = new ArrayList<>();
    String prefix = getNamespacePrefix(namespace);
    String marker = null;
    do {
      ListObjectsResult listObjectsResult = client.listObjects(
          new ListObjectsRequest(catalogBucket)
              .withDelimiter(EcsCatalogConstants.DELIMITER)
              .withPrefix(prefix)
              .withMarker(marker));
      marker = listObjectsResult.getNextMarker();
      for (S3Object objectSummary : listObjectsResult.getObjects()) {
        if (!objectSummary.getKey().endsWith(EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX)) {
          continue;
        }
        tableIdList.add(parseTableId(namespace, prefix, objectSummary));
      }
    } while (marker != null);
    return tableIdList;
  }

  /**
   * Get object prefix of namespace.
   */
  private String getNamespacePrefix(Namespace namespace) {
    if (namespace.isEmpty()) {
      return catalogObjectPrefix;
    } else {
      return catalogObjectPrefix + String.join(EcsCatalogConstants.DELIMITER, namespace.levels()) +
          EcsCatalogConstants.DELIMITER;
    }
  }

  private TableIdentifier parseTableId(Namespace namespace, String prefix, S3Object s3Object) {
    String key = s3Object.getKey();
    Preconditions.checkArgument(key.startsWith(prefix),
        "List result should have same prefix", key, prefix);
    Preconditions.checkArgument(key.endsWith(EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX),
        "Key should have table suffix", key);
    String tableName = key.substring(
        prefix.length(),
        key.length() - EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX.length());
    return TableIdentifier.of(namespace, tableName);
  }

  /**
   * Remove table object. If the purge flag is set, remove all data objects.
   */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String tableObject = getTableObjectName(identifier);
    if (purge) {
      // if re-use the same instance, current() will throw exception.
      TableOperations ops = newTableOps(identifier);
      TableMetadata current = ops.current();
      if (current == null) {
        return false;
      }
      CatalogUtil.dropTableData(ops.io(), current);
    }
    client.deleteObject(catalogBucket, tableObject);
    return true;
  }

  private String getTableObjectName(TableIdentifier id) {
    return getNamespacePrefix(id.namespace()) + id.name() + EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX;
  }

  /**
   * Table rename will only move table object, the data objects will still be in-place.
   *
   * @param fromTable identifier of the table to rename
   * @param toTable   new table name
   */
  @Override
  public void renameTable(TableIdentifier fromTable, TableIdentifier toTable) {
    String fromObject = getTableObjectName(fromTable);
    String toObject = getTableObjectName(toTable);
    Optional<S3ObjectMetadata> fromMetadataOpt = getS3ObjectMetadata(fromObject);
    if (!fromMetadataOpt.isPresent()) {
      throw new NoSuchTableException("table %s(%s) is absent", fromTable, fromObject);
    }
    String eTag = fromMetadataOpt.get().getETag();
    CopyObjectRequest copyObjectRequest = new CopyObjectRequest(catalogBucket, fromObject, catalogBucket, toObject);
    copyObjectRequest.setIfSourceMatch(eTag);
    copyObjectRequest.setIfTargetNoneMatch("*");

    if (cas(() -> client.copyObject(copyObjectRequest))) {
      log.info("rename table {} to {}", fromTable, toTable);
      client.deleteObject(catalogBucket, fromObject);
    } else {
      throw new AlreadyExistsException("table %s is present", toTable);
    }
  }

  private void assertPrefix(String name) {
    Preconditions.checkArgument(name.startsWith(catalogObjectPrefix),
        "All properties objects should have prefix", name);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> properties) {
    if (namespace.isEmpty()) {
      throw new AlreadyExistsException("namespace %s has already existed", namespace);
    }
    String namespaceObject = getNamespaceObjectName(namespace);
    if (getS3ObjectMetadata(namespaceObject).isPresent()) {
      throw new AlreadyExistsException("namespace %s(%s) has already existed", namespace, namespaceObject);
    }
    if (!createNewPropertiesObject(namespaceObject, properties)) {
      throw new AlreadyExistsException("namespace %s(%s) has already existed", namespace, namespaceObject);
    }
  }

  private String getNamespaceObjectName(Namespace namespace) {
    return catalogObjectPrefix + String.join(EcsCatalogConstants.DELIMITER, namespace.levels()) +
        EcsCatalogConstants.NAMESPACE_PROPERTIES_OBJECT_SUFFIX;
  }

  @Override
  public List<Namespace> listNamespaces(Namespace parent) throws NoSuchNamespaceException {
    List<Namespace> namespaceList = new ArrayList<>();
    String prefix = getNamespacePrefix(parent);
    String marker = null;
    do {
      ListObjectsResult listObjectsResult = client.listObjects(
          new ListObjectsRequest(catalogBucket)
              .withDelimiter(EcsCatalogConstants.DELIMITER)
              .withPrefix(prefix)
              .withMarker(marker));
      marker = listObjectsResult.getNextMarker();
      for (S3Object objectSummary : listObjectsResult.getObjects()) {
        if (!objectSummary.getKey().endsWith(EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX)) {
          continue;
        }
        namespaceList.add(parseNamespace(parent, prefix, objectSummary));
      }
    } while (marker != null);
    return namespaceList;
  }

  private Namespace parseNamespace(Namespace parent, String prefix, S3Object s3Object) {
    String key = s3Object.getKey();
    Preconditions.checkArgument(key.startsWith(prefix),
        "List result should have same prefix", key, prefix);
    Preconditions.checkArgument(key.endsWith(EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX),
        "Key should have table suffix", key);
    String namespaceName = key.substring(
        prefix.length(),
        key.length() - EcsCatalogConstants.TABLE_PROPERTIES_OBJECT_SUFFIX.length());
    String[] namespace = Arrays.copyOf(parent.levels(), parent.levels().length + 1);
    namespace[namespace.length - 1] = namespaceName;
    return Namespace.of(namespace);
  }

  /**
   * Load namespace properties.
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    String namespaceObject = getNamespaceObjectName(namespace);
    if (!getS3ObjectMetadata(namespaceObject).isPresent()) {
      throw new NoSuchNamespaceException("Namespace %s(%s) properties object is absent", namespace, namespaceObject);
    }
    return getPropertiesFromObject(namespaceObject);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (namespace.isEmpty()) {
      // empty namespace can't be dropped
      return false;
    }
    if (!listNamespaces(namespace).isEmpty() || !listTables(namespace).isEmpty()) {
      throw new NamespaceNotEmptyException("namespace %s is not empty", namespace);
    }
    String namespaceObject = getNamespaceObjectName(namespace);
    client.deleteObject(catalogBucket, namespaceObject);
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    return updateProperties(namespace, r -> r.putAll(properties));
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    return updateProperties(namespace, r -> r.keySet().removeAll(properties));
  }

  public boolean updateProperties(Namespace namespace, Consumer<Map<String, String>> propertiesFn)
      throws NoSuchNamespaceException {
    // Load old properties
    Map<String, String> oldProperties = loadNamespaceMetadata(namespace);
    String eTag = oldProperties.get(EcsCatalogConstants.E_TAG_PROPERTIES_KEY);
    Preconditions.checkArgument(eTag != null, "ETag must in object properties");

    // Put new properties
    Map<String, String> newProperties = new LinkedHashMap<>(oldProperties);
    propertiesFn.accept(newProperties);
    return updatePropertiesObject(getNamespaceObjectName(namespace), eTag, newProperties);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    if (namespace.isEmpty()) {
      return true;
    }
    return getS3ObjectMetadata(getNamespaceObjectName(namespace)).isPresent();
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new EcsTableOperations(catalogName + "." + tableIdentifier,
        getTableObjectName(tableIdentifier), fileIO, this);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    StringBuilder builder = new StringBuilder();
    builder.append(warehouseLocation);
    for (String level : tableIdentifier.namespace().levels()) {
      builder.append(EcsCatalogConstants.DELIMITER);
      builder.append(level);
    }
    builder.append(EcsCatalogConstants.DELIMITER);
    builder.append(tableIdentifier.name());
    return builder.toString();
  }

  /// Util methods

  /**
   * Get S3 object metadata which include E-Tag, user metadata and so on.
   */
  public Optional<S3ObjectMetadata> getS3ObjectMetadata(String name) {
    assertPrefix(name);
    try {
      return Optional.of(client.getObjectMetadata(catalogBucket, name));
    } catch (S3Exception e) {
      if (e.getHttpCode() == 404) {
        return Optional.empty();
      }
      throw e;
    }
  }

  /**
   * Parse object content and metadata as properties.
   */
  public Map<String, String> getPropertiesFromObject(String name) {
    assertPrefix(name);
    GetObjectResult<InputStream> result = client.getObject(catalogBucket, name);
    S3ObjectMetadata objectMetadata = result.getObjectMetadata();
    String version = objectMetadata.getUserMetadata(EcsCatalogConstants.PROPERTIES_VERSION_OBJECT_USER_METADATA_KEY);
    Map<String, String> properties = new LinkedHashMap<>();
    try (InputStream input = result.getObject()) {
      properties.putAll(propertiesSerDes.read(input, version));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    properties.put(EcsCatalogConstants.E_TAG_PROPERTIES_KEY, objectMetadata.getETag());
    return properties;
  }

  /**
   * Create a new object to store properties.
   */
  public boolean createNewPropertiesObject(String name, Map<String, String> properties) {
    assertPrefix(name);
    PutObjectRequest request = new PutObjectRequest(catalogBucket, name, propertiesSerDes.toBytes(properties));
    request.setIfNoneMatch("*");
    request.setObjectMetadata(new S3ObjectMetadata().addUserMetadata(
        EcsCatalogConstants.PROPERTIES_VERSION_OBJECT_USER_METADATA_KEY,
        propertiesSerDes.getCurrentVersion()));
    return cas(() -> client.putObject(request));
  }

  /**
   * Update a exist object to store properties.
   */
  public boolean updatePropertiesObject(String name, String eTag, Map<String, String> properties) {
    assertPrefix(name);
    // Exclude some keys
    Map<String, String> newProperties = new LinkedHashMap<>(properties);
    newProperties.remove(EcsCatalogConstants.E_TAG_PROPERTIES_KEY);

    // Replace properties object
    PutObjectRequest request = new PutObjectRequest(catalogBucket, name, propertiesSerDes.toBytes(newProperties));
    request.setIfMatch(eTag);
    request.setObjectMetadata(new S3ObjectMetadata().addUserMetadata(
        EcsCatalogConstants.PROPERTIES_VERSION_OBJECT_USER_METADATA_KEY,
        propertiesSerDes.getCurrentVersion()));
    return cas(() -> client.putObject(request));
  }

  private boolean cas(Runnable task) {
    try {
      task.run();
      return true;
    } catch (S3Exception e) {
      if ("PreconditionFailed".equals(e.getErrorCode())) {
        return false;
      }
      throw e;
    }
  }
}
