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
import java.util.Collections;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;

/**
 * Use {@link S3Client} to implement {@link BaseMetastoreTableOperations}
 */
public class EcsTableOperations extends BaseMetastoreTableOperations {

  public static final String ICEBERG_METADATA_LOCATION = "iceberg_metadata_location";

  private final String tableName;
  private final FileIO io;
  private final EcsCatalog catalog;
  private final String tableMetadataObject;

  /**
   * cached properties for CAS commit
   *
   * @see #doRefresh() when reset this field
   * @see #doCommit(TableMetadata, TableMetadata) when use this field
   */
  private Map<String, String> cachedProperties;

  public EcsTableOperations(
      String tableName,
      String tableMetadataObject,
      FileIO io,
      EcsCatalog catalog) {
    this.tableName = tableName;
    this.tableMetadataObject = tableMetadataObject;
    this.io = io;
    this.catalog = catalog;
  }

  @Override
  protected String tableName() {
    return tableName;
  }

  @Override
  public FileIO io() {
    return io;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation;
    if (!catalog.getS3ObjectMetadata(tableMetadataObject).isPresent()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException("metadata is null");
      } else {
        metadataLocation = null;
      }
    } else {
      Map<String, String> metadata = loadTableMetadata();
      this.cachedProperties = metadata;
      metadataLocation = metadata.get(ICEBERG_METADATA_LOCATION);
      if (metadataLocation == null) {
        throw new IllegalStateException("can't find location from table metadata");
      }
    }
    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    int currentVersion = currentVersion();
    int nextVersion = currentVersion + 1;
    String newMetadataLocation = writeNewMetadata(metadata, nextVersion);
    if (base == null) {
      // create a new table, the metadataKey should be absent
      if (!catalog.createNewPropertiesObject(tableMetadataObject, buildProperties(newMetadataLocation))) {
        throw new CommitFailedException("table exist when commit %s(%s)", debug(metadata), newMetadataLocation);
      }
    } else {
      Map<String, String> properties = cachedProperties;
      if (properties == null) {
        throw new CommitFailedException("when commit, local metadata is null, %s(%s) -> %s(%s)",
            debug(base), currentMetadataLocation(),
            debug(metadata), newMetadataLocation);
      }
      // replace to a new version, the E-Tag should be present and matched
      boolean result = catalog.updatePropertiesObject(
          tableMetadataObject,
          properties.get(EcsCatalogConstants.E_TAG_PROPERTIES_KEY),
          buildProperties(newMetadataLocation));
      if (!result) {
        throw new CommitFailedException("replace failed, properties %s, %s(%s) -> %s(%s)", properties,
            debug(base), currentMetadataLocation(),
            debug(metadata), newMetadataLocation);
      }
    }
  }

  private Map<String, String> loadTableMetadata() {
    return catalog.getPropertiesFromObject(tableMetadataObject);
  }

  /**
   * debug string for exception
   *
   * @param metadata is table metadata
   * @return debug string of metadata
   */
  private String debug(TableMetadata metadata) {
    if (metadata.currentSnapshot() == null) {
      return "EmptyTable";
    } else {
      return "Table(currentSnapshotId = " + metadata.currentSnapshot().snapshotId() + ")";
    }
  }

  /**
   * build a new properties for table
   *
   * @param metadataLocation is metadata json file location
   * @return properties
   */
  private Map<String, String> buildProperties(String metadataLocation) {
    return Collections.singletonMap(ICEBERG_METADATA_LOCATION, metadataLocation);
  }
}
