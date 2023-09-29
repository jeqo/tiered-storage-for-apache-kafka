/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.azure;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.tieredstorage.config.validators.NonEmptyPassword;
import io.aiven.kafka.tieredstorage.config.validators.Null;

public class AzureBlobStorageConfig extends AbstractConfig {
    static final String AZURE_ACCOUNT_NAME_CONFIG = "azure.account.name";
    private static final String AZURE_ACCOUNT_NAME_DOC = "Azure account name";

    static final String AZURE_ACCOUNT_KEY_CONFIG = "azure.account.key";
    private static final String AZURE_ACCOUNT_KEY_DOC = "Azure account key";

    static final String AZURE_SAS_TOKEN_CONFIG = "azure.sas.token";
    private static final String AZURE_SAS_TOKEN_DOC = "Azure SAS token";

    static final String AZURE_CONTAINER_NAME_CONFIG = "azure.container.name";
    private static final String AZURE_CONTAINER_NAME_DOC = "Azure container to store log segments";

    static final String AZURE_ENDPOINT_URL_CONFIG = "azure.endpoint.url";
    private static final String AZURE_ENDPOINT_URL_DOC = "Custom Azure Blob Storage endpoint URL";

    static final String AZURE_CONNECTION_STRING_CONFIG = "azure.connection.string";
    private static final String AZURE_CONNECTION_STRING_DOC = "Azure connection string. "
        + "Cannot be used together with azure.account.name, azure.account.key, and azure.endpoint.url";
    
    static final String AZURE_UPLOAD_BLOCK_SIZE_CONFIG = "azure.upload.block.size";
    private static final String AZURE_UPLOAD_BLOCK_SIZE_DOC = "Size of blocks to use when uploading objects to Azure";
    static final int AZURE_UPLOAD_BLOCK_SIZE_DEFAULT = 5 * 1024 * 1024; // 5MiB
    static final int AZURE_UPLOAD_BLOCK_SIZE_MIN = 100 * 1024;
    static final int AZURE_UPLOAD_BLOCK_SIZE_MAX = Integer.MAX_VALUE;

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
            .define(
                AZURE_ACCOUNT_NAME_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.HIGH,
                AZURE_ACCOUNT_NAME_DOC)
            .define(
                AZURE_ACCOUNT_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_ACCOUNT_KEY_DOC)
            .define(
                AZURE_SAS_TOKEN_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_SAS_TOKEN_DOC)
            .define(
                AZURE_CONTAINER_NAME_CONFIG,
                ConfigDef.Type.PASSWORD,
                ConfigDef.NO_DEFAULT_VALUE,
                new NonEmptyPassword(),
                ConfigDef.Importance.HIGH,
                AZURE_CONTAINER_NAME_DOC)
            .define(
                AZURE_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.LOW,
                AZURE_ENDPOINT_URL_DOC)
            .define(
                AZURE_CONNECTION_STRING_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_CONNECTION_STRING_DOC)
            .define(
                AZURE_UPLOAD_BLOCK_SIZE_CONFIG,
                ConfigDef.Type.INT,
                AZURE_UPLOAD_BLOCK_SIZE_DEFAULT,
                ConfigDef.Range.between(AZURE_UPLOAD_BLOCK_SIZE_MIN, AZURE_UPLOAD_BLOCK_SIZE_MAX),
                ConfigDef.Importance.MEDIUM,
                AZURE_UPLOAD_BLOCK_SIZE_DOC);
    }

    public AzureBlobStorageConfig(final Map<String, ?> props) {
        super(CONFIG, props);
        validate();
    }

    private void validate() {
        if (connectionString() != null) {
            if (accountName() != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.account.name\".");
            }
            if (accountKey() != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.account.key\".");
            }
            if (sasToken() != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.sas.token\".");
            }
            if (endpointUrl() != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.endpoint.url\".");
            }
        } else {
            if (accountName() == null && sasToken() == null) {
                throw new ConfigException(
                    "\"azure.account.name\" and/or \"azure.sas.token\" "
                        + "must be set if \"azure.connection.string\" is not set.");
            }
        }
    }

    String accountName() {
        return getPasswordValue(AZURE_ACCOUNT_NAME_CONFIG);
    }

    String accountKey() {
        return getPasswordValue(AZURE_ACCOUNT_KEY_CONFIG);
    }

    String sasToken() {
        final Password key = getPassword(AZURE_SAS_TOKEN_CONFIG);
        return key == null ? null : key.value();
    }

    String containerName() {
        return getPasswordValue(AZURE_CONTAINER_NAME_CONFIG);
    }

    String endpointUrl() {
        return getPasswordValue(AZURE_ENDPOINT_URL_CONFIG);
    }

    String connectionString() {
        return getPasswordValue(AZURE_CONNECTION_STRING_CONFIG);
    }

    private String getPasswordValue(final String configName) {
        final Password key = getPassword(configName);
        return key == null ? null : key.value();
    }

    int uploadBlockSize() {
        return getInt(AZURE_UPLOAD_BLOCK_SIZE_CONFIG);
    }
}
