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
import io.aiven.kafka.tieredstorage.config.validators.ValidUrl;

public class AzureBlobStorageConfig extends AbstractConfig {
    static final String AZURE_ACCOUNT_NAME_CONFIG = "azure.account.name";
    private static final String AZURE_ACCOUNT_NAME_DOC = "Azure account name";

    static final String AZURE_ACCOUNT_KEY_CONFIG = "azure.account.key";
    private static final String AZURE_ACCOUNT_KEY_DOC = "Azure account key";

    static final String AZURE_CONTAINER_NAME_CONFIG = "azure.container.name";
    private static final String AZURE_CONTAINER_NAME_DOC = "Azure container to store log segments";

    static final String AZURE_ENDPOINT_URL_CONFIG = "azure.endpoint.url";
    private static final String AZURE_ENDPOINT_URL_DOC = "Custom Azure Blob Storage endpoint URL";

    static final String AZURE_CONNECTION_STRING_CONFIG = "azure.connection.string";
    private static final String AZURE_CONNECTION_STRING_DOC = "Azure connection string. "
        + "Cannot be used together with azure.account.name, azure.account.key, and azure.endpoint.url";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
            .define(
                AZURE_ACCOUNT_NAME_CONFIG,
                ConfigDef.Type.STRING,
                null,
                Null.or(new ConfigDef.NonEmptyString()),
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
                AZURE_CONTAINER_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                AZURE_CONTAINER_NAME_DOC)
            .define(
                AZURE_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                Null.or(new ValidUrl()),
                ConfigDef.Importance.LOW,
                AZURE_ENDPOINT_URL_DOC)
            .define(
                AZURE_CONNECTION_STRING_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_CONNECTION_STRING_DOC);
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
            if (endpointUrl() != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.endpoint.url\".");
            }
        } else {
            if (accountName() == null) {
                throw new ConfigException(
                    "\"azure.account.name\" must be set if \"azure.connection.string\" is not set.");
            }
        }
    }

    String accountName() {
        return getString(AZURE_ACCOUNT_NAME_CONFIG);
    }

    String accountKey() {
        final Password key = getPassword(AZURE_ACCOUNT_KEY_CONFIG);
        return key == null ? null : key.value();
    }

    String containerName() {
        return getString(AZURE_CONTAINER_NAME_CONFIG);
    }

    String endpointUrl() {
        return getString(AZURE_ENDPOINT_URL_CONFIG);
    }

    String connectionString() {
        final Password connectionString = getPassword(AZURE_CONNECTION_STRING_CONFIG);
        return connectionString == null ? null : connectionString.value();
    }
}
