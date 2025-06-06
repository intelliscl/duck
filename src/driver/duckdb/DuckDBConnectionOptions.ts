import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"

/**
 * DuckDB connection options specific to the node-neo API
 */
export interface DuckDBConnectionCredentialsOptions {
    readonly type: "duckdb";

    /**
     * Database path. Use ":memory:" for in-memory database (default)
     */
    readonly database?: string // path to file or ':memory:'
    readonly schema?: string
    /**
     * Configuration options for DuckDB instance
     */
    readonly config?: {
        /**
         * Number of threads for DuckDB to use
         */
        threads?: string | number

        /**
         * Maximum memory to use (e.g., "512MB", "2GB")
         */
        max_memory?: string

        /**
         * Access mode: "automatic", "read_only", "read_write"
         */
        access_mode?: "automatic" | "read_only" | "read_write"

        /**
         * Temporary directory path
         */
        temp_directory?: string

        /**
         * Enable checkpoint on shutdown
         */
        checkpoint_on_shutdown?: boolean

        /**
         * Other DuckDB configuration options
         */
        [key: string]: any
    }

    /**
     * Use instance cache to prevent multiple instances on the same database
     */
    readonly useInstanceCache?: boolean

    /**
     * DuckLake: Connection string for the lake (see https://github.com/duckdb/ducklake)
     * Example: "ducklake://aws/s3/bucket/path" or "ducklake://gcp/gcs/bucket/path"
     */
    readonly lakeConnectionString?: string

    /**
     * DuckLake: List of lakes to attach on connect.
     * Each lake can specify a name, path, and cloud secret reference.
     */
    readonly lakes?: {
        /**
         * Logical name for the lake (used in ATTACH ... AS ...)
         */
        name: string

        /**
         * Data path for the lake (e.g., "s3://bucket/path" or "gcs://bucket/path")
         */
        path: string

        /**
         * Cloud secret to use for this lake (must match a secret defined below)
         */
        secret: string

        /**
         * Optional: scope for the secret (e.g., "session", "global")
         */
        scope?: "session" | "global"
    }[]

    /**
     * DuckLake: Cloud secrets for AWS/GCP/Azure.
     * These will be used to configure credentials for lakes.
     * See: https://duckdb.org/docs/stable/configuration/secrets_manager.html
     */
    readonly cloudSecrets?: {
        /**
         * Secret name (referenced by lakes[].secret)
         */
        name: string

        /**
         * Provider: "aws", "gcp", or "azure"
         */
        provider: "aws" | "gcp" | "azure" | "r2"


        /**
         * Scope for the secret (DuckDB supports "session" or "global")
         */
        scope?: "session" | "global"

        /**
         * AWS credentials (if provider is "aws")
         * See: https://duckdb.org/docs/stable/configuration/secrets_manager.html#aws
         */
        s3?: {
            type: "s3"
            PROVIDER: "config",
            KEY_ID: string
            SECRET?: string
            REGION?: string
            SCOPE?: string
        }

        /**
         * GCP credentials (if provider is "gcp")
         * See: https://duckdb.org/docs/stable/configuration/secrets_manager.html#gcp
         */
        gcs?: {
            type?: "gcs"
            KEY_ID?: string
            SECRET?: string
            USE_SSL?: "true"
        }

        r2?: {
            type?: "r2"
            KEY_ID?: string
            SECRET?: string
            ACCOUNT_ID?: string
            ENDPOINT?: string
        }

        /**
         * Azure credentials (if provider is "azure")
         * See: https://duckdb.org/docs/stable/core_extensions/azure.html
         */
        azure?: {
            type?: "azure"
            AZURE_STORAGE_ACCOUNT_NAME?: string
            AZURE_STORAGE_ACCOUNT_KEY?: string
            AZURE_STORAGE_SAS_TOKEN?: string
            AZURE_STORAGE_CONNECTION_STRING?: string
            AZURE_CLIENT_ID?: string
            AZURE_CLIENT_SECRET?: string
            AZURE_TENANT_ID?: string
            AZURE_AUTH_TYPE?: "default" | "client_secret" | "sas" | "account_key" | "connection_string"
            AZURE_BLOB_ENDPOINT?: string
            AZURE_BLOB_USE_HTTPS?: boolean
        }

        /**
         * Any additional provider-specific options
         */
        [key: string]: any
    }[]
}

/**
 * DuckDB specific connection options
 */
export interface DuckDBConnectionOptions extends BaseDataSourceOptions, DuckDBConnectionCredentialsOptions {
    /**
     * Database type
     */
    readonly type: "duckdb"
}
