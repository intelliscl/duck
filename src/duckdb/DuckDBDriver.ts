import { Driver } from "../Driver"
import { DuckDBConnectionOptions } from "./DuckDBConnectionOptions"
//import { DuckDBQueryRunner } from "./DuckDBQueryRunner"
import { DataSource } from "../../data-source/DataSource"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { ColumnType } from "../types/ColumnTypes"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { SchemaBuilder } from "../../schema-builder/SchemaBuilder"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import { TypeORMError } from "../../error"
import { ApplyValueTransformers } from "../../util/ApplyValueTransformers"
import { OrmUtils } from "../../util/OrmUtils"
import { Table } from "../../schema-builder/table/Table"
import { View } from "../../schema-builder/view/View"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { UpsertType } from "../types/UpsertType"
import { ReplicationMode } from "../types/ReplicationMode"
import { SelectQueryBuilder } from "../../query-builder/SelectQueryBuilder"
import { EntityTarget } from "../../common/EntityTarget"
import { SqlInMemory } from "../../driver/SqlInMemory"
import { OnDeleteType } from "../../metadata/types/OnDeleteType"
import { OnUpdateType } from "../../metadata/types/OnUpdateType"

/**
 * Organizes communication with DuckDB database
 */
export class DuckDBDriver implements Driver {

    /**
     * Loaded DuckDB module
     */
    protected duckdb?: any


    /**
     * Connection used by driver
     */
    connection: DataSource

    /**
     * DuckDB instance
     */
    duckdbInstance: any

    master: any

    slaves: any[]=[]

    /**
     * Database version
     */
    version?: string

    /**
     * Supported ON DELETE types
     */
    supportedOnDeleteTypes: OnDeleteType[] = ["CASCADE", "RESTRICT", "SET NULL", "NO ACTION"]

    /**
     * Supported ON UPDATE types
     */
    supportedOnUpdateTypes: OnUpdateType[] = ["CASCADE", "RESTRICT", "SET NULL", "NO ACTION"]

    /**
     * Name of dummy table for certain operations
     */
    dummyTableName?: string = "dual"

    /**
     * We store all created query runners because we need to release them.
     */

    connectedQueryRunners: QueryRunner[] = []


    /**
     * Real database connection
     */
    duckdbConnection: any

    /**
     * Connection options
     */
    options: DuckDBConnectionOptions

    /**
     * Database name
     */
    database?: string

    /**
     * Schema name
     */
    schema?: string

    /**
     * Indicates if replication is enabled
     */
    isReplicated = false

    /**
     * Indicates if tree tables are supported
     */
    treeSupport = true

    /**
     * Transaction support: DuckDB supports nested transactions
     */
    transactionSupport = "simple" as const

    /**
     * Gets list of supported column data types by a driver
     */
    supportedDataTypes: ColumnType[] = [
        "bigint",
        "bit",
        "blob",
        "boolean",
        "date",
        "decimal",
        "double",
        "double precision",
        "float",
        "int",
        "integer",
        "interval",
        "json",
        "real",
        "smallint",
        "time",
        "timestamp",
        "timestamp with time zone",
        "tinyint",
        "uuid",
        "varchar",
        // Unsigned types
        "unsigned big int",
        // Array and List types
        "int array",
        "bigint array",
        "smallint array",
        "text array",
        // Special types
        "enum",
        "hugeint",
           "list",
        "struct",
        "map",
        "union",
       // "uinteger",
        "ubigint",
        "usmallint",
        "utinyint",
        "hugeint",
       // "uhugeint",
        // Spatial types when spatial extension is loaded
        "geometry",
        "point",
        "linestring",
        "polygon"
    ]

    /**
     * Returns type of upsert supported by driver if any
     */
    supportedUpsertTypes: UpsertType[] = ["on-conflict-do-update"]

    /**
     * Gets list of spatial column data types
     */
    spatialTypes: ColumnType[] = [] // DuckDB doesn't have built-in spatial types yet

    /**
     * Gets list of column data types that support length by a driver
     */
    withLengthColumnTypes: ColumnType[] = ["varchar", "bit"]

    /**
     * Gets list of column data types that support precision by a driver
     */
    withPrecisionColumnTypes: ColumnType[] = ["decimal", "time", "timestamp", "timestamp with time zone"]

    /**
     * Gets list of column data types that support scale by a driver
     */
    withScaleColumnTypes: ColumnType[] = ["decimal"]

    /**
     * ORM has special columns and we need to know what database column types should be for those
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDateDefault: "CURRENT_TIMESTAMP",
        updateDate: "timestamp",
        updateDateDefault: "CURRENT_TIMESTAMP",
        deleteDate: "timestamp",
        deleteDateNullable: true,
        version: "integer",
        treeLevel: "integer",
        migrationId: "integer",
        migrationName: "varchar",
        migrationTimestamp: "bigint",
        cacheId: "integer",
        cacheIdentifier: "varchar",
        cacheTime: "bigint",
        cacheDuration: "integer",
        cacheQuery: "text",
        cacheResult: "text",
        metadataType: "varchar",
        metadataDatabase: "varchar",
        metadataSchema: "varchar",
        metadataTable: "varchar",
        metadataName: "varchar",
        metadataValue: "text",
    }


    /**
     * The prefix used for the parameters
     */
    parametersPrefix: string = "$"

    /**
     * Default values of length, precision and scale depends on column data type
     */
    dataTypeDefaults: DataTypeDefaults = {
        "varchar": { length: 255 },
        "bit": { length: 1 },
        "decimal": { precision: 18, scale: 3 },
    }

    /**
     * Max length allowed by the DBMS for aliases
     */
    maxAliasLength = 63


    /**
     * CTE capabilities
     */
    cteCapabilities: CteCapabilities = {
        enabled: true,
        requiresRecursiveHint: true,
        materializedHint: true
    }


    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection?: DataSource) {
    if (!connection) {
        return
    }

    this.connection = connection
    this.options = connection.options as DuckDBConnectionOptions
    // Set database path
    this.database = this.options.database || ":memory:"

    // Set schema if provided
    if (this.options.schema) {
        this.schema = this.options.schema
    }

    //this.version?: string | undefined
    //this.supportedOnDeleteTypes?: OnDeleteType[] | undefined
    //this.supportedOnUpdateTypes?: OnUpdateType[] | undefined

    //dummyTableName?: string | undefined
}


    // -------------------------------------------------------------------------
    // Public Implemented Methods
    // -------------------------------------------------------------------------
     /**
     * Performs connection to the database
     */

    /**
     * Loads DuckDB module lazily
     */
    protected async loadDuckDBModule(): Promise<any> {
        try {
            // Check if already loaded
            if (this.duckdb) {
                return this.duckdb
            }

            // Dynamically import the module
            this.duckdb = await import("@duckdb/node-api")
            return this.duckdb
        } catch (error) {
            throw new TypeORMError(
                `DuckDB package (@duckdb/node-api) is not installed. Please run "npm install @duckdb/node-api"`
            )
        }
    }



    private async configureCloudSecrets(): Promise<void> {
    if (!this.options.cloudSecrets || this.options.cloudSecrets.length === 0) {
        return
    }

    for (const secret of this.options.cloudSecrets) {
        let secretQuery = ""

        if (secret.provider === "aws" && secret.s3) {
            secretQuery = `
                CREATE SECRET IF NOT EXISTS ${this.escape(secret.name)} (
                    TYPE S3,
                    PROVIDER '${secret.s3.PROVIDER}',
                    KEY_ID '${secret.s3.KEY_ID}',
                    SECRET '${secret.s3.SECRET}',
                    REGION '${secret.s3.REGION || 'us-east-1'}',
                    SCOPE ${secret.scope || 'session'}
                )
            `
        } else if (secret.provider === "gcp" && secret.gcs) {
            secretQuery = `
                CREATE SECRET IF NOT EXISTS ${this.escape(secret.name)} (
                    TYPE GCS,
                    KEY_ID '${secret.gcs.KEY_ID}',
                    SECRET '${secret.gcs.SECRET}',
                    SCOPE ${secret.scope || 'session'}
                )
            `
        } else if (secret.provider === "r2" && secret.r2) {
            secretQuery = `
                CREATE SECRET IF NOT EXISTS ${this.escape(secret.name)} (
                    TYPE R2,
                    KEY_ID '${secret.r2.KEY_ID}',
                    SECRET '${secret.r2.SECRET}',
                    ACCOUNT_ID '${secret.r2.ACCOUNT_ID}',
                    ENDPOINT '${secret.r2.ENDPOINT || 'https://<account_id>.r2.cloudflarestorage.com'}',
                    SCOPE ${secret.scope || 'session'}
                )
            `
        } else if (secret.provider === "azure" && secret.azure) {
            // Handle Azure secrets based on auth type
            const azureConfig = secret.azure
            let authConfig = ""

            if (azureConfig.AZURE_AUTH_TYPE === 'account_key') {
                authConfig = `ACCOUNT_NAME '${azureConfig.AZURE_STORAGE_ACCOUNT_NAME}',
                             ACCOUNT_KEY '${azureConfig.AZURE_STORAGE_ACCOUNT_KEY}'`
            } else if (azureConfig.AZURE_AUTH_TYPE === 'sas') {
                authConfig = `SAS_TOKEN '${azureConfig.AZURE_STORAGE_SAS_TOKEN}'`
            }
            // Add other auth types as needed

            secretQuery = `
                CREATE SECRET IF NOT EXISTS ${this.escape(secret.name)} (
                    TYPE AZURE,
                    ${authConfig},
                    SCOPE ${secret.scope || 'session'}
                )
            `
        }

        if (secretQuery) {
            await this.query(secretQuery)
        }
    }
}


        private async query(query: string, parameters?: any[]): Promise<any> {
            return new Promise((resolve, reject) => {
                if (parameters && parameters.length > 0) {
                    this.duckdbConnection.prepare(query, (err: any, statement: any) => {
                        if (err) return reject(err)

                        parameters.forEach((param, index) => {
                            this.bindParameter(statement, index + 1, param)
                        })

                        statement.run((err: any, result: any) => {
                            if (err) return reject(err)
                            statement.finalize()
                            resolve(result)
                        })
                    })
                } else {
                    this.duckdbConnection.run(query, (err: any, result: any) => {
                        if (err) return reject(err)
                        resolve(result)
                    })
                }
            })
        }



        /**
         * Attaches configured lakes to DuckDB
         */
        private async attachLakes(): Promise<void> {
            if (!this.options.lakes || this.options.lakes.length === 0) {
                return
            }

            for (const lake of this.options.lakes) {
                const attachQuery = `
                    ATTACH '${lake.path}' AS ${this.escape(lake.name)} (TYPE DUCKDB)
                `

                // If a secret is specified, it should already be configured
                await this.query(attachQuery)
            }
        }



    async connect(): Promise<void> {
        // Load DuckDB module
        const duckdb = await this.loadDuckDBModule()

        // Handle instance caching
        if (this.options.useInstanceCache) {
            const DuckDBInstanceCache = duckdb.DuckDBInstanceCache
            const cache = await DuckDBInstanceCache.create()
            this.duckdbInstance = await cache.getOrCreateInstance(
                this.database!,
                this.options.config
            )
        } else {
            // Direct instance creation
            const DuckDBInstance = duckdb.DuckDBInstance
            this.duckdbInstance = await DuckDBInstance.create(
                this.database!,
                this.options.config
            )
        }

        // Create connection
        this.duckdbConnection = await new Promise((resolve, reject) => {
            this.duckdbInstance.connect((err: any, connection: any) => {
                if (err) return reject(err)
                resolve(connection)
            })
        })

    // Configure cloud storage
    await this.configureCloudSecrets()
    await this.attachLakes()
}

    async afterConnect(): Promise<void> {
        // Set up extensions if needed
        await this.query("INSTALL httpfs")
        await this.query("LOAD httpfs")

        if (this.options.lakes && this.options.lakes.length > 0) {
            // Install cloud extensions as needed
            const providers = new Set(this.options.cloudSecrets?.map(s => s.provider) || [])

            if (providers.has('aws')) {
                await this.query("INSTALL aws")
                await this.query("LOAD aws")
            }

            if (providers.has('azure')) {
                await this.query("INSTALL azure")
                await this.query("LOAD azure")
            }
        }
    }



       /**
     * Closes connection with database and releases all connection pools
     */
    async disconnect(): Promise<void> {
        // Release all query runners first
        await Promise.all(this.connectedQueryRunners.map(qr => qr.release()))
        this.connectedQueryRunners = []

        // Then close the connection
        if (this.duckdbConnection && typeof this.duckdbConnection.close === "function") {
            await new Promise((resolve, reject) => {
                this.duckdbConnection.close((err: any) => {
                    if (err) return reject(err)
                    resolve(undefined)
                })
            })
            this.duckdbConnection = undefined
        }

        // Close the database instance
        if (this.duckdbInstance && typeof this.duckdbInstance.close === "function") {
            await new Promise((resolve, reject) => {
                this.duckdbInstance.close((err: any) => {
                    if (err) return reject(err)
                    resolve(undefined)
                })
            })
            this.duckdbInstance = undefined
        }
    }







/**
 * Helper to bind parameters to prepared statements
 */
private bindParameter(statement: any, index: number, value: any): void {
    if (value === null || value === undefined) {
        statement.bindNull(index)
    } else if (typeof value === "boolean") {
        statement.bindBoolean(index, value)
    } else if (typeof value === "number") {
        if (Number.isInteger(value)) {
            statement.bindInteger(index, value)
        } else {
            statement.bindDouble(index, value)
        }
    } else if (typeof value === "bigint") {
        statement.bindBigInt(index, value)
    } else if (typeof value === "string") {
        statement.bindText(index, value)
    } else if (value instanceof Date) {
        statement.bindText(index, value.toISOString())
    } else if (value instanceof Buffer || value instanceof Uint8Array) {
        statement.bindBlob(index, value)
    } else {
        statement.bindText(index, JSON.stringify(value))
    }
}

    /**
     * Creates a schema builder used to build and sync a schema
     */
    createSchemaBuilder(): SchemaBuilder {
        // DuckDBSchemaBuilder is defined below
        return new DuckDBSchemaBuilder()
    }

    /**
     * Creates a query runner used to execute database queries
     */
    createQueryRunner(mode: ReplicationMode = "master"): QueryRunner {
        const queryRunner = new DuckDBQueryRunner(this, mode)
        this.connectedQueryRunners.push(queryRunner)
        return queryRunner
    }




    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformTo(columnMetadata.transformer, value)

        if (value === null || value === undefined)
            return value

        switch (columnMetadata.type) {
            case "boolean":
                return value === true || value === "true" || value === 1
            case "date":
                return this.formatDate(value)
            case "time":
                return this.formatTime(value)
            case "timestamp":
            case "timestamp with time zone":
                return this.formatTimestamp(value)
            case "json":
                return JSON.stringify(value)
            case "uuid":
                return (value as any).toString()
            case "enum":
                return value
            default:
                return value
        }
    }

    /**
     * Prepares given value to a value to be hydrated, based on its column type or metadata
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return columnMetadata.transformer
                ? ApplyValueTransformers.transformFrom(columnMetadata.transformer, value)
                : value

        switch (columnMetadata.type) {
            case "boolean":
                value = value === true || value === 1 || value === "1" || value === "true"
                break
            case "date":
                value = new Date(value)
                break
            case "time":
                value == value
                break
            case "timestamp":
            case "timestamp with time zone":
                value = new Date(value)
                break
            case "json":
                value = typeof value === "string" ? JSON.parse(value) : value
                break
        }

        if (columnMetadata.transformer)
            value = ApplyValueTransformers.transformFrom(columnMetadata.transformer, value)

        return value
    }

    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query
     */
    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
        nativeParameters: ObjectLiteral
    ): [string, any[]] {
        const escapedParameters: any[] = Object.keys(nativeParameters).map((key) => {
            if (nativeParameters[key] instanceof Array) {
                return nativeParameters[key]
            }
            return nativeParameters[key]
        })

        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters]

        sql = sql.replace(/:(\.\.\.)?([A-Za-z0-9_.]+)/g, (full, isArray: string, key: string): string => {
            if (!parameters.hasOwnProperty(key)) {
                return full
            }

            let value: any = parameters[key]

            if (isArray) {
                return value.map((v: any) => {
                    escapedParameters.push(v)
                    return "$" + escapedParameters.length
                }).join(", ")
            }

            if (value instanceof Function) {
                return value()
            }

            escapedParameters.push(value)
            return "$" + escapedParameters.length
        })

        return [sql, escapedParameters]
    }

    /**
     * Escapes a column name
     */
    escape(columnName: string): string {
        return `"${columnName}"`
    }

    /**
     * Build full table name with schema name if available
     */
    buildTableName(tableName: string, schema?: string): string {
        schema = schema || this.schema

        if (schema && schema !== "main") {
            return `${this.escape(schema)}.${this.escape(tableName)}`
        }

        return this.escape(tableName)
    }

    /**
     * Creates a database type from a given column metadata
     */
    normalizeType(column: {
        type?: ColumnType | string
        length?: number | string
        precision?: number | null
        scale?: number
        isArray?: boolean
    }): string {
        if (column.type === Number || column.type === "int") {
            return "integer"
        } else if (column.type === String) {
            return "varchar"
        } else if (column.type === Date) {
            return "timestamp"
        } else if (column.type === Boolean) {
            return "boolean"
        } else if ((column.type as any) === Buffer) {
            return "blob"
        } else if (column.type === "uuid") {
            return "uuid"
        } else if (column.type === "json") {
            return "json"
        }

        return column.type as string || ""
    }


    /**
     * If parameter is a datetime function, e.g. "CURRENT_TIMESTAMP", normalizes it.
     * Otherwise returns original input.
     */
    protected normalizeDatetimeFunction(value: string) {
        // check if input is datetime function
        const upperCaseValue = value.toUpperCase()
        const isDatetimeFunction =
            upperCaseValue.indexOf("CURRENT_TIMESTAMP") !== -1 ||
            upperCaseValue.indexOf("CURRENT_DATE") !== -1 ||
            upperCaseValue.indexOf("CURRENT_TIME") !== -1 ||
            upperCaseValue.indexOf("LOCALTIMESTAMP") !== -1 ||
            upperCaseValue.indexOf("LOCALTIME") !== -1

        if (isDatetimeFunction) {
            // extract precision, e.g. "(3)"
            const precision = value.match(/\(\d+\)/)

            if (upperCaseValue.indexOf("CURRENT_TIMESTAMP") !== -1) {
                return precision
                    ? `('now'::text)::timestamp${precision[0]} with time zone`
                    : "now()"
            } else if (upperCaseValue === "CURRENT_DATE") {
                return "('now'::text)::date"
            } else if (upperCaseValue.indexOf("CURRENT_TIME") !== -1) {
                return precision
                    ? `('now'::text)::time${precision[0]} with time zone`
                    : "('now'::text)::time with time zone"
            } else if (upperCaseValue.indexOf("LOCALTIMESTAMP") !== -1) {
                return precision
                    ? `('now'::text)::timestamp${precision[0]} without time zone`
                    : "('now'::text)::timestamp without time zone"
            } else if (upperCaseValue.indexOf("LOCALTIME") !== -1) {
                return precision
                    ? `('now'::text)::time${precision[0]} without time zone`
                    : "('now'::text)::time without time zone"
            }
        }

        return value
    }


    /**
     * Normalizes "default" value of the column
     */
     /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        const defaultValue = columnMetadata.default

        if (defaultValue === null || defaultValue === undefined) {
            return undefined
        }

        if (columnMetadata.isArray && Array.isArray(defaultValue)) {
            return `'{${defaultValue
                .map((val: string) => `${val}`)
                .join(",")}}'`
        }

        if (
            (columnMetadata.type === "enum" ||
                columnMetadata.type === "simple-enum" ||
                typeof defaultValue === "number" ||
                typeof defaultValue === "string") &&
            defaultValue !== undefined
        ) {
            return `'${defaultValue}'`
        }

        if (typeof defaultValue === "boolean") {
            return defaultValue ? "true" : "false"
        }

        if (typeof defaultValue === "function") {
            const value = defaultValue()

            return this.normalizeDatetimeFunction(value)
        }

        if (typeof defaultValue === "object") {
            return `'${JSON.stringify(defaultValue)}'`
        }

        return `${defaultValue}`
    }


    /**
     * Normalizes "isUnique" value of the column
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.indices.some(
            idx => idx.isUnique &&
            idx.columns.length === 1 &&
            idx.columns[0] === column
        )
    }

    /**
     * Returns default column lengths, which is required on column creation
     */
    getColumnLength(column: ColumnMetadata | TableColumn): string {
        if (column.length)
            return column.length.toString()

        if (column.type === "varchar")
            return "255"

        return ""
    }

    /**
     * Creates column type definition including length, precision and scale
     */
    createFullType(column: TableColumn): string {
        let type = column.type

        if (column.length) {
            type += `(${column.length})`
        } else if (column.precision !== null && column.precision !== undefined && column.scale !== null && column.scale !== undefined) {
            type += `(${column.precision},${column.scale})`
        } else if (column.precision !== null && column.precision !== undefined) {
            type += `(${column.precision})`
        }

        if (column.isArray) {
            type = type + "[]"
        }

        return type
    }

    /**
     * Obtains a new database connection to a master server
     */
    async obtainMasterConnection(): Promise<any> {
        // Always return the same connection for DuckDB
        return this.duckdbConnection
    }

    /**
     * Obtains a new database connection to a slave server
     */
    async obtainSlaveConnection(): Promise<any> {
        // Always return the same connection for DuckDB
        return this.duckdbConnection
    }

    /**
     * Creates generated map of values generated or returned by database after INSERT query
     */
    createGeneratedMap(metadata: EntityMetadata, insertResult: any, entityIndex: number): any {
        const generatedMap = metadata.generatedColumns.reduce((map, generatedColumn) => {
            let value: any
            if (generatedColumn.generationStrategy === "increment" && insertResult) {
                // DuckDB returns the last inserted row id
                value = insertResult
            }
            return OrmUtils.mergeDeep(map, generatedColumn.createValueMap(value))
        }, {} as ObjectLiteral)

        return Object.keys(generatedMap).length > 0 ? generatedMap : undefined
    }

    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed
     */
    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: ColumnMetadata[]): ColumnMetadata[] {
        return columnMetadatas.filter(columnMetadata => {
            const tableColumn = tableColumns.find(c => c.name === columnMetadata.databaseName)
            if (!tableColumn)
                return false

            return tableColumn.name !== columnMetadata.databaseName
                || tableColumn.type !== this.normalizeType(columnMetadata)
                || tableColumn.length !== columnMetadata.length
                || tableColumn.precision !== columnMetadata.precision
                || tableColumn.scale !== columnMetadata.scale
                || tableColumn.default !== this.normalizeDefault(columnMetadata)
                || tableColumn.isPrimary !== columnMetadata.isPrimary
                || tableColumn.isNullable !== columnMetadata.isNullable
                || tableColumn.isUnique !== this.normalizeIsUnique(columnMetadata)
                || (tableColumn.enum && columnMetadata.enum && !OrmUtils.isArraysEqual(tableColumn.enum, columnMetadata.enum.map(val => val + "")))
                || tableColumn.isGenerated !== columnMetadata.isGenerated
        })
    }

    /**
     * Required by Driver interface: returns true if driver supports RETURNING / OUTPUT statement
     */
    isReturningSqlSupported(): boolean {
        return true
    }

    /**
     * Required by Driver interface: returns true if driver supports fulltext indices
     */
    isFullTextColumnTypeSupported(): boolean {
        return false
    }

    /**
     * Required by Driver interface: returns true if driver supports uuid values generation on its own
     */
    isUUIDGenerationSupported(): boolean {
        return true
    }

    /**
     * Required by Driver interface: returns true if driver supports creating index on function
     */
    isCreateIndexConcurrentlySupported(): boolean {
        return false
    }

    // /**
    //  * Required by Driver interface: creates a query builder used to build SQL queries
    //  */
    // createQueryBuilder<Entity extends ObjectLiteral = any>(): SelectQueryBuilder<Entity> {
    //     throw new TypeORMError("Query Builder is not supported by DuckDB driver yet.")
    // }

    /**
     * Parses table name and returns database, schema and table name
     */
    parseTableName(
        target: string | Table | View | TableForeignKey | EntityMetadata
    ): { database?: string; schema?: string; tableName: string } {
        let tableName: string

        if (
            target instanceof Table ||
            target instanceof View ||
            target instanceof TableForeignKey
        ) {
            tableName = target.name ?? ""
        } else if ((target as EntityMetadata)?.tableName !== undefined) {
            // Handle EntityMetadata
            tableName = (target as EntityMetadata).tableName
        } else {
            tableName = target as string
        }

        if (tableName.includes(".")) {
            const parts = tableName.split(".")
            if (parts.length === 3) {
                return {
                    database: parts[0],
                    schema: parts[1],
                    tableName: parts[2]
                }
            } else if (parts.length === 2) {
                return {
                    schema: parts[0],
                    tableName: parts[1]
                }
            }
        }

        return {
            tableName: tableName ?? ""
        }
    }

    /**
     * Creates an escaped parameter
     */
    createParameter(parameterName: string, index: number): string {
        return "$" + (index + 1)
    }







    /**
     * Format date to DuckDB format
     */
    protected formatDate(date: Date | string): string {
        if (typeof date === "string") {
            return date
        }
        const year = date.getFullYear()
        const month = String(date.getMonth() + 1).padStart(2, "0")
        const day = String(date.getDate()).padStart(2, "0")
        return `${year}-${month}-${day}`
    }

    /**
     * Format time to DuckDB format
     */
    protected formatTime(date: Date | string): string {
        if (typeof date === "string") {
            return date
        }
        const hours = String(date.getHours()).padStart(2, "0")
        const minutes = String(date.getMinutes()).padStart(2, "0")
        const seconds = String(date.getSeconds()).padStart(2, "0")
        const ms = String(date.getMilliseconds()).padStart(3, "0")
        return `${hours}:${minutes}:${seconds}.${ms}`
    }

    /**
     * Format timestamp to DuckDB format
     */
    protected formatTimestamp(date: Date | string): string {
        if (typeof date === "string") {
            return date
        }
        return `${this.formatDate(date)} ${this.formatTime(date)}`
    }
}

/**
 * Schema builder implementation for DuckDB
 */
class DuckDBSchemaBuilder implements SchemaBuilder {
    protected currentSchema?: string

    // You may need to define a queryRunner property or pass it in the constructor,
    // since SchemaBuilder is now only an interface and doesn't provide it.
    queryRunner: any

    // Implement required build method
    async build(): Promise<void> {
        // DuckDB schema sync is not implemented yet
        // This is a no-op for now
    }

    // Implement required log method
    async log(): Promise<SqlInMemory> {
        // Return an empty SqlInMemory object as no schema sync operations are performed
        return new SqlInMemory()
    }

    async getCurrentDatabase(): Promise<string> {
        const query = `SELECT current_database() as db`
        const result = await this.queryRunner.query(query)
        return result[0]["db"]
    }

    async getCurrentSchema(): Promise<string> {
        const query = `SELECT current_schema() as schema`
        const result = await this.queryRunner.query(query)
        return result[0]["schema"]
    }

    async getSchemas(database?: string): Promise<string[]> {
        throw new TypeORMError("Schema operations are not supported in DuckDB yet")
    }

    async getTable(tableName: string): Promise<Table | undefined> {
        const sql = `SELECT * FROM information_schema.tables WHERE table_name = $1`
        const results = await this.queryRunner.query(sql, [tableName])
        return results.length > 0 ? new Table({ name: tableName }) : undefined
    }

    async getTables(tablePath: string[]): Promise<Table[]> {
        throw new TypeORMError("Not implemented yet")
    }

    async getViews(viewPaths: string[]): Promise<View[]> {
        throw new TypeORMError("Not implemented yet")
    }

    async getForeignKeys(tablePath: string): Promise<TableForeignKey[]> {
        throw new TypeORMError("Not implemented yet")
    }

    async getConstraints(tablePath: string): Promise<any[]> {
        throw new TypeORMError("Not implemented yet")
    }

    protected async loadTables(tableNames?: string[]): Promise<any[]> {
        throw new TypeORMError("Not implemented yet")
    }

    protected async loadViews(viewNames?: string[]): Promise<any[]> {
        throw new TypeORMError("Not implemented yet")
    }


    /**
     * Executes given query on the given connection.
     */
    protected executeQuery(connection: any, query: string): Promise<any> {
        this.connection.logger.logQuery(query)

        return new Promise((resolve, reject) => {
            connection.all(query, (err: any, rows: any[]) => {
                if (err) {
                    this.connection.logger.logQueryError(err, query, [], this)
                    return reject(err)
                }
                resolve(rows)
            })
        })
    }

        /**
     * Escapes a given comment.
     */
    protected escapeComment(comment?: string) {
        if (!comment) return comment

        comment = comment.replace(/\u0000/g, "") // Null bytes aren't allowed in comments

        return comment
    }


    /**
     * Executes given query with parameters on the given connection.
     */
    protected executeQueryWithParameters(
        connection: any,
        query: string,
        parameters: any[]
    ): Promise<any> {
        this.connection.logger.logQuery(query, parameters)

        return new Promise((resolve, reject) => {
            connection.all(query, parameters, (err: any, rows: any[]) => {
                if (err) {
                    this.connection.logger.logQueryError(err, query, parameters, this)
                    return reject(err)
                }
                resolve(rows)
            })
        })
    }


}
