import { QueryRunner } from "../../query-runner/QueryRunner"
import { ObjectLiteral } from "../../common/ObjectLiteral"
import { TransactionAlreadyStartedError } from "../../error/TransactionAlreadyStartedError"
import { TransactionNotStartedError } from "../../error/TransactionNotStartedError"
import { QueryRunnerAlreadyReleasedError } from "../../error/QueryRunnerAlreadyReleasedError"
import { QueryFailedError } from "../../error/QueryFailedError"
import { Table } from "../../schema-builder/table/Table"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { TableUnique } from "../../schema-builder/table/TableUnique"
import { TableCheck } from "../../schema-builder/table/TableCheck"
import { TableExclusion } from "../../schema-builder/table/TableExclusion"
import { View } from "../../schema-builder/view/View"
import { Query } from "../Query"
import { DuckDBDriver } from "./DuckDBDriver"
import { Broadcaster } from "../../subscriber/Broadcaster"
import { BroadcasterResult } from "../../subscriber/BroadcasterResult"
import { ReplicationMode } from "../types/ReplicationMode"
import { TypeORMError } from "../../error"
import { QueryResult } from "../../query-runner/QueryResult"
import { ColumnType } from "../types/ColumnTypes"

export class DuckDBQueryRunner implements QueryRunner {
    driver: DuckDBDriver
    manager: import("../../entity-manager/EntityManager").EntityManager
    broadcaster: Broadcaster
    mode: ReplicationMode
    isReleased = false
    isTransactionActive = false
    protected databaseConnection: any
    protected duckdbConnection: any
    protected transactionDepth = 0
    loadedTables: any[] = []
    loadedViews: any[] = []
    sqlMemoryMode: boolean = false
    sqlInMemory: Query[] = []

    constructor(driver: DuckDBDriver, mode: ReplicationMode = "master") {
        this.driver = driver
        this.connection = driver.connection
        this.broadcaster = new Broadcaster(this)
        this.mode = mode
        this.manager = this.duckdbConnection.manager
    }

       async connect(): Promise<any> {
        if (this.databaseConnection) {
            return this.databaseConnection
        }

        const connection = this.mode === "slave"
            ? await this.driver.obtainSlaveConnection()
            : await this.driver.obtainMasterConnection()

        this.databaseConnection = connection
        this.duckdbConnection = connection

        return this.databaseConnection
    }

     // Make sure you have these methods for memory mode
    enableSqlMemory(): void {
        this.sqlMemoryMode = true
    }

    disableSqlMemory(): void {
        this.sqlMemoryMode = false
    }

 async clearSqlMemory(): Promise<void> {
        this.sqlInMemory = []
    }

    getMemorySql(): Query[] {
        return this.sqlInMemory
    }

       async executeMemoryUpSql(): Promise<void> {
        for (const query of this.sqlInMemory) {
            await this.query(query.query, query.parameters)
        }
    }

    async executeMemoryDownSql(): Promise<void> {
        // Execute in reverse order
        for (const query of this.sqlInMemory.reverse()) {
            await this.query(query.query, query.parameters)
        }
    }


    async release(): Promise<void> {
        this.isReleased = true
        return Promise.resolve()
    }

    async startTransaction(isolationLevel?: string): Promise<void> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError()
        if (this.isTransactionActive)
            throw new TransactionAlreadyStartedError()
        // Broadcaster: BeforeTransactionStart
        const broadcasterResult = new BroadcasterResult()
        this.broadcaster.broadcastBeforeTransactionStartEvent(broadcasterResult)
        if (broadcasterResult.promises.length > 0) {
            await Promise.all(broadcasterResult.promises)
        }
        this.isTransactionActive = true
        this.transactionDepth += 1
        await this.query("BEGIN TRANSACTION")
        // Broadcaster: AfterTransactionStart
        const afterBroadcasterResult = new BroadcasterResult()
        this.broadcaster.broadcastAfterTransactionStartEvent(afterBroadcasterResult)
        if (afterBroadcasterResult.promises.length > 0) {
            await Promise.all(afterBroadcasterResult.promises)
        }
    }

    async commitTransaction(): Promise<void> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError()
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError()
        // Broadcaster: BeforeTransactionCommit
        const beforeBroadcasterResult = new BroadcasterResult()
        this.broadcaster.broadcastBeforeTransactionCommitEvent(beforeBroadcasterResult)
        if (beforeBroadcasterResult.promises.length > 0) {
            await Promise.all(beforeBroadcasterResult.promises)
        }
        await this.query("COMMIT")
        this.transactionDepth -= 1
        if (this.transactionDepth === 0) {
            this.isTransactionActive = false
        }
        // Broadcaster: AfterTransactionCommit
        const afterBroadcasterResult = new BroadcasterResult()
        this.broadcaster.broadcastAfterTransactionCommitEvent(afterBroadcasterResult)
        if (afterBroadcasterResult.promises.length > 0) {
            await Promise.all(afterBroadcasterResult.promises)
        }
    }

    async rollbackTransaction(): Promise<void> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError()
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError()
        // Broadcaster: BeforeTransactionRollback
        const beforeBroadcasterResult = new BroadcasterResult()
        this.broadcaster.broadcastBeforeTransactionRollbackEvent(beforeBroadcasterResult)
        if (beforeBroadcasterResult.promises.length > 0) {
            await Promise.all(beforeBroadcasterResult.promises)
        }
        await this.query("ROLLBACK")
        this.transactionDepth -= 1
        if (this.transactionDepth === 0) {
            this.isTransactionActive = false
        }
        // Broadcaster: AfterTransactionRollback
        const afterBroadcasterResult = new BroadcasterResult()
        this.broadcaster.broadcastAfterTransactionRollbackEvent(afterBroadcasterResult)
        if (afterBroadcasterResult.promises.length > 0) {
            await Promise.all(afterBroadcasterResult.promises)
        }
    }

    async query(query: string, parameters?: any[], useStructuredResult: boolean = false): Promise<any> {
    if (this.isReleased)
        throw new QueryRunnerAlreadyReleasedError()

    const connection = this.duckdbConnection || await this.connect()

    try {
        this.driver.connection.logger.logQuery(query, parameters, this)
        const queryStartTime = Date.now()

        let result: any

        if (parameters && parameters.length > 0) {
            // Use prepared statements for parameterized queries
            const prepared = await new Promise((resolve, reject) => {
                connection.prepare(query, (err: any, statement: any) => {
                    if (err) return reject(err)
                    resolve(statement)
                })
            })

            // Bind parameters
            parameters.forEach((param, index) => {
                this.bindParameter(prepared, index + 1, param)
            })

            // Execute prepared statement
            result = await new Promise((resolve, reject) => {
                prepared.run((err: any, res: any) => {
                    if (err) return reject(err)
                    resolve(res)
                })
            })

            // Finalize prepared statement
            prepared.finalize()
        } else {
            // Execute directly for non-parameterized queries
            result = await new Promise((resolve, reject) => {
                connection.run(query, (err: any, res: any) => {
                    if (err) return reject(err)
                    resolve(res)
                })
            })
        }

        // Get results
        const rows = await new Promise((resolve, reject) => {
            connection.all(query, parameters || [], (err: any, rows: any[]) => {
                if (err) return reject(err)
                resolve(rows)
            })
        })

        const queryEndTime = Date.now()
        const queryExecutionTime = queryEndTime - queryStartTime

        this.driver.connection.logger.logQuerySlow(
            queryExecutionTime,
            query,
            parameters,
            this
        )

        if (useStructuredResult) {
            return {
                records: rows,
                affected: this.getAffectedRows(query, rows),
                raw: rows
            } as QueryResult
        }

        return rows
    } catch (error: any) {
        this.driver.connection.logger.logQueryError(error, query, parameters, this)
        throw new QueryFailedError(query, parameters, error)
    }
}

    protected bindParameter(prepared: any, index: number, value: any): void {
        if (value === null || value === undefined) {
            prepared.bindNull(index)
        } else if (typeof value === "boolean") {
            prepared.bindBoolean(index, value)
        } else if (typeof value === "number") {
            if (Number.isInteger(value)) {
                prepared.bindInteger(index, value)
            } else {
                prepared.bindDouble(index, value)
            }
        } else if (typeof value === "bigint") {
            prepared.bindBigInt(index, value)
        } else if (typeof value === "string") {
            prepared.bindVarchar(index, value)
        } else if (value instanceof Date) {
            prepared.bindTimestamp(index, value)
        } else if (value instanceof Buffer || value instanceof Uint8Array) {
            prepared.bindBlob(index, value)
        } else if (Array.isArray(value)) {
            prepared.bindList(index, value)
        } else {
            prepared.bindVarchar(index, String(value))
        }
    }

    protected getAffectedRows(command: string, rows: any[]): number {
        switch (command) {
            case "INSERT":
            case "UPDATE":
            case "DELETE":
                return rows.length || 1
            default:
                return 0
        }
    }

    async stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<any> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError()
        const connection = await this.connect()
        try {
            let result: any
            if (parameters && parameters.length > 0) {
                const prepared = await connection.prepare(query)
                parameters.forEach((param, index) => {
                    this.bindParameter(prepared, index + 1, param)
                })
                result = await prepared.stream()
            } else {
                result = await connection.stream(query)
            }
            return {
                [Symbol.asyncIterator]: async function* () {
                    try {
                        let chunk
                        while ((chunk = await result.fetchChunk()) && chunk.rowCount > 0) {
                            const rows = chunk.getRowObjects()
                            for (const row of rows) {
                                yield row
                            }
                        }
                        if (onEnd) onEnd()
                    } catch (error) {
                        if (onError) onError(error)
                        throw error
                    }
                }
            }
        } catch (error) {
            if (onError) onError(error)
            throw new QueryFailedError(query, parameters, error)
        }
    }

    /**
     * Returns all available databases
     */
    async getDatabases(): Promise<string[]> {
        // DuckDB doesn't support multiple databases in the same instance
        return [this.driver.database || "main"]
    }



    /**
     * Returns all schemas
     */
    async getSchemas(database?: string): Promise<string[]> {
        const query = `SELECT schema_name FROM information_schema.schemata`
        const schemas = await this.query(query)
        return schemas.map((schema: any) => schema.schema_name)
    }

    /**
     * Checks if database exists
     */
    async hasDatabase(database: string): Promise<boolean> {
        // DuckDB doesn't support checking other databases
        return database === this.driver.database || database === "main"
    }

    /**
     * Loads current database
     */
    async getCurrentDatabase(): Promise<string> {
        const query = `SELECT current_database() as database`
        const result = await this.query(query)
        return result[0].database
    }

    /**
     * Checks if schema exists
     */
    async hasSchema(schema: string): Promise<boolean> {
        const query = `SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1`
        const result = await this.query(query, [schema])
        return result.length > 0
    }

    /**
     * Loads current schema
     */
    async getCurrentSchema(): Promise<string> {
        const query = `SELECT current_schema() as schema`
        const result = await this.query(query)
        return result[0].schema
    }

    /**
     * Checks if table exists
     */
    async hasTable(tableOrName: Table | string): Promise<boolean> {
        const tableName = tableOrName instanceof Table ? tableOrName.name : tableOrName
        const schema = tableOrName instanceof Table ? tableOrName.schema : this.driver.schema

        const query = `
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = $1
            ${schema ? "AND table_schema = $2" : ""}
        `
        const parameters = schema ? [tableName, schema] : [tableName]
        const result = await this.query(query, parameters)
        return result.length > 0
    }

    /**
     * Checks if column exists
     */
    async hasColumn(tableOrName: Table | string, columnName: string): Promise<boolean> {
        const tableName = tableOrName instanceof Table ? tableOrName.name : tableOrName
        const schema = tableOrName instanceof Table ? tableOrName.schema : this.driver.schema

        const query = `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1 AND column_name = $2
            ${schema ? "AND table_schema = $3" : ""}
        `
        const parameters = schema ? [tableName, columnName, schema] : [tableName, columnName]
        const result = await this.query(query, parameters)
        return result.length > 0
    }

    /**
     * Creates a new database
     */
    async createDatabase(database: string, ifNotExist?: boolean): Promise<void> {
        throw new TypeORMError("DuckDB doesn't support creating databases through SQL")
    }

    /**
     * Drops database
     */
    async dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        throw new TypeORMError("DuckDB doesn't support dropping databases through SQL")
    }

    /**
     * Creates new schema
     */
    async createSchema(schemaPath: string, ifNotExist?: boolean): Promise<void> {
        const query = ifNotExist
            ? `CREATE SCHEMA IF NOT EXISTS ${this.driver.escape(schemaPath)}`
            : `CREATE SCHEMA ${this.driver.escape(schemaPath)}`
        await this.query(query)
    }

    /**
     * Drops schema
     */
    async dropSchema(schemaPath: string, ifExist?: boolean, isCascade?: boolean): Promise<void> {
        const query = `DROP SCHEMA ${ifExist ? "IF EXISTS " : ""}${this.driver.escape(schemaPath)}${isCascade ? " CASCADE" : ""}`
        await this.query(query)
    }

    /**
     * Creates a new table
     */
    async createTable(
        table: Table,
        ifNotExist: boolean = false,
        createForeignKeys: boolean = true,
        createIndices: boolean = true
    ): Promise<void> {
        const upQueries: Query[] = []
        const downQueries: Query[] = []

        // Broadcaster: Emit BeforeCreateTableEvent (not supported by TypeORM Broadcaster, remove)
        // Instead, use entity listeners or subscribers if needed

        upQueries.push(this.createTableSql(table, ifNotExist))
        downQueries.push(this.dropTableSql(table))

        // Create indices
        if (createIndices) {
            table.indices.forEach(index => {
                // Skip primary key indices
                if (!index.isUnique) {
                    upQueries.push(this.createIndexSql(table, index))
                    downQueries.push(this.dropIndexSql(table, index))
                }
            })
        }

        // Create foreign keys
        if (createForeignKeys) {
            table.foreignKeys.forEach(foreignKey => {
                upQueries.push(this.createForeignKeySql(table, foreignKey))
                downQueries.push(this.dropForeignKeySql(table, foreignKey))
            })
        }

        // Execute queries
        for (const query of upQueries) {
            await this.query(query.query, query.parameters)
        }

        // Broadcaster: Emit AfterCreateTableEvent (not supported by TypeORM Broadcaster, remove)
    }

    /**
     * Drops a table
     */
    async dropTable(
        targetOrName: Table | string,
        ifExist?: boolean,
        dropForeignKeys: boolean = true,
        dropIndices: boolean = true
    ): Promise<void> {
        const table = targetOrName instanceof Table ? targetOrName : new Table({ name: targetOrName })

        // Broadcaster: Emit BeforeDropTableEvent (not supported by TypeORM Broadcaster, remove)

        const query = this.dropTableSql(table, ifExist)
        await this.query(query.query, query.parameters)

        // Broadcaster: Emit AfterDropTableEvent (not supported by TypeORM Broadcaster, remove)
    }

    /**
     * Creates a new view
     */
    async createView(view: View, syncWithMetadata?: boolean): Promise<void> {
        const query = this.createViewSql(view)
        await this.query(query.query, query.parameters)
    }

    /**
     * Drops a view
     */
    async dropView(target: View | string): Promise<void> {
        const viewName = target instanceof View ? target.name : target
        const query = `DROP VIEW ${this.driver.escape(viewName)}`
        await this.query(query)
    }

    /**
     * Renames a table
     */
    async renameTable(oldTableOrName: Table | string, newTableName: string): Promise<void> {
        const oldTableName = oldTableOrName instanceof Table ? oldTableOrName.name : oldTableOrName
        const query = `ALTER TABLE ${this.driver.escape(oldTableName)} RENAME TO ${this.driver.escape(newTableName)}`
        await this.query(query)
    }

    /**
     * Adds new column
     */
    async addColumn(tableOrName: Table | string, column: TableColumn): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const query = this.addColumnSql(table, column)
        await this.query(query.query, query.parameters)
    }

    /**
     * Adds new columns
     */
    async addColumns(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        for (const column of columns) {
            await this.addColumn(tableOrName, column)
        }
    }

    /**
     * Renames a column
     */
    async renameColumn(tableOrName: Table | string, oldColumnName: string, newColumnName: string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} RENAME COLUMN ${this.driver.escape(oldColumnName)} TO ${this.driver.escape(newColumnName)}`
        await this.query(query)
    }

    /**
     * Changes a column type
     */
    async changeColumn(
        tableOrName: Table | string,
        oldColumn: TableColumn,
        newColumn: TableColumn
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })

        if (oldColumn.name !== newColumn.name) {
            await this.renameColumn(table, oldColumn.name, newColumn.name)
        }

        if (oldColumn.type !== newColumn.type || oldColumn.length !== newColumn.length ||
            oldColumn.precision !== newColumn.precision || oldColumn.scale !== newColumn.scale) {
            const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} ALTER COLUMN ${this.driver.escape(newColumn.name)} TYPE ${this.driver.createFullType(newColumn)}`
            await this.query(query)
        }

        if (oldColumn.default !== newColumn.default) {
            if (newColumn.default !== null && newColumn.default !== undefined) {
                const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} ALTER COLUMN ${this.driver.escape(newColumn.name)} SET DEFAULT ${newColumn.default}`
                await this.query(query)
            } else {
                const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} ALTER COLUMN ${this.driver.escape(newColumn.name)} DROP DEFAULT`
                await this.query(query)
            }
        }

        if (oldColumn.isNullable !== newColumn.isNullable) {
            const query = newColumn.isNullable
                ? `ALTER TABLE ${this.driver.buildTableName(table.name)} ALTER COLUMN ${this.driver.escape(newColumn.name)} DROP NOT NULL`
                : `ALTER TABLE ${this.driver.buildTableName(table.name)} ALTER COLUMN ${this.driver.escape(newColumn.name)} SET NOT NULL`
            await this.query(query)
        }
    }

    /**
     * Changes columns
     */
    async changeColumns(
        tableOrName: Table | string,
        changedColumns: { oldColumn: TableColumn; newColumn: TableColumn }[]
    ): Promise<void> {
        for (const { oldColumn, newColumn } of changedColumns) {
            await this.changeColumn(tableOrName, oldColumn, newColumn)
        }
    }

    /**
     * Drops a column
     */
    async dropColumn(tableOrName: Table | string, columnOrName: TableColumn | string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const columnName = columnOrName instanceof TableColumn ? columnOrName.name : columnOrName
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} DROP COLUMN ${this.driver.escape(columnName)}`
        await this.query(query)
    }

    /**
     * Drops columns
     */
    async dropColumns(tableOrName: Table | string, columns: TableColumn[] | string[]): Promise<void> {
        for (const column of columns) {
            await this.dropColumn(tableOrName, column)
        }
    }

    /**
     * Creates a primary key
     */
    async createPrimaryKey(
        tableOrName: Table | string,
        columns: string[],
        constraintName?: string
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const columnNames = columns.map(column => this.driver.escape(column)).join(", ")
        const name = constraintName || this.connection.namingStrategy.primaryKeyName(table, columns)
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} ADD CONSTRAINT ${this.driver.escape(name)} PRIMARY KEY (${columnNames})`
        await this.query(query)
    }

    /**
     * Updates a primary key
     */
    async updatePrimaryKeys(tableOrName: Table | string, columns: TableColumn[]): Promise<void> {
        // DuckDB requires dropping and recreating primary keys
        throw new TypeORMError("Updating primary keys is not supported in DuckDB. Drop and recreate the primary key instead.")
    }

    /**
     * Drops a primary key
     */
    async dropPrimaryKey(tableOrName: Table | string, constraintName?: string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const name = constraintName || this.connection.namingStrategy.primaryKeyName(table, [])
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} DROP CONSTRAINT ${this.driver.escape(name)}`
        await this.query(query)
    }

    /**
     * Creates unique constraint
     */
    async createUniqueConstraint(
        tableOrName: Table | string,
        uniqueConstraint: TableUnique
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const columnNames = uniqueConstraint.columnNames.map(column => this.driver.escape(column)).join(", ")
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} ADD CONSTRAINT ${this.driver.escape(uniqueConstraint.name!)} UNIQUE (${columnNames})`
        await this.query(query)
    }

    /**
     * Creates unique constraints
     */
    async createUniqueConstraints(
        tableOrName: Table | string,
        uniqueConstraints: TableUnique[]
    ): Promise<void> {
        for (const uniqueConstraint of uniqueConstraints) {
            await this.createUniqueConstraint(tableOrName, uniqueConstraint)
        }
    }

    /**
     * Drops unique constraint
     */
    async dropUniqueConstraint(
        tableOrName: Table | string,
        uniqueOrName: TableUnique | string
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const name = uniqueOrName instanceof TableUnique ? uniqueOrName.name! : uniqueOrName
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} DROP CONSTRAINT ${this.driver.escape(name)}`
        await this.query(query)
    }

    /**
     * Drops unique constraints
     */
    async dropUniqueConstraints(
        tableOrName: Table | string,
        uniqueConstraints: TableUnique[]
    ): Promise<void> {
        for (const uniqueConstraint of uniqueConstraints) {
            await this.dropUniqueConstraint(tableOrName, uniqueConstraint)
        }
    }

    /**
     * Creates check constraint
     */
    async createCheckConstraint(
        tableOrName: Table | string,
        checkConstraint: TableCheck
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} ADD CONSTRAINT ${this.driver.escape(checkConstraint.name!)} CHECK (${checkConstraint.expression})`
        await this.query(query)
    }

    /**
     * Creates check constraints
     */
    async createCheckConstraints(
        tableOrName: Table | string,
        checkConstraints: TableCheck[]
    ): Promise<void> {
        for (const checkConstraint of checkConstraints) {
            await this.createCheckConstraint(tableOrName, checkConstraint)
        }
    }

    /**
     * Drops check constraint
     */
    async dropCheckConstraint(
        tableOrName: Table | string,
        checkOrName: TableCheck | string
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const name = checkOrName instanceof TableCheck ? checkOrName.name! : checkOrName
        const query = `ALTER TABLE ${this.driver.buildTableName(table.name)} DROP CONSTRAINT ${this.driver.escape(name)}`
        await this.query(query)
    }

    /**
     * Drops check constraints
     */
    async dropCheckConstraints(
        tableOrName: Table | string,
        checkConstraints: TableCheck[]
    ): Promise<void> {
        for (const checkConstraint of checkConstraints) {
            await this.dropCheckConstraint(tableOrName, checkConstraint)
        }
    }

    /**
     * Creates exclusion constraint
     */
    async createExclusionConstraint(
        tableOrName: Table | string,
        exclusionConstraint: TableExclusion
    ): Promise<void> {
        throw new TypeORMError("DuckDB does not support exclusion constraints")
    }

    /**
     * Creates exclusion constraints
     */
    async createExclusionConstraints(
        tableOrName: Table | string,
        exclusionConstraints: TableExclusion[]
    ): Promise<void> {
        throw new TypeORMError("DuckDB does not support exclusion constraints")
    }

    /**
     * Drops exclusion constraint
     */
    async dropExclusionConstraint(
        tableOrName: Table | string,
        exclusionOrName: TableExclusion | string
    ): Promise<void> {
        throw new TypeORMError("DuckDB does not support exclusion constraints")
    }

    /**
     * Drops exclusion constraints
     */
    async dropExclusionConstraints(
        tableOrName: Table | string,
        exclusionConstraints: TableExclusion[]
    ): Promise<void> {
        throw new TypeORMError("DuckDB does not support exclusion constraints")
    }

    /**
     * Creates foreign key
     */
    async createForeignKey(
        tableOrName: Table | string,
        foreignKey: TableForeignKey
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        // Ensure foreignKey.name is set
        if (!foreignKey.name) {
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(
                table,
                foreignKey.columnNames,
                foreignKey.referencedTableName,
                foreignKey.referencedColumnNames
            )
        }
        const query = this.createForeignKeySql(table, foreignKey)
        await this.query(query.query, query.parameters)
    }

    /**
     * Creates foreign keys
     */
    async createForeignKeys(
        tableOrName: Table | string,
        foreignKeys: TableForeignKey[]
    ): Promise<void> {
        for (const foreignKey of foreignKeys) {
            await this.createForeignKey(tableOrName, foreignKey)
        }
    }

    /**
     * Drops foreign key
     */
    async dropForeignKey(
        tableOrName: Table | string,
        foreignKeyOrName: TableForeignKey | string
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        let foreignKey: TableForeignKey
        if (foreignKeyOrName instanceof TableForeignKey) {
            foreignKey = foreignKeyOrName
        } else {
            // Find the foreign key by name if only the name is provided
            // This requires loading the table's foreign keys, but for brevity, we just construct a new one
            foreignKey = new TableForeignKey({ name: foreignKeyOrName })
        }
        if (!foreignKey.name) {
            // If name is still not set, try to generate it
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(
                table,
                foreignKey.columnNames || [],
                foreignKey.referencedTableName || "",
                foreignKey.referencedColumnNames || []
            )
        }
        const query = this.dropForeignKeySql(table, foreignKey)
        await this.query(query.query, query.parameters)
    }

    /**
     * Drops foreign keys
     */
    async dropForeignKeys(
        tableOrName: Table | string,
        foreignKeys: TableForeignKey[]
    ): Promise<void> {
        for (const foreignKey of foreignKeys) {
            await this.dropForeignKey(tableOrName, foreignKey)
        }
    }

    /**
     * Creates index
     */
    async createIndex(
        tableOrName: Table | string,
        index: TableIndex
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const query = this.createIndexSql(table, index)
        await this.query(query.query, query.parameters)
    }

    /**
     * Creates indices
     */
    async createIndices(
        tableOrName: Table | string,
        indices: TableIndex[]
    ): Promise<void> {
        for (const index of indices) {
            await this.createIndex(tableOrName, index)
        }
    }

    /**
     * Drops index
     */
    async dropIndex(
        tableOrName: Table | string,
        indexOrName: TableIndex | string
    ): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : new Table({ name: tableOrName })
        const index = indexOrName instanceof TableIndex ? indexOrName : new TableIndex({ name: indexOrName })
        const query = this.dropIndexSql(table, index)
        await this.query(query.query, query.parameters)
    }

    /**
     * Drops indices
     */
    async dropIndices(
        tableOrName: Table | string,
        indices: TableIndex[]
    ): Promise<void> {
        for (const index of indices) {
            await this.dropIndex(tableOrName, index)
        }
    }

    /**
     * Clears table
     */
    async clearTable(tableName: string): Promise<void> {
        await this.query(`DELETE FROM ${this.driver.escape(tableName)}`)
    }

    /**
     * Clears database
     */
    async clearDatabase(): Promise<void> {
        const tables = await this.getTables([])
        for (const table of tables) {
            await this.dropTable(table, true, true, true)
        }
    }

    /**
     * Gets table from database
     */
    async getTable(tableName: string): Promise<Table | undefined> {
        const tables = await this.getTables([tableName])
        return tables.length > 0 ? tables[0] : undefined
    }

    /**
     * Gets all tables from database
     */
    async getTables(tableNames?: string[]): Promise<Table[]> {
        if (!tableNames || tableNames.length === 0) {
            tableNames = []
        }

        const tablesSql = `
            SELECT
                t.table_name,
                t.table_schema
            FROM information_schema.tables t
            WHERE t.table_type = 'BASE TABLE'
            ${tableNames.length > 0 ? `AND t.table_name IN (${tableNames.map(name => `'${name}'`).join(",")})` : ""}
        `

        const tables: Table[] = []
        const dbTables = await this.query(tablesSql)

        if (dbTables.length === 0) {
            return tables
        }

        for (const dbTable of dbTables) {
            const table = new Table()
            table.database = this.driver.database
            table.schema = dbTable["table_schema"]
            table.name = dbTable["table_name"]

            // Get columns
            const columnsSql = `
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns
                WHERE table_name = '${table.name}'
                AND table_schema = '${table.schema}'
                ORDER BY ordinal_position
            `

            const dbColumns = await this.query(columnsSql)
            table.columns = dbColumns.map((dbColumn: any) => {
                const tableColumn = new TableColumn()
                tableColumn.name = dbColumn["column_name"]
                tableColumn.type = this.normalizeDataType(dbColumn["data_type"])
                tableColumn.length = dbColumn["character_maximum_length"]
                tableColumn.precision = dbColumn["numeric_precision"]
                tableColumn.scale = dbColumn["numeric_scale"]
                tableColumn.default = dbColumn["column_default"]
                tableColumn.isNullable = dbColumn["is_nullable"] === "YES"
                return tableColumn
            })

            // Get primary keys
            const pkSql = `
                SELECT
                    tc.constraint_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = '${table.name}'
                AND tc.table_schema = '${table.schema}'
                AND tc.constraint_type = 'PRIMARY KEY'
            `

            const dbPrimaryKeys = await this.query(pkSql)
            for (const pk of dbPrimaryKeys) {
                const column = table.columns.find(c => c.name === pk["column_name"])
                if (column) {
                    column.isPrimary = true
                }
            }

            tables.push(table)
        }

        return tables
    }

    /**
     * Gets all views from database
     */
    async getViews(viewNames?: string[]): Promise<View[]> {
        const hasViewNames = viewNames && viewNames.length > 0
        const viewsSql = `
            SELECT
                table_name as view_name,
                view_definition as definition
            FROM information_schema.views
            ${hasViewNames ? `WHERE table_name IN (${viewNames!.map(name => `'${name}'`).join(",")})` : ""}
        `

        const dbViews = await this.query(viewsSql)
        return dbViews.map((dbView: any) => {
            const view = new View()
            view.name = dbView["view_name"]
            view.expression = dbView["definition"]
            return view
        })
    }

    /**
     * Loads all tables and views
     */
    protected async loadTables(tableNames?: string[]): Promise<Table[]> {
        return this.getTables(tableNames)
    }

    /**
     * Builds create table sql
     */
    protected createTableSql(table: Table, ifNotExist?: boolean): Query {
        const columnDefinitions = table.columns.map(column => {
            const columnDef = `${this.driver.escape(column.name)} ${this.driver.createFullType(column)}`
            const parts = [columnDef]

            if (column.isPrimary) {
                parts.push("PRIMARY KEY")
            }

            if (!column.isNullable) {
                parts.push("NOT NULL")
            }

            if (column.default !== null && column.default !== undefined) {
                parts.push(`DEFAULT ${column.default}`)
            }

            if (column.isUnique) {
                parts.push("UNIQUE")
            }

            return parts.join(" ")
        }).join(", ")

        const sql = `CREATE TABLE ${ifNotExist ? "IF NOT EXISTS " : ""}${this.driver.buildTableName(table.name, table.schema)} (${columnDefinitions})`

        return new Query(sql)
    }

    /**
     * Builds drop table sql
     */
    protected dropTableSql(table: Table, ifExist?: boolean): Query {
        const sql = `DROP TABLE ${ifExist ? "IF EXISTS " : ""}${this.driver.buildTableName(table.name, table.schema)}`
        return new Query(sql)
    }

    /**
     * Builds create index sql
     */
    protected createIndexSql(table: Table, index: TableIndex): Query {
        const columns = index.columnNames.map(columnName => this.driver.escape(columnName)).join(", ")
        const indexName = index.name || this.connection.namingStrategy.indexName(table, index.columnNames, index.where)

        let sql = "CREATE "
        if (index.isUnique) sql += "UNIQUE "
        sql += `INDEX ${this.driver.escape(indexName)} ON ${this.driver.buildTableName(table.name, table.schema)} (${columns})`

        if (index.where) {
            sql += ` WHERE ${index.where}`
        }

        return new Query(sql)
    }

    /**
     * Builds drop index sql
     */
    protected dropIndexSql(table: Table, index: TableIndex): Query {
        const indexName = index.name || this.connection.namingStrategy.indexName(table, index.columnNames, index.where)
        const sql = `DROP INDEX ${this.driver.escape(indexName)}`
        return new Query(sql)
    }

    /**
     * Builds create foreign key sql
     */
    protected createForeignKeySql(table: Table, foreignKey: TableForeignKey): Query {
        // Ensure foreignKey.name is set
        if (!foreignKey.name) {
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(
                table,
                foreignKey.columnNames,
                foreignKey.referencedTableName,
                foreignKey.referencedColumnNames
            )
        }
        const columnNames = foreignKey.columnNames.map(column => this.driver.escape(column)).join(", ")
        const referencedColumnNames = foreignKey.referencedColumnNames.map(column => this.driver.escape(column)).join(", ")
        const referencedTableName = this.driver.buildTableName(foreignKey.referencedTableName, table.schema)

        let sql = `ALTER TABLE ${this.driver.buildTableName(table.name, table.schema)} `
        sql += `ADD CONSTRAINT ${this.driver.escape(foreignKey.name)} `
        sql += `FOREIGN KEY (${columnNames}) `
        sql += `REFERENCES ${referencedTableName} (${referencedColumnNames})`

        if (foreignKey.onDelete) {
            sql += ` ON DELETE ${foreignKey.onDelete}`
        }

        if (foreignKey.onUpdate) {
            sql += ` ON UPDATE ${foreignKey.onUpdate}`
        }

        return new Query(sql)
    }

    /**
     * Builds drop foreign key sql
     */
    protected dropForeignKeySql(table: Table, foreignKey: TableForeignKey): Query {
        // Ensure foreignKey.name is set
        if (!foreignKey.name) {
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(
                table,
                foreignKey.columnNames || [],
                foreignKey.referencedTableName || "",
                foreignKey.referencedColumnNames || []
            )
        }
        const sql = `ALTER TABLE ${this.driver.buildTableName(table.name, table.schema)} DROP CONSTRAINT ${this.driver.escape(foreignKey.name)}`
        return new Query(sql)
    }

    /**
     * Builds add column sql
     */
    protected addColumnSql(table: Table, column: TableColumn): Query {
        let sql = `ALTER TABLE ${this.driver.buildTableName(table.name, table.schema)} ADD COLUMN ${this.driver.escape(column.name)} ${this.driver.createFullType(column)}`

        if (!column.isNullable) {
            sql += " NOT NULL"
        }

        if (column.default !== null && column.default !== undefined) {
            sql += ` DEFAULT ${column.default}`
        }

        if (column.isUnique) {
            sql += " UNIQUE"
        }

        return new Query(sql)
    }

    /**
     * Builds create view sql
     */
    protected createViewSql(view: View): Query {
        const materializedClause = view.materialized ? "MATERIALIZED " : ""
        const sql = `CREATE ${materializedClause}VIEW ${this.driver.escape(view.name)} AS ${view.expression}`
        return new Query(sql)
    }

    /**
     * Normalizes data type from database
     */
    protected normalizeDataType(type: string): ColumnType {
        switch (type.toLowerCase()) {
            case "int":
            case "integer":
            case "int4":
                return "integer"
            case "bigint":
            case "int8":
                return "bigint"
            case "smallint":
            case "int2":
                return "smallint"
            case "tinyint":
            case "int1":
                return "tinyint"
            case "decimal":
            case "numeric":
                return "decimal"
            case "float":
            case "float4":
            case "real":
                return "float"
            case "double":
            case "double precision":
            case "float8":
                return "double"
            case "boolean":
            case "bool":
                return "boolean"
            case "varchar":
            case "character varying":
            case "text":
            case "string":
                return "varchar"
            case "date":
                return "date"
            case "time":
                return "time"
            case "timestamp":
            case "datetime":
                return "timestamp"
            case "timestamp with time zone":
            case "timestamptz":
                return "timestamp with time zone"
            case "uuid":
                return "uuid"
            case "json":
                return "json"
            case "blob":
            case "bytea":
                return "blob"
            case "bit":
            case "bitstring":
                return "bit"
            case "interval":
                return "interval"
            case "hugeint":
                return "hugeint"
            default:
                return type as ColumnType
        }
    }

}

