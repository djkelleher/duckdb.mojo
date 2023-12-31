from os import getenv, setenv
from sys.ffi import DLHandle, RTLD
from duckdb.utils import str_to_char_ptr, char_ptr_to_str
from duckdb.ctypes import void, void_ptr, char_ptr, idx_t

alias duckdb_state = Int32
alias DuckDBSuccess = 0
alias DuckDBError = 1


fn load_libduckdb(lib_path: StringRef) -> DLHandle:
    print("Loading libduckdb 🦆 ", lib_path)
    return DLHandle(lib_path, RTLD.LAZY)


struct LibDuckDB:
    var libcfg: LibCfg
    var libdb: LibDB
    var libconn: LibConn
    var libresult: LibResult

    fn __init__(inout self) raises:
        let libduckdb_path = getenv("LIBDUCKDB")
        let libduckdb = load_libduckdb(libduckdb_path)
        if not libduckdb.handle:
            raise Error("❌COULD NOT LOAD LIBDUCKDB!: " + String(libduckdb_path))
        print("✅Loaded libduckdb ", libduckdb_path)
        self.libcfg = LibCfg(libduckdb)
        self.libdb = LibDB(libduckdb)
        self.libconn = LibConn(libduckdb)
        self.libresult = LibResult(libduckdb)


@value
struct LibCfg:
    """Configuration"""

    alias duckdb_create_config_sig = fn (Pointer[void_ptr]) -> duckdb_state
    alias duckdb_config_count_sig = fn () -> Int
    alias duckdb_get_config_flag_sig = fn (
        Int, Pointer[char_ptr], Pointer[char_ptr]
    ) -> duckdb_state
    alias duckdb_set_config_sig = fn (void_ptr, char_ptr, char_ptr) -> duckdb_state
    alias duckdb_destroy_config_sig = fn (void_ptr) -> void
    """
    Initializes an empty configuration object that can be used to provide start-up options for the DuckDB instance
    through `duckdb_open_ext`.

    This will always succeed unless there is a malloc failure.

    * out_config: The result configuration object.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_create_config: LibCfg.duckdb_create_config_sig

    """
    This returns the total amount of configuration options available for usage with `duckdb_get_config_flag`.

    This should not be called in a loop as it internally loops over all the options.

    * returns: The amount of config options available.
    """
    var duckdb_config_count: LibCfg.duckdb_config_count_sig

    """
    Obtains a human-readable name and description of a specific configuration option. This can be used to e.g.
    display configuration options. This will succeed unless `index` is out of range (i.e. `>= duckdb_config_count`).

    The result name or description MUST NOT be freed.

    * index: The index of the configuration option (between 0 and `duckdb_config_count`)
    * out_name: A name of the configuration flag.
    * out_description: A description of the configuration flag.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_get_config_flag: LibCfg.duckdb_get_config_flag_sig

    """
    Sets the specified option for the specified configuration. The configuration option is indicated by name.
    To obtain a list of config options, see `duckdb_get_config_flag`.

    In the source code, configuration options are defined in `config.cpp`.

    This can fail if either the name is invalid, or if the value provided for the option is invalid.

    * void_ptr: The configuration object to set the option on.
    * name: The name of the configuration flag to set.
    * option: The value to set the configuration flag to.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_set_config: LibCfg.duckdb_set_config_sig

    """
    Destroys the specified configuration option and de-allocates all memory allocated for the object.

    * config: The configuration object to destroy.
    """
    var duckdb_destroy_config: LibCfg.duckdb_destroy_config_sig

    fn __init__(inout self, libduckdb: DLHandle):
        self.duckdb_create_config = libduckdb.get_function[
            self.duckdb_create_config_sig
        ]("duckdb_create_config")
        self.duckdb_config_count = libduckdb.get_function[self.duckdb_config_count_sig](
            "duckdb_config_count"
        )
        self.duckdb_get_config_flag = libduckdb.get_function[
            self.duckdb_get_config_flag_sig
        ]("duckdb_get_config_flag")
        self.duckdb_set_config = libduckdb.get_function[self.duckdb_set_config_sig](
            "duckdb_set_config"
        )
        self.duckdb_destroy_config = libduckdb.get_function[
            self.duckdb_destroy_config_sig
        ]("duckdb_destroy_config")


@value
struct LibDB:
    """Databases"""

    alias duckdb_open_sig = fn (char_ptr, Pointer[void_ptr]) -> duckdb_state
    alias duckdb_open_ext_sig = fn (
        char_ptr,
        Pointer[void_ptr],
        void_ptr,
        Pointer[char_ptr],
    ) -> duckdb_state
    alias duckdb_close_sig = fn (Pointer[void_ptr]) -> void
    """
    Creates a new database or opens an existing database file stored at the given path.
    If no path is given a new in-memory database is created instead.
    The instantiated database should be closed with 'duckdb_close'

    * path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
    * out_database: The result database object.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_open: LibDB.duckdb_open_sig

    """
    Extended version of duckdb_open. Creates a new database or opens an existing database file stored at the given path.

    * path: Path to the database file on disk, or `nullptr` or `:memory:` to open an in-memory database.
    * out_database: The result database object.
    * config: (Optional) configuration used to start up the database system.
    * out_error: If set and the function returns DuckDBError, this will contain the reason why the start-up failed.
    Note that the error must be freed using `duckdb_free`.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_open_ext: LibDB.duckdb_open_ext_sig

    """
    Closes the specified database and de-allocates all memory allocated for that database.
    This should be called after you are done with any database allocated through `duckdb_open`.
    Note that failing to call `duckdb_close` (in case of e.g. a program crash) will not cause data corruption.
    Still it is recommended to always correctly close a database object after you are done with it.

    * database: The database object to shut down.
    """
    var duckdb_close: LibDB.duckdb_close_sig

    fn __init__(inout self, libduckdb: DLHandle):
        self.duckdb_open = libduckdb.get_function[self.duckdb_open_sig]("duckdb_open")
        self.duckdb_open_ext = libduckdb.get_function[self.duckdb_open_ext_sig](
            "duckdb_open_ext"
        )
        self.duckdb_close = libduckdb.get_function[self.duckdb_close_sig](
            "duckdb_close"
        )


@value
struct LibConn:
    """Connections"""

    alias duckdb_connect_sig = fn (void_ptr, Pointer[void_ptr]) -> duckdb_state
    alias duckdb_disconnect_sig = fn (Pointer[void_ptr]) -> void
    alias duckdb_query_sig = fn (
        void_ptr, char_ptr, AnyPointer[duckdb_result]
    ) -> duckdb_state
    """
    Opens a connection to a database. Connections are required to query the database, and store transactional state
    associated with the connection.
    The instantiated connection should be closed using 'duckdb_disconnect'

    * database: The database file to connect to.
    * out_connection: The result connection object.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_connect: LibConn.duckdb_connect_sig

    """
    Closes the specified connection and de-allocates all memory allocated for that connection.

    * connection: The connection to close.
    """
    var duckdb_disconnect: LibConn.duckdb_disconnect_sig

    """
    Executes a SQL query within a connection and stores the full (materialized) result in the out_result pointer.
    If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling
    `duckdb_result_error`.

    Note that after running `duckdb_query`, `duckdb_destroy_result` must be called on the result object even if the
    query fails, otherwise the error stored within the result will not be freed correctly.

    * connection: The connection to perform the query in.
    * query: The SQL query to run.
    * out_result: The query result.
    * returns: `DuckDBSuccess` on success or `DuckDBError` on failure.
    """
    var duckdb_query: LibConn.duckdb_query_sig

    fn __init__(inout self, libduckdb: DLHandle):
        self.duckdb_connect = libduckdb.get_function[self.duckdb_connect_sig](
            "duckdb_connect"
        )
        self.duckdb_disconnect = libduckdb.get_function[self.duckdb_disconnect_sig](
            "duckdb_disconnect"
        )
        self.duckdb_query = libduckdb.get_function[self.duckdb_query_sig](
            "duckdb_query"
        )


@value
struct LibResult:
    """Query Results"""

    # TODO factor out LibChunk, LibVector?

    alias duckdb_column_name_sig = fn (AnyPointer[duckdb_result], idx_t) -> char_ptr
    # alias duckdb_column_type_sig = fn (AnyPointer[duckdb_result], idx_t) -> duckdb_type
    alias duckdb_column_type_sig = fn (AnyPointer[duckdb_result], idx_t) -> Int
    # alias duckdb_column_logical_type_sig = fn (AnyPointer[duckdb_result], idx_t) -> duckdb_logical_type
    alias duckdb_column_logical_type_sig = fn (
        AnyPointer[duckdb_result], idx_t
    ) -> void_ptr
    alias duckdb_column_count_sig = fn (AnyPointer[duckdb_result]) -> idx_t
    alias duckdb_row_count_sig = fn (AnyPointer[duckdb_result]) -> idx_t
    alias duckdb_rows_changed_sig = fn (AnyPointer[duckdb_result]) -> idx_t
    alias duckdb_result_get_chunk_sig = fn (duckdb_result, idx_t) -> Pointer[void_ptr]
    alias duckdb_result_is_streaming_sig = fn (duckdb_result) -> Bool
    alias duckdb_result_chunk_count_sig = fn (duckdb_result) -> idx_t
    alias duckdb_result_error_sig = fn (AnyPointer[duckdb_result]) -> char_ptr
    alias duckdb_destroy_result_sig = fn (AnyPointer[duckdb_result]) -> void
    alias duckdb_destroy_data_chunk_sig = fn (Pointer[void_ptr]) -> void
    alias duckdb_data_chunk_get_size_sig = fn (Pointer[void_ptr]) -> idx_t
    alias duckdb_free_sig = fn (char_ptr) -> void
    alias duckdb_data_chunk_get_column_count_sig = fn (void_ptr) -> idx_t
    alias duckdb_data_chunk_get_vector_sig = fn (void_ptr, idx_t) -> void_ptr
    alias duckdb_vector_get_column_type_sig = fn (void_ptr) -> void_ptr
    alias duckdb_vector_get_data_sig = fn (void_ptr) -> void_ptr
    alias duckdb_vector_get_validity_sig = fn (void_ptr) -> UInt64

    """
    Returns the column name of the specified column. The result should not need be freed; the column names will
    automatically be destroyed when the result is destroyed.

    Returns `NULL` if the column is out of range.

    * result: The result object to fetch the column name from.
    * col: The column index.
    * returns: The column name of the specified column.
    """
    var duckdb_column_name: LibResult.duckdb_column_name_sig

    """
    Returns the column type of the specified column.

    Returns `DUCKDB_TYPE_INVALID` if the column is out of range.

    * r0esult: The result object to fetch the column type from.
    * col: The column index.
    * returns: The column type of the specified column.
    """
    var duckdb_column_type: LibResult.duckdb_column_type_sig

    """
    Returns the logical column type of the specified column.

    The return type of this call should be destroyed with `duckdb_destroy_logical_type`.

    Returns `NULL` if the column is out of range.

    * result: The result object to fetch the column type from.
    * col: The column index.
    * returns: The logical column type of the specified column.
    """
    var duckdb_column_logical_type: LibResult.duckdb_column_logical_type_sig

    """
    Returns the number of columns present in a the result object.

    * result: The result object.
    * returns: The number of columns present in the result object.
    """
    var duckdb_column_count: LibResult.duckdb_column_count_sig

    """
    Returns the number of rows present in a the result object.

    * result: The result object.
    * returns: The number of rows present in the result object.
    """
    var duckdb_row_count: LibResult.duckdb_row_count_sig

    """
    Returns the number of rows changed by the query stored in the result. This is relevant only for INSERT/UPDATE/DELETE
    queries. For other queries the rows_changed will be 0.

    * result: The result object.
    * returns: The number of rows changed.
    """
    var duckdb_rows_changed: LibResult.duckdb_rows_changed_sig

    """
    Fetches a data chunk from the duckdb_result. This function should be called repeatedly until the result is exhausted.

    The result must be destroyed with `duckdb_destroy_data_chunk`.

    This function supersedes all `duckdb_value` functions, as well as the `duckdb_column_data` and `duckdb_nullmask_data`
    functions. It results in significantly better performance, and should be preferred in newer code-bases.

    If this function is used, none of the other result functions can be used and vice versa (i.e. this function cannot be
    mixed with the legacy result functions).

    Use `duckdb_result_chunk_count` to figure out how many chunks there are in the result.

    * result: The result object to fetch the data chunk from.
    * chunk_index: The chunk index to fetch from.
    * returns: The resulting data chunk. Returns `NULL` if the chunk index is out of bounds.
    """
    var duckdb_result_get_chunk: LibResult.duckdb_result_get_chunk_sig

    """
    Checks if the type of the internal result is StreamQueryResult.

    * result: The result object to check.
    * returns: Whether or not the result object is of the type StreamQueryResult
    """
    var duckdb_result_is_streaming: LibResult.duckdb_result_is_streaming_sig

    """
    Returns the number of data chunks present in the result.

    * result: The result object
    * returns: Number of data chunks present in the result.
    """
    var duckdb_result_chunk_count: LibResult.duckdb_result_chunk_count_sig

    """
    Retrieves the number of columns in a data chunk.

    * chunk: The data chunk to get the data from
    * returns: The number of columns in the data chunk
    """
    var duckdb_data_chunk_get_column_count: LibResult.duckdb_data_chunk_get_column_count_sig

    """
    Retrieves the current number of tuples in a data chunk.

    * chunk: The data chunk to get the data from
    * returns: The number of tuples in the data chunk
    """
    var duckdb_data_chunk_get_size: LibResult.duckdb_data_chunk_get_size_sig

    """
    Retrieves the vector at the specified column index in the data chunk.

    The pointer to the vector is valid for as long as the chunk is alive.
    It does NOT need to be destroyed.

    * chunk: The data chunk to get the data from
    * returns: The vector
    """
    var duckdb_data_chunk_get_vector: LibResult.duckdb_data_chunk_get_vector_sig

    """
    Retrieves the column type of the specified vector.

    The result must be destroyed with `duckdb_destroy_logical_type`.

    * vector: The vector get the data from
    * returns: The type of the vector
    """
    var duckdb_vector_get_column_type: LibResult.duckdb_vector_get_column_type_sig

    """
    Retrieves the data pointer of the vector.

    The data pointer can be used to read or write values from the vector.
    How to read or write values depends on the type of the vector.

    * vector: The vector to get the data from
    * returns: The data pointer
    """
    var duckdb_vector_get_data: LibResult.duckdb_vector_get_data_sig

    """
    Retrieves the validity mask pointer of the specified vector.

    If all values are valid, this function MIGHT return NULL!

    The validity mask is a bitset that signifies null-ness within the data chunk.
    It is a series of uint64_t values, where each uint64_t value contains validity for 64 tuples.
    The bit is set to 1 if the value is valid (i.e. not NULL) or 0 if the value is invalid (i.e. NULL).

    Validity of a specific value can be obtained like this:

    idx_t entry_idx = row_idx / 64;
    idx_t idx_in_entry = row_idx % 64;
    bool is_valid = validity_mask[entry_idx] & (1 << idx_in_entry);

    Alternatively, the (slower) duckdb_validity_row_is_valid function can be used.

    * vector: The vector to get the data from
    * returns: The pointer to the validity mask, or NULL if no validity mask is present
    """
    var duckdb_vector_get_validity: LibResult.duckdb_vector_get_validity_sig

    """
    Returns the error message contained within the result. The error is only set if `duckdb_query` returns `DuckDBError`.

    The result of this function must not be freed. It will be cleaned up when `duckdb_destroy_result` is called.

    * result: The result object to fetch the error from.
    * returns: The error of the result.
    """
    var duckdb_result_error: LibResult.duckdb_result_error_sig

    """
    Closes the result and de-allocates all memory allocated for that connection.

    * result: The result to destroy.
    """
    var duckdb_destroy_result: LibResult.duckdb_destroy_result_sig

    """
    Destroys the data chunk and de-allocates all memory allocated for that chunk.

    * chunk: The data chunk to destroy.
    """
    var duckdb_destroy_data_chunk: LibResult.duckdb_destroy_data_chunk_sig

    """
    Free a value returned from `duckdb_malloc`, `duckdb_value_varchar` or `duckdb_value_blob`.

    * ptr: The memory region to de-allocate.
    """
    var duckdb_free: LibResult.duckdb_free_sig

    fn __init__(inout self, libduckdb: DLHandle):
        self.duckdb_column_name = libduckdb.get_function[self.duckdb_column_name_sig](
            "duckdb_column_name"
        )
        self.duckdb_column_type = libduckdb.get_function[self.duckdb_column_type_sig](
            "duckdb_column_type"
        )
        self.duckdb_column_logical_type = libduckdb.get_function[
            self.duckdb_column_logical_type_sig
        ]("duckdb_column_logical_type")
        self.duckdb_column_count = libduckdb.get_function[self.duckdb_column_count_sig](
            "duckdb_column_count"
        )
        self.duckdb_row_count = libduckdb.get_function[self.duckdb_row_count_sig](
            "duckdb_row_count"
        )
        self.duckdb_rows_changed = libduckdb.get_function[self.duckdb_rows_changed_sig](
            "duckdb_rows_changed"
        )
        self.duckdb_result_get_chunk = libduckdb.get_function[
            self.duckdb_result_get_chunk_sig
        ]("duckdb_result_get_chunk")
        self.duckdb_result_is_streaming = libduckdb.get_function[
            self.duckdb_result_is_streaming_sig
        ]("duckdb_result_is_streaming")
        self.duckdb_result_chunk_count = libduckdb.get_function[
            self.duckdb_result_chunk_count_sig
        ]("duckdb_result_chunk_count")
        self.duckdb_data_chunk_get_column_count = libduckdb.get_function[
            self.duckdb_data_chunk_get_column_count_sig
        ]("duckdb_data_chunk_get_column_count")
        self.duckdb_data_chunk_get_size = libduckdb.get_function[
            self.duckdb_data_chunk_get_size_sig
        ]("duckdb_data_chunk_get_size")
        self.duckdb_data_chunk_get_vector = libduckdb.get_function[
            self.duckdb_data_chunk_get_vector_sig
        ]("duckdb_data_chunk_get_vector")
        self.duckdb_vector_get_column_type = libduckdb.get_function[
            self.duckdb_vector_get_column_type_sig
        ]("duckdb_vector_get_column_type")
        self.duckdb_vector_get_data = libduckdb.get_function[
            self.duckdb_vector_get_data_sig
        ]("duckdb_vector_get_data")
        self.duckdb_vector_get_validity = libduckdb.get_function[
            self.duckdb_vector_get_validity_sig
        ]("duckdb_vector_get_validity")
        self.duckdb_result_error = libduckdb.get_function[self.duckdb_result_error_sig](
            "duckdb_result_error"
        )
        self.duckdb_destroy_result = libduckdb.get_function[
            self.duckdb_destroy_result_sig
        ]("duckdb_destroy_result")
        self.duckdb_destroy_data_chunk = libduckdb.get_function[
            self.duckdb_destroy_data_chunk_sig
        ]("duckdb_destroy_data_chunk")
        self.duckdb_free = libduckdb.get_function[self.duckdb_free_sig]("duckdb_free")


struct duckdb_column(Movable):
    # deprecated, use duckdb_column_data
    var __deprecated_data: void_ptr
    # deprecated, use duckdb_nullmask_data
    var __deprecated_nullmask: Pointer[Bool]
    # deprecated, use duckdb_column_type
    # TODO this is enum duckdb_type
    var __deprecated_type: Int
    # deprecated, use duckdb_column_name
    var __deprecated_name: char_ptr
    var internal_data: Pointer[UInt8]

    fn __init__(inout self):
        print("Initialized duckdb_column.")
        self.__deprecated_data = void_ptr().alloc(1)
        self.__deprecated_nullmask = Pointer[Bool]().alloc(1)
        self.__deprecated_type = 0
        self.__deprecated_name = char_ptr().alloc(1)
        self.internal_data = Pointer[UInt8]().alloc(1)

    fn __moveinit__(inout self, owned existing: Self):
        print("Moving duckdb_column.")
        self.__deprecated_data = existing.__deprecated_data
        self.__deprecated_nullmask = existing.__deprecated_nullmask
        self.__deprecated_type = existing.__deprecated_type
        self.__deprecated_name = existing.__deprecated_name
        self.internal_data = existing.internal_data

    fn __del__(owned self):
        print("Deleting duckdb_column.")
        if self.__deprecated_data:
            self.__deprecated_data.free()
        if self.__deprecated_nullmask:
            self.__deprecated_nullmask.free()
        if self.__deprecated_name:
            self.__deprecated_name.free()
        if self.internal_data:
            self.internal_data.free()


struct duckdb_result(Movable):
    # deprecated, use duckdb_column_count
    var __deprecated_column_count: UInt64
    # deprecated, use duckdb_row_count
    var __deprecated_row_count: UInt64
    # deprecated, use duckdb_rows_changed
    var __deprecated_rows_changed: UInt64
    # deprecated, use duckdb_column_ family of functions
    var __deprecated_columns: AnyPointer[duckdb_column]
    # deprecated, use duckdb_result_error
    var __deprecated_error_message: char_ptr
    var internal_data: Pointer[UInt8]

    fn __init__(inout self):
        print("Initialized duckdb_result.")
        self.__deprecated_column_count = 0
        self.__deprecated_row_count = 0
        self.__deprecated_rows_changed = 0
        self.__deprecated_columns = AnyPointer[duckdb_column]().alloc(1)
        self.__deprecated_error_message = char_ptr().alloc(1)
        self.internal_data = Pointer[UInt8]().alloc(1)

    fn __moveinit__(inout self, owned existing: Self):
        print("Moving duckdb_result.")
        self.__deprecated_column_count = existing.__deprecated_column_count
        self.__deprecated_row_count = existing.__deprecated_row_count
        self.__deprecated_rows_changed = existing.__deprecated_rows_changed
        self.__deprecated_columns = existing.__deprecated_columns
        self.__deprecated_error_message = existing.__deprecated_error_message
        self.internal_data = existing.internal_data

    fn __del__(owned self):
        print("Deleting duckdb_result.")
        if self.__deprecated_columns:
            self.__deprecated_columns.free()
        if self.__deprecated_error_message:
            self.__deprecated_error_message.free()
        if self.internal_data:
            self.internal_data.free()


"""
@register_passable
struct duckdb_column:
    # deprecated, use duckdb_column_data
    var __deprecated_data: void_ptr
    # deprecated, use duckdb_nullmask_data
    var __deprecated_nullmask: Pointer[Bool]
    # deprecated, use duckdb_column_type
    # TODO this is enum duckdb_type
    var __deprecated_type: Int
    # deprecated, use duckdb_column_name
    var __deprecated_name: char_ptr
    var internal_data: void_ptr

    fn __init__(self) -> Self:
        print("Initialized duckdb_column.")
        return Self {
            __deprecated_data: void_ptr().alloc(1),
            __deprecated_nullmask: Pointer[Bool]().alloc(1),
            __deprecated_type: 0,
            __deprecated_name: char_ptr().alloc(1),
            internal_data: void_ptr().alloc(1),
        }

    fn __copyinit__(existing: Self) -> Self:
        print("Moving duckdb_column.")
        return Self {
            __deprecated_data: existing.__deprecated_data,
            __deprecated_nullmask: existing.__deprecated_nullmask,
            __deprecated_type: existing.__deprecated_type,
            __deprecated_name: existing.__deprecated_name,
            internal_data: existing.internal_data,
        }

    fn __del__(owned self):
        print("Deleting duckdb_column.")
        if self.__deprecated_data:
            self.__deprecated_data.free()
        if self.__deprecated_nullmask:
            self.__deprecated_nullmask.free()
        if self.__deprecated_name:
            self.__deprecated_name.free()
        if self.internal_data:
            self.internal_data.free()


@register_passable
struct duckdb_result:
    # deprecated, use duckdb_column_count
    var __deprecated_column_count: UInt64
    # deprecated, use duckdb_row_count
    var __deprecated_row_count: UInt64
    # deprecated, use duckdb_rows_changed
    var __deprecated_rows_changed: UInt64
    # deprecated, use duckdb_column_ family of functions
    var __deprecated_columns: Pointer[duckdb_column]
    # deprecated, use duckdb_result_error
    var __deprecated_error_message: char_ptr
    var internal_data: Pointer[UInt8]

    fn __init__() -> Self:
        print("Initialized duckdb_result.")
        return Self {
            __deprecated_column_count: 0,
            __deprecated_row_count: 0,
            __deprecated_rows_changed: 0,
            __deprecated_columns: Pointer[duckdb_column]().alloc(1),
            __deprecated_error_message: char_ptr().alloc(1),
            internal_data: Pointer[UInt8].alloc(1),
        }

    fn __copyinit__(existing: Self) -> Self:
        print("Copying duckdb_result.")
        return Self {
            __deprecated_column_count: existing.__deprecated_column_count,
            __deprecated_row_count: existing.__deprecated_row_count,
            __deprecated_rows_changed: existing.__deprecated_rows_changed,
            __deprecated_columns: existing.__deprecated_columns,
            __deprecated_error_message: existing.__deprecated_error_message,
            internal_data: existing.internal_data,
        }

    fn __del__(owned self):
        print("Deleting duckdb_result.")
        if self.__deprecated_columns:
            self.__deprecated_columns.free()
        if self.__deprecated_error_message:
            self.__deprecated_error_message.free()
        if self.internal_data:
            self.internal_data.free()
"""
