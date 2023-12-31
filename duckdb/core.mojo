from os import getenv
from duckdb.ctypes import char_ptr, void_ptr, void, idx_t
from duckdb.libduckdb import (
    duckdb_state,
    duckdb_result,
    DuckDBSuccess,
    LibDuckDB,
    LibCfg,
    LibResult,
    LibConn,
    LibDB,
)
from collections.vector import DynamicVector
from duckdb.utils import str_to_char_ptr, char_ptr_to_str


struct Config:
    var created_ok: Bool
    var _ptr: void_ptr
    var _ptr_ptr: Pointer[void_ptr]
    var _lib: LibCfg

    fn __init__(inout self, lib: LibCfg):
        self._lib = lib
        self._ptr_ptr = Pointer[void_ptr]().alloc(1)
        print("Creating Config")
        let create_ok = self._lib.duckdb_create_config(self._ptr_ptr)
        if create_ok == DuckDBSuccess:
            self.created_ok = True
            print("✅Config Created")
        else:
            self.created_ok = False
            print("❌Could not create config")
        self._ptr = self._ptr_ptr.load(0)

    fn __moveinit__(inout self, owned existing: Self):
        print("Moving Config")
        self.created_ok = existing.created_ok
        self._ptr = existing._ptr
        self._ptr_ptr = existing._ptr_ptr
        self._lib = existing._lib

    fn set_option(self, name: String, option: String) -> Bool:
        let name_chars = str_to_char_ptr(name)
        let option_chars = str_to_char_ptr(option)
        if (
            self._lib.duckdb_set_config(self._ptr, name_chars, option_chars)
            == DuckDBSuccess
        ):
            print("✅Set configuration option", name, "=", option)
            return True
        print("❌Could not set configuration option", name, "=", option)
        return False

    fn show_options(self):
        let n_opt = self._lib.duckdb_config_count()
        let name_ptr_ptr = Pointer[char_ptr]().alloc(1)
        let description_ptr_ptr = Pointer[char_ptr]().alloc(1)
        for i in range(n_opt):
            let ok = self._lib.duckdb_get_config_flag(
                i, name_ptr_ptr, description_ptr_ptr
            )
            # assert ok?
            let name_str = char_ptr_to_str(name_ptr_ptr.load(0))
            let description_str = char_ptr_to_str(description_ptr_ptr.load(0))
            print(name_str + ": " + description_str)
        if name_ptr_ptr:
            name_ptr_ptr.free()
        if description_ptr_ptr:
            description_ptr_ptr.free()

    fn __del__(owned self):
        print("Removing config.")
        let destroyed = self._lib.duckdb_destroy_config(self._ptr)
        print("Removed config", destroyed)
        if self._ptr_ptr:
            print("Freeing config pointer.")
            self._ptr_ptr.free()


struct Database:
    var path: String
    var config: Config
    var is_open: Bool
    var _ptr: void_ptr
    var _ptr_ptr: Pointer[void_ptr]
    var _lib: LibDuckDB

    fn __init__(inout self, path: String) raises:
        self.path = path
        self._lib = LibDuckDB()
        # TODO config options.
        self.config = Config(self._lib.libcfg)
        print("Opening database: ", self.path)
        let path_ptr = str_to_char_ptr(self.path)
        let error_ptr_ptr = Pointer[char_ptr]().alloc(1)
        self._ptr_ptr = Pointer[void_ptr]().alloc(1)
        let open_ok = self._lib.libdb.duckdb_open_ext(
            path_ptr, self._ptr_ptr, self.config._ptr, error_ptr_ptr
        )
        path_ptr.free()
        self._ptr = self._ptr_ptr.load(0)
        if open_ok == DuckDBSuccess:
            self.is_open = True
            print("✅Opened database:", self.path)
        else:
            self.is_open = False
            print("❌Could not open database:", self.path)
            let error_ptr = error_ptr_ptr.load(0)
            let err_msg = char_ptr_to_str(error_ptr)
            print(err_msg)
            _ = self._lib.libresult.duckdb_free(error_ptr)
        error_ptr_ptr.free()

    fn connect(self) -> Connection:
        return Connection(
            database=self, libconn=self._lib.libconn, libresult=self._lib.libresult
        )

    fn close(self):
        # TODO close all connections.
        print("Closing database.")
        _ = self._lib.libdb.duckdb_close(self._ptr_ptr)

    fn __del__(owned self):
        self.close()
        if self._ptr_ptr:
            self._ptr_ptr.free()


struct Connection:
    var is_connected: duckdb_state
    var _ptr: void_ptr
    var _ptr_ptr: Pointer[void_ptr]
    var _libconn: LibConn
    var _libresult: LibResult

    fn __init__(inout self, database: Database, libconn: LibConn, libresult: LibResult):
        self._libconn = libconn
        self._libresult = libresult
        self._ptr_ptr = Pointer[void_ptr]().alloc(1)
        let status = self._libconn.duckdb_connect(database._ptr, self._ptr_ptr)
        self._ptr = self._ptr_ptr.load(0)
        if status == DuckDBSuccess:
            self.is_connected = True
            print("✅Connected to database.")
        else:
            self.is_connected = False
            print("❌Could not connect to database.")

    fn disconnect(self):
        _ = self._libconn.duckdb_disconnect(self._ptr_ptr)

    fn query(self, query: String) -> Result:
        print("Running query: ", query)
        let result = Result(self._libresult)
        let query_chars = str_to_char_ptr(query)
        let status = self._libconn.duckdb_query(
            self._ptr, query_chars, result._data_ptr
        )
        if status == DuckDBSuccess:
            print("Query finished OK:", query)
        else:
            print("Error running query:", query)
        return result ^

    fn __del__(owned self):
        self.disconnect()
        if self._ptr_ptr:
            self._ptr_ptr.free()


struct Result:
    var _data_ptr: AnyPointer[duckdb_result]
    # var _data: duckdb_result
    var _lib: LibResult

    fn __init__(inout self, lib: LibResult):
        self._lib = lib
        self._data_ptr = AnyPointer[duckdb_result]().alloc(1)

    fn __moveinit__(inout self, owned existing: Self):
        print("Moving Result")
        self._data_ptr = existing._data_ptr
        # self._data = existing._data
        self._lib = existing._lib

    fn row_count(self) -> idx_t:
        return self._lib.duckdb_row_count(self._data_ptr)

    fn rows_changed(self) -> idx_t:
        return self._lib.duckdb_rows_changed(self._data_ptr)

    fn column_count(self) -> idx_t:
        return self._lib.duckdb_column_count(self._data_ptr)

    fn column_names(self) -> DynamicVector[String]:
        let n_cols = self.column_count()
        var names = DynamicVector[String]()
        for i in range(n_cols):
            let name_chars = self._lib.duckdb_column_name(self._data_ptr, i)
            names.append(char_ptr_to_str(name_chars))
        return names

    fn data_pointers(self) -> Int:
        let col_count = self.column_count()
        let row_count = self.row_count()
        # TODO deal with having value taken
        let data = self._data_ptr.take_value()
        # let data = self._data_ptr.load(0)
        print("COL COUNT FUNC:", col_count)
        print("ROW COUNT FUNC:", row_count)
        let is_null = data.internal_data == Pointer[UInt8].get_null()
        print("IS NULL INTERNAL:", is_null)
        print("COL COUNT:", data.__deprecated_column_count)
        print("ROW COUNT:", data.__deprecated_row_count)
        print("ROWS CHAGED:", data.__deprecated_rows_changed)
        if data.__deprecated_error_message != char_ptr.get_null():
            let err_msg = char_ptr_to_str(data.__deprecated_error_message)
            print("ERROR MSG:", err_msg)
        else:
            print("NO ERROR")
        let chunk_count = self._lib.duckdb_result_chunk_count(data)
        print("CHUNK COUNT:", chunk_count)
        """
        for i in range(chunk_count):
            # let chunk = DataChunk(self._lib, data, i)
            let _data_ptr_ptr = self._lib.duckdb_result_get_chunk(data, i)
            print("GOT CHUNK")
            let _data_ptr = _data_ptr_ptr.load(0)
            print("GOT CHUNK DATA")
            let chunk_row_count = self._lib.duckdb_data_chunk_get_size(_data_ptr_ptr)
            print("CHUNK ROW COUNT:", chunk_row_count)
            # let col_cnt = self._lib.duckdb_data_chunk_get_column_count(chunk._data_ptr)
            #    for c in range(col_cnt):
            #        let vec = self._lib.duckdb_data_chunk_get_vector(chunk._data_ptr, c)
            #        # TODO destroy type.
            #        let logical_t = self._lib.duckdb_vector_get_column_type(vec)
            #        let data_ptr = self._lib.duckdb_vector_get_data(vec)
            #        let val_mask_ptr = self._lib.duckdb_vector_get_validity(vec)
        """
        return 0

    fn __del__(owned self):
        print("Deleting Result")
        _ = self._lib.duckdb_destroy_result(self._data_ptr)
        if self._data_ptr:
            print("Freeing result pointer.")
            self._data_ptr.free()
        print("Result pointer freed")


@value
struct DataChunk:
    var row_count: UInt64
    var _data_ptr_ptr: Pointer[void_ptr]
    var _data_ptr: void_ptr
    var _c_del: fn (Pointer[void_ptr]) -> void

    fn __init__(inout self, lib: LibResult, result: duckdb_result, chunk_index: Int):
        self._c_del = lib.duckdb_destroy_data_chunk
        self._data_ptr_ptr = lib.duckdb_result_get_chunk(result, chunk_index)
        print("GOT CHUNK")
        self._data_ptr = self._data_ptr_ptr.load(0)
        print("GOT CHUNK DATA")
        self.row_count = lib.duckdb_data_chunk_get_size(self._data_ptr_ptr)
        print("ROW COUNT:", self.row_count)

    fn __del__(owned self):
        print("Deleting DataChunk")
        _ = self._c_del(self._data_ptr_ptr)


# TODO DataVector
