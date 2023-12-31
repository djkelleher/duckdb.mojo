from os import setenv
from duckdb.libduckdb import LibDuckDB
from duckdb.core import Database
from sys.ffi import DLHandle, RTLD
from duckdb.libduckdb import duckdb_result


fn db_read() raises:
    let set_ok = setenv("LIBDUCKDB", "/home/dan/libduckdb/libduckdb.so", True)
    let db = Database(path="/home/dan/repos-dev/duckdb.mojo/experimental/test.db")
    let conn = db.connect()
    let query = String("SELECT * FROM test_tbl")
    let res = conn.query(query)
    let row_cnt = res.row_count()
    print("Row Count:", row_cnt)
    let rows_chg = res.rows_changed()
    print("Rows Changed:", rows_chg)
    let col_cnt = res.column_count()
    print("Column Count:", col_cnt)
    let cc = res.column_names()
    for i in range(len(cc)):
        print("Column:", cc[i])
    let ret = res.data_pointers()


fn test_ffi():
    let lib = DLHandle("/home/dan/repos-dev/duckdb.mojo/experimental/ffi.so", RTLD.LAZY)
    let set_struct_values = lib.get_function[fn (AnyPointer[duckdb_result]) -> UInt8](
        "set_struct_values"
    )
    let read_struct_values = lib.get_function[fn (duckdb_result) -> UInt8](
        "read_struct_values"
    )
    # TODO test assiging entire stuct instace on C side.
    let result = duckdb_result()
    let result_ptr = AnyPointer[duckdb_result]().alloc(1)
    result_ptr.emplace_value(result ^)
    print("Setting values")
    _ = set_struct_values(result_ptr)
    print("Reading values")
    _ = read_struct_values(result_ptr.take_value())
    print("done")


fn main() raises:
    print("Start")
    db_read()
    # test_ffi()
    print("End")
