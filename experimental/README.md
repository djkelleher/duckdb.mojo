
(assuming libduckdb is in user home directory)
- `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/libduckdb`   
*create/read database*   
- `clang -o db db.c -I$HOME/libduckdb -L$HOME/libduckdb -lduckdb`
- `./db w r`   
*FFI testing* 
- `clang -shared -o ffi.so ffi.c -I$HOME/libduckdb -L$HOME/libduckdb -lduckdb`     
- `clang -o ffi ffi.c -I$HOME/libduckdb -L$HOME/libduckdb -lduckdb`   
- `./ffi p t`