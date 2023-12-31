## Experimental Mojo🔥 DuckDB Client

This internally uses the DuckDB C API and currently has some FFI/ABI issues.

Specific things failing:
1. Loading `Pointer[struct]` (and `AnyPointer[struct]`) value into Mojo, where the struct instance is assigned on the C side. Only basic fields are needed (int and void pointers).   
Current behavior: After `load` or `take_value` (Mojo side), only some int fields will have correct value, but if a shallow copy of the pointer to the struct (as in `__moveinit__`) is done before load/take_value, then all int fields will have correct values 🤷‍♂️   
2. Pass simple structs by value to C functions. (currently results in either SIGBUS or SIGSEGV)  

### Setup
- Download and unzip the [DuckDB C/C++ library](https://duckdb.org/docs/installation/?version=latest&environment=cplusplus&installer=binary&platform=linux)
- Edit and run main.mojo

### Future Plans
Any one of:
- Figure out this is actually somehow currently possible in Mojo
- Wait for Modular to document/further implement FFI and AnyPointer
- Wait (probably a long time?) for [C++ Module interop](https://docs.modular.com/mojo/roadmap.html#cc-interop). This would be cool since DuckDB is written in C++ (C API is a wrapper).
- Write custom C functions (as replacement for some current C API functions) so we can pass all arguments as void pointers   

Then:
1. Finish MVP/POC code for reading columnar chunks/vectors from DuckDB to Mojo.
2. Implement prepared statements, 'appender' API, support Mojo parallelism as alterative to DuckDB CPP thread pool.
