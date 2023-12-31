#include "duckdb.h"
#include <stdio.h>
#include <stdalign.h>

void print_allocs()
{
    const size_t config_sizeof = sizeof(duckdb_config);
    const size_t config_alignof = alignof(duckdb_config);

    const size_t database_sizeof = sizeof(duckdb_database);
    const size_t database_alignof = alignof(duckdb_database);

    const size_t connection_sizeof = sizeof(duckdb_connection);
    const size_t connection_alignof = alignof(duckdb_connection);

    // 48
    const size_t result_sizeof = sizeof(duckdb_result);
    // 8
    const size_t result_alignof = alignof(duckdb_result);

    const size_t statement_sizeof = sizeof(duckdb_prepared_statement);
    const size_t statement_alignof = alignof(duckdb_prepared_statement);

    const size_t appender_sizeof = sizeof(duckdb_appender);
    const size_t appender_alignof = alignof(duckdb_appender);

    printf("RESULT SIZE: %li\n", result_sizeof);
    printf("RESULT ALIGN: %li\n", result_alignof);
}

/* typedef struct
{
#if DUCKDB_API_VERSION < DUCKDB_API_0_3_2
    idx_t column_count;
    idx_t row_count;
    idx_t rows_changed;
    duckdb_column *columns;
    char *error_message;
#else
    // deprecated, use duckdb_column_count
    idx_t __deprecated_column_count;
    // deprecated, use duckdb_row_count
    idx_t __deprecated_row_count;
    // deprecated, use duckdb_rows_changed
    idx_t __deprecated_rows_changed;
    // deprecated, use duckdb_column_ family of functions
    duckdb_column *__deprecated_columns;
    // deprecated, use duckdb_result_error
    char *__deprecated_error_message;
#endif
    void *internal_data;
} duckdb_result; */

void set_struct_values(duckdb_result *out_result)
{
    out_result->__deprecated_column_count = 100;
    out_result->__deprecated_row_count = 200;
    out_result->__deprecated_rows_changed = 300;
    out_result->__deprecated_error_message = "error message";
    out_result->internal_data = "internal data";
}

void read_struct_values(duckdb_result result)
{
    printf("column_count: %lu\n", result.__deprecated_column_count);
    printf("row_count: %lu\n", result.__deprecated_row_count);
    printf("rows_changed: %lu\n", result.__deprecated_rows_changed);
    printf("error_message: %s\n", result.__deprecated_error_message);
    printf("internal_data: %s\n", result.internal_data);
}

void test_struct()
{
    duckdb_result result;
    set_struct_values(&result);
    read_struct_values(result);
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++)
    {
        char *arg = argv[i];
        if (*arg == 't')
        {
            test_struct();
        }
        else if (*arg == 'p')
        {
            print_allocs();
        }
    }
}