from duckdb.ctypes import char


fn str_to_char_ptr(s: String) -> Pointer[char]:
    """Convert ASCII string to char pointer."""
    let ptr = Pointer[char]().alloc(len(s))
    for i in range(len(s)):
        ptr.store(i, ord(s[i]))
    return ptr


fn char_ptr_to_str(chars: Pointer[char]) -> String:
    let n_chars = strlen(chars) + 1
    # let tmp = DTypePointer[DType.int8]().alloc(n_chars)
    let tmp = Pointer[Int8]().alloc(n_chars)
    # memcpy(tmp, chars.bitcast[Int8], n_chars)
    for i in range(n_chars):
        tmp.store(i, chars.load(i))
    return String(tmp, n_chars)


fn strlen(s: DTypePointer[DType.int8]) -> Int:
    """libc POSIX `strlen` function
    Reference: https://man7.org/linux/man-pages/man3/strlen.3p.html
    Fn signature: size_t strlen(const char *s)
    """
    return external_call["strlen", Int, DTypePointer[DType.int8]](s)


fn strlen(s: Pointer[char]) -> Int:
    """libc POSIX `strlen` function
    Reference: https://man7.org/linux/man-pages/man3/strlen.3p.html
    Fn signature: size_t strlen(const char *s)
    """
    return external_call["strlen", Int, Pointer[char]](s)
