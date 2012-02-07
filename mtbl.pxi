cimport cython
from cpython cimport bool
from cpython.string cimport *
from libc.stddef cimport *
from libc.stdint cimport *
from libc.stdlib cimport *

cdef extern from "mtbl.h":
    ctypedef enum mtbl_res:
        mtbl_res_failure
        mtbl_res_success

    ctypedef enum mtbl_compression_type:
        MTBL_COMPRESSION_NONE
        MTBL_COMPRESSION_SNAPPY
        MTBL_COMPRESSION_ZLIB

    struct mtbl_iter:
        pass
    struct mtbl_reader:
        pass
    struct mtbl_reader_options:
        pass
    struct mtbl_writer:
        pass
    struct mtbl_writer_options:
        pass
    struct mtbl_merger:
        pass
    struct mtbl_merger_options:
        pass
    struct mtbl_sorter:
        pass
    struct mtbl_sorter_options:
        pass

    ctypedef void (*mtbl_merge_func)(void *clos, uint8_t *, size_t, uint8_t *, size_t, uint8_t *, size_t, uint8_t **, size_t *)
    
    # iter
    void mtbl_iter_destroy(mtbl_iter **)
    mtbl_res mtbl_iter_next(mtbl_iter *, uint8_t **, size_t *, uint8_t **, size_t *)

    # reader
    mtbl_reader *mtbl_reader_init(char *, mtbl_reader_options *)
    void mtbl_reader_destroy(mtbl_reader **)
    mtbl_res mtbl_reader_get(mtbl_reader *, uint8_t *, size_t, uint8_t **, size_t *)
    mtbl_iter *mtbl_reader_iter(mtbl_reader *)
    mtbl_iter *mtbl_reader_get_range(mtbl_reader *, uint8_t *, size_t, uint8_t *, size_t)
    mtbl_iter *mtbl_reader_get_prefix(mtbl_reader *, uint8_t *, size_t)

    mtbl_reader_options *mtbl_reader_options_init()
    void mtbl_reader_options_destroy(mtbl_reader_options **)
    void mtbl_reader_options_set_verify_checksums(mtbl_reader_options *, bool)

    # writer
    mtbl_writer *mtbl_writer_init(char *, mtbl_writer_options *)
    void mtbl_writer_destroy(mtbl_writer **)
    mtbl_res mtbl_writer_add(mtbl_writer *, uint8_t *, size_t, uint8_t *, size_t)

    mtbl_writer_options *mtbl_writer_options_init()
    void mtbl_writer_options_destroy(mtbl_writer_options **)
    void mtbl_writer_options_set_compression(mtbl_writer_options *, mtbl_compression_type)
    void mtbl_writer_options_set_block_size(mtbl_writer_options *, size_t)
    void mtbl_writer_options_set_block_restart_interval(mtbl_writer_options *, size_t)