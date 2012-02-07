include "mtbl.pxi"

COMPRESSION_NONE = MTBL_COMPRESSION_NONE
COMPRESSION_SNAPPY = MTBL_COMPRESSION_SNAPPY
COMPRESSION_ZLIB = MTBL_COMPRESSION_ZLIB

class IterException(Exception):
    pass

class KeyOrderError(Exception):
    pass

class TableClosedException(Exception):
    pass

class UnknownCompressionTypeException(Exception):
    pass

class UninitializedException(Exception):
    pass

ImmutableError = TypeError('object does not support mutation')

@cython.internal
cdef class iterkeys(object):
    cdef mtbl_iter *_instance

    def __cinit__(self):
        self._instance = NULL

    def __dealloc__(self):
        mtbl_iter_destroy(&self._instance)

    def __iter__(self):
        if self._instance == NULL:
            raise NotImplementedError
        return self

    def __next__(self):
        cdef mtbl_res res
        cdef uint8_t *key
        cdef uint8_t *val
        cdef size_t len_key
        cdef size_t len_val

        if self._instance == NULL:
            raise StopIteration

        res = mtbl_iter_next(self._instance, &key, &len_key, &val, &len_val)
        if res == mtbl_res_failure:
            raise StopIteration
        return PyString_FromStringAndSize(<char *> key, len_key)

@cython.internal
cdef class itervalues(object):
    cdef mtbl_iter *_instance

    def __cinit__(self):
        self._instance = NULL

    def __dealloc__(self):
        mtbl_iter_destroy(&self._instance)

    def __iter__(self):
        if self._instance == NULL:
            raise NotImplementedError
        return self

    def __next__(self):
        cdef mtbl_res res
        cdef uint8_t *key
        cdef uint8_t *val
        cdef size_t len_key
        cdef size_t len_val

        if self._instance == NULL:
            raise StopIteration

        res = mtbl_iter_next(self._instance, &key, &len_key, &val, &len_val)
        if res == mtbl_res_failure:
            raise StopIteration
        return PyString_FromStringAndSize(<char *> val, len_val)

@cython.internal
cdef class iteritems(object):
    cdef mtbl_iter *_instance

    def __cinit__(self):
        self._instance = NULL

    def __dealloc__(self):
        mtbl_iter_destroy(&self._instance)

    def __iter__(self):
        if self._instance == NULL:
            raise NotImplementedError
        return self

    def __next__(self):
        cdef mtbl_res res
        cdef uint8_t *key
        cdef uint8_t *val
        cdef size_t len_key
        cdef size_t len_val

        if self._instance == NULL:
            raise StopIteration

        res = mtbl_iter_next(self._instance, &key, &len_key, &val, &len_val)
        if res == mtbl_res_failure:
            raise StopIteration
        return (PyString_FromStringAndSize(<char *> key, len_key),
                PyString_FromStringAndSize(<char *> val, len_val))

cdef get_iterkeys(mtbl_iter *instance):
    if instance == NULL:
        raise IterException
    it = iterkeys()
    it._instance = instance
    return it

cdef get_itervalues(mtbl_iter *instance):
    if instance == NULL:
        raise IterException
    it = itervalues()
    it._instance = instance
    return it

cdef get_iteritems(mtbl_iter *instance):
    if instance == NULL:
        raise IterException
    it = iteritems()
    it._instance = instance
    return it

@cython.internal
cdef class DictMixin(object):
    def __iter__(self):
        return self.iterkeys()

    def iter(self):
        return self.iterkeys()

    def items(self):
        return [ (k, v) for k, v in self.iteritems() ]

    def keys(self):
        return [ k for k in self.iterkeys() ]

    def values(self):
        return [ v for v in self.itervalues() ]

    def __delitem__(self, key):
        raise ImmutableError

    def __setitem__(self, key, value):
        raise ImmutableError

    def pop(self, *a, **b):
        raise ImmutableError

    def popitem(self):
        raise ImmutableError

    def update(self, *a, **b):
        raise ImmutableError

cdef class reader(DictMixin):
    cdef mtbl_reader *_instance

    def __cinit__(self):
        self._instance = NULL

    def __dealloc__(self):
        mtbl_reader_destroy(&self._instance)

    def __init__(self, bytes fname, bool verify_checksums=False):
        cdef mtbl_reader_options *opt
        opt = mtbl_reader_options_init()
        mtbl_reader_options_set_verify_checksums(opt, verify_checksums)
        self._instance = mtbl_reader_init(fname, opt)
        mtbl_reader_options_destroy(&opt)
        if (self._instance == NULL):
            raise IOError("unable to open file: '%s'" % fname)

    def check_initialized(self):
        if self._instance == NULL:
            raise UninitializedException

    def iterkeys(self):
        self.check_initialized()
        it = iterkeys()
        it._instance = mtbl_reader_iter(self._instance)
        if it._instance == NULL:
            raise IterException
        return it

    def itervalues(self):
        self.check_initialized()
        it = itervalues()
        it._instance = mtbl_reader_iter(self._instance)
        if it._instance == NULL:
            raise IterException
        return it

    def iteritems(self):
        self.check_initialized()
        it = iteritems()
        it._instance = mtbl_reader_iter(self._instance)
        if it._instance == NULL:
            raise IterException
        return it

    def __contains__(self, bytes py_key):
        try:
            self.__getitem__(py_key)
            return True
        except KeyError:
            pass
        return False

    def has_key(self, bytes py_key):
        return self.__contains__(py_key)

    def get(self, bytes py_key, default=None):
        try:
            return self.__getitem__(py_key)
        except KeyError:
            pass
        return default

    def __getitem__(self, bytes py_key):
        cdef mtbl_res res
        cdef uint8_t *key
        cdef uint8_t *val
        cdef size_t len_key
        cdef size_t len_val

        self.check_initialized()

        key = <uint8_t *> PyString_AsString(py_key)
        len_key = PyString_Size(py_key)

        res = mtbl_reader_get(self._instance, key, len_key, &val, &len_val)
        if res == mtbl_res_success:
            return PyString_FromStringAndSize(<char *> val, len_val)
        raise KeyError(py_key)

cdef class writer(object):
    cdef mtbl_writer *_instance

    def __cinit__(self):
        self._instance = NULL

    def __dealloc__(self):
        mtbl_writer_destroy(&self._instance)

    def __init__(self,
            bytes fname,
            mtbl_compression_type compression=MTBL_COMPRESSION_NONE,
            size_t block_size=8192,
            size_t block_restart_interval=16):
        if not (compression == COMPRESSION_NONE or
                compression == COMPRESSION_SNAPPY or
                compression == COMPRESSION_ZLIB):
            raise UnknownCompressionTypeException

        cdef mtbl_writer_options *opt
        opt = mtbl_writer_options_init()
        mtbl_writer_options_set_compression(opt, compression)
        mtbl_writer_options_set_block_size(opt, block_size)
        mtbl_writer_options_set_block_restart_interval(opt, block_restart_interval)
        self._instance = mtbl_writer_init(fname, opt)
        mtbl_writer_options_destroy(&opt)
        if self._instance == NULL:
            raise IOError("unable to initialize file: '%s'" % fname)

    def close(self):
        mtbl_writer_destroy(&self._instance)

    def __setitem__(self, bytes py_key, bytes py_val):
        cdef mtbl_res res
        cdef uint8_t *key
        cdef uint8_t *val
        cdef size_t len_key
        cdef size_t len_val

        if self._instance == NULL:
            raise TableClosedException

        key = <uint8_t *> PyString_AsString(py_key)
        val = <uint8_t *> PyString_AsString(py_val)
        len_key = PyString_Size(py_key)
        len_val = PyString_Size(py_val)

        res = mtbl_writer_add(self._instance, key, len_key, val, len_val)
        if res == mtbl_res_failure:
            raise KeyOrderError

cdef void merge_func_wrapper(void *clos,
        uint8_t *key, size_t len_key,
        uint8_t *val0, uint8_t len_val0,
        uint8_t *val1, uint8_t len_val1,
        uint8_t **merged_val, size_t *len_merged_val) with gil:
    cdef str py_key
    cdef str py_val0
    cdef str py_val1
    cdef str py_merged_val
    py_key = PyString_FromStringAndSize(<char *> key, len_key)
    py_val0 = PyString_FromStringAndSize(<char *> val0, len_val0)
    py_val1 = PyString_FromStringAndSize(<char *> val1, len_val1)
    py_merged_val = (<object> clos)(py_key, py_val0, py_val1)
    len_merged_val[0] = <size_t> PyString_Size(py_merged_val)
    merged_val[0] = <uint8_t *> malloc(len_merged_val[0])
    memcpy(merged_val[0], PyString_AsString(py_merged_val), len_merged_val[0])

cdef class merger(object):
    cdef mtbl_merger *_instance

    def __cinit__(self):
        self._instance = NULL

    def __dealloc__(self):
        mtbl_merger_destroy(&self._instance)

    def __init__(self, object merge_func):
        cdef mtbl_merger_options *opt
        opt = mtbl_merger_options_init()
        mtbl_merger_options_set_merge_func(opt,
                                           <mtbl_merge_func> merge_func_wrapper,
                                           <void *> merge_func)
        self._instance = mtbl_merger_init(opt)
        mtbl_merger_options_destroy(&opt)

    def add_reader(self, reader r):
        cdef mtbl_res res

        res = mtbl_merger_add_reader(self._instance, r._instance)
        if res != mtbl_res_success:
            raise RuntimeError
        r._instance = NULL

    def write(self, writer w):
        cdef mtbl_res res

        res = mtbl_merger_write(self._instance, w._instance)
        if res != mtbl_res_success:
            raise RuntimeError

    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        return get_iterkeys(mtbl_merger_iter(self._instance))

    def itervalues(self):
        return get_itervalues(mtbl_merger_iter(self._instance))

    def iteritems(self):
        return get_iteritems(mtbl_merger_iter(self._instance))
