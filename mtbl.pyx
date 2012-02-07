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

    def iterkeys(self):
        it = iterkeys()
        it._instance = mtbl_reader_iter(self._instance)
        if it._instance == NULL:
            raise IterException
        return it

    def itervalues(self):
        it = itervalues()
        it._instance = mtbl_reader_iter(self._instance)
        if it._instance == NULL:
            raise IterException
        return it

    def iteritems(self):
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
