# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# distutils: language=c++

from libc.stdlib cimport malloc
from pyarrow.lib cimport *
import random


def get_array_length(obj):
    # Just an example function accessing both the pyarrow Cython API
    # and the Arrow C++ API
    cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(obj)
    if arr.get() == NULL:
        raise TypeError("not an array")
    
    cdef int array_size = arr.get().length()
    
    cdef int *hashes = <int *> malloc(array_size * sizeof(int))
    if not hashes:
        raise MemoryError()

    ran = random.normalvariate
    for i in array_size:
        hashes[i] = ran(0, 1)

    return pyarrow_wrap_array(<shared_ptr[CArray]> hashes)