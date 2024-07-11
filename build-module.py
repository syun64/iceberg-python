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

import os
import shutil
from pathlib import Path

allowed_to_fail = os.environ.get("CIBUILDWHEEL", "0") != "1"


def build_cython_extensions() -> None:
    import Cython.Compiler.Options
    from Cython.Build import build_ext, cythonize
    from setuptools import Extension
    from setuptools.dist import Distribution
    import numpy as np
    import pyarrow as pa

    package_path = "pyiceberg"

    extra_compile_args = ["-std=c++17"] if os.name == "posix" else []
    hashing_extension = Extension(
        # Your .pyx file will be available to cpython at this location.
        name="pyiceberg.utils.hashing",
        sources=[
            os.path.join(package_path, "utils", "hashing.pyx"),
        ],
        extra_compile_args=extra_compile_args,
        language="c++",
        include_dirs=[np.get_include(), pa.get_include()],
        libraries=pa.get_libraries(),
        library_dirs=pa.get_library_dirs(),
    )
    arrow_hashing_ext_modules = cythonize([hashing_extension], include_path=list(package_path), language_level=3, annotate=True)

    Cython.Compiler.Options.annotate = True

    if os.name == "nt":  # Windows
        extra_compile_args = [
            "/O2",
        ]
    else:  # UNIX-based systems
        extra_compile_args = [
            "-O3",
        ]

    extension = Extension(
        # Your .pyx file will be available to cpython at this location.
        name="pyiceberg.avro.decoder_fast",
        sources=[
            os.path.join(package_path, "avro", "decoder_fast.pyx"),
        ],
        extra_compile_args=extra_compile_args,
        language="c",
    )

    ext_modules = cythonize([extension], include_path=list(package_path), language_level=3, annotate=True)
    dist = Distribution({"ext_modules": ext_modules + arrow_hashing_ext_modules})
    cmd = build_ext(dist)
    cmd.ensure_finalized()

    cmd.run()

    for output in cmd.get_outputs():
        output = Path(output)
        relative_extension = output.relative_to(cmd.build_lib)
        shutil.copyfile(output, relative_extension)


try:
    build_cython_extensions()
except Exception:
    if not allowed_to_fail:
        raise
