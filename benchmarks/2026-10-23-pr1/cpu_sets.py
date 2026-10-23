"""Read Windows CPU-set metadata. Does not measure load or change CPU policy.

ABI: https://learn.microsoft.com/windows/win32/api/winnt/ns-winnt-system_cpu_set_information
"""
import ctypes
import json
import struct
import sys


def cpu_sets():
    if sys.platform != "win32":
        raise RuntimeError("Windows CPU-set query requires Windows")
    function = ctypes.WinDLL("kernel32", use_last_error=True).GetSystemCpuSetInformation
    function.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p, ctypes.c_uint32]
    function.restype = ctypes.c_int
    length = ctypes.c_uint32()
    function(None, 0, ctypes.byref(length), None, 0)
    if not length.value:
        raise ctypes.WinError(ctypes.get_last_error())
    buffer = ctypes.create_string_buffer(length.value)
    if not function(buffer, len(buffer), ctypes.byref(length), None, 0):
        raise ctypes.WinError(ctypes.get_last_error())
    raw = buffer.raw[:length.value]
    offset, result = 0, []
    while offset < len(raw):
        size, kind = struct.unpack_from("<II", raw, offset)
        if size < 8 or offset + size > len(raw):
            raise ValueError("Invalid CPU-set record length")
        if kind == 0:
            if size < 32:
                raise ValueError("Truncated CPU-set record")
            result.append(dict(zip(
                ["id", "group", "logical_index", "core_index", "cache_index", "numa_index", "efficiency_class", "flags"],
                struct.unpack_from("<IHBBBBBB", raw, offset + 8))))
        offset += size
    return result


if __name__ == "__main__":
    print(json.dumps({"schema_version": 1, "cpu_sets": cpu_sets()}, indent=2))
