use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CString;

/// Extract an ArrowArrayStreamReader from a Python object implementing the Arrow PyCapsule Interface.
///
/// See: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
pub fn get_arrow_stream(obj: &Bound<'_, PyAny>) -> PyResult<ArrowArrayStreamReader> {
    // 1. Call __arrow_c_stream__
    // 0. Fallback: Legacy _export_to_c (Bypass Capsule issues if present)
    if obj.hasattr("_export_to_c")? {
        let mut stream = FFI_ArrowArrayStream::empty();
        let ptr = &mut stream as *mut _ as usize;
        obj.call_method1("_export_to_c", (ptr,))?;
        return ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()));
    }

    // 1. Call __arrow_c_stream__
    if !obj.hasattr("__arrow_c_stream__")? {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Object does not implement __arrow_c_stream__ or _export_to_c",
        ));
    }

    let capsule = obj.call_method0("__arrow_c_stream__")?;
    let capsule = capsule.downcast::<PyCapsule>()?;

    // 2. Validate capsule name
    let name = capsule.name()?;
    let expected_name = CString::new("arrow_array_stream").unwrap();
    if name != Some(expected_name.as_c_str()) {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
            "Expected capsule name 'arrow_array_stream', got {:?}",
            name
        )));
    }

    // 3. Extract pointer
    let stream_ptr = unsafe {
        pyo3::ffi::PyCapsule_GetPointer(capsule.as_ptr(), expected_name.as_ptr())
            as *mut FFI_ArrowArrayStream
    };

    if stream_ptr.is_null() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Arrow stream capsule contains NULL pointer",
        ));
    }

    // 4. Move ownership to Rust
    // To prevent double-free (Python GC vs Rust Drop), we must steal ownership.
    // We do this by nulling out the capsule destructor so it does nothing when collected.
    let ret = unsafe { pyo3::ffi::PyCapsule_SetDestructor(capsule.as_ptr(), None) };
    if ret != 0 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Failed to clear PyCapsule destructor",
        ));
    }

    // Now safely take the stream from our extracted pointer
    unsafe {
        let stream = std::ptr::read(stream_ptr);
        ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
    }
}
