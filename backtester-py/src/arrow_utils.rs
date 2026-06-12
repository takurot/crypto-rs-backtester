use arrow::array::{Array, ArrayData, RecordBatch, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CString;
use std::ptr::addr_of;

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
    let capsule = capsule.cast::<PyCapsule>()?;

    // 2. Validate capsule name
    let name = capsule.name()?;
    let expected_name = CString::new("arrow_array_stream").unwrap();
    // SAFETY: the name is only read for an immediate comparison.
    let name_matches =
        matches!(name, Some(n) if unsafe { n.as_cstr() } == expected_name.as_c_str());
    if !name_matches {
        return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
            "Expected capsule name 'arrow_array_stream'",
        ));
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

/// Convert Arrow `ArrayData` into a `pyarrow.Array` via the C Data Interface.
///
/// Replaces `arrow::pyarrow::ToPyArrow`, which is unavailable here because the
/// `arrow` crate's `pyarrow` feature pins `pyo3` to `^0.28`, conflicting (via
/// Cargo's `links = "python"` uniqueness rule) with this crate's `pyo3` version.
pub fn array_data_to_pyarrow<'py>(py: Python<'py>, data: &ArrayData) -> PyResult<Py<PyAny>> {
    let array = FFI_ArrowArray::new(data);
    let schema = FFI_ArrowSchema::try_from(data.data_type())
        .map_err(|e| PyValueError::new_err(e.to_string()))?;

    let pyarrow = py.import("pyarrow")?;
    let array_cls = pyarrow.getattr("Array")?;
    // SAFETY: `Array._import_from_c` performs a *moving* import per the Arrow C
    // Data Interface spec: it copies the struct contents and takes ownership of
    // the `release` callback/`private_data`, nulling out `array`/`schema`'s
    // `release` pointers in place. When `array`/`schema` are dropped at the end
    // of this scope, their `release == NULL` makes `FFI_ArrowArray`/`FFI_ArrowSchema`
    // drop into no-ops, so the underlying buffers are not double-freed.
    let py_array = array_cls.call_method1(
        "_import_from_c",
        (addr_of!(array) as usize, addr_of!(schema) as usize),
    )?;
    Ok(py_array.unbind())
}

/// Convert an Arrow `RecordBatch` into a `pyarrow.RecordBatch` via the C Data Interface.
pub fn record_batch_to_pyarrow<'py>(py: Python<'py>, batch: &RecordBatch) -> PyResult<Py<PyAny>> {
    let struct_array: StructArray = batch.clone().into();
    let py_struct_array = array_data_to_pyarrow(py, &struct_array.to_data())?;

    let pyarrow = py.import("pyarrow")?;
    let record_batch_cls = pyarrow.getattr("RecordBatch")?;
    let py_batch = record_batch_cls.call_method1("from_struct_array", (py_struct_array,))?;
    Ok(py_batch.unbind())
}
