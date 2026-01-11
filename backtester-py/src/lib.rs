#![allow(unsafe_op_in_unsafe_fn)]

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};

#[pyclass]
pub struct Backtester {
    data: Py<PyAny>,
    seed: u64,
}

#[pymethods]
impl Backtester {
    #[new]
    pub fn new(data: Py<PyAny>, seed: u64) -> Self {
        Self { data, seed }
    }

    /// Minimal E2E "run" to validate packaging + determinism plumbing.
    ///
    /// Expects:
    /// - `data`: dict[str, polars.LazyFrame]
    /// - LazyFrames contain columns: ts_exchange, price, qty, side (and optional seq)
    pub fn run_smoke(&self, py: Python<'_>) -> PyResult<i64> {
        let _seed = self.seed; // kept for forward compatibility / reproducibility config
        checksum_from_polars_data(py, &self.data)
    }
}

/// Call `strategy.on_ticks(ticks)` from Rust in a tight loop.
///
/// This intentionally measures the Python↔Rust↔Python boundary overhead for a batched callback.
#[pyfunction]
pub fn call_strategy_on_ticks(
    py: Python<'_>,
    strategy: Py<PyAny>,
    batch_size: usize,
    iterations: usize,
) -> PyResult<()> {
    let ticks = PyList::new_bound(py, (0..batch_size).map(|i| i as i64));
    let strategy = strategy.bind(py);
    for _ in 0..iterations {
        strategy.call_method1("on_ticks", (&ticks,))?;
    }
    Ok(())
}

fn checksum_from_polars_data(py: Python<'_>, data: &Py<PyAny>) -> PyResult<i64> {
    let data_any = data.bind(py);
    let data_dict = data_any.downcast::<PyDict>()?;

    let mut checksum: i128 = 0;
    for (_k, lf) in data_dict.iter() {
        // lf: polars.LazyFrame
        let df = lf.call_method0("collect")?;

        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("as_series", false)?;
        let dict_any = df.call_method("to_dict", (), Some(&kwargs))?;
        let dict = dict_any.downcast::<PyDict>()?;

        let ts_exchange_any = dict
            .get_item("ts_exchange")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing ts_exchange"))?;
        let price_any = dict
            .get_item("price")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing price"))?;
        let qty_any = dict
            .get_item("qty")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing qty"))?;
        let side_any = dict
            .get_item("side")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing side"))?;

        let seq_any = dict.get_item("seq")?;

        let ts_exchange = ts_exchange_any.downcast::<PyList>().map_err(|_| {
            PyErr::new::<pyo3::exceptions::PyTypeError, _>("ts_exchange must be a list")
        })?;
        let price = price_any
            .downcast::<PyList>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("price must be a list"))?;
        let qty = qty_any
            .downcast::<PyList>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("qty must be a list"))?;
        let side = side_any
            .downcast::<PyList>()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyTypeError, _>("side must be a list"))?;
        let seq_list = match seq_any.as_ref() {
            Some(any) => Some(any.downcast::<PyList>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("seq must be a list")
            })?),
            None => None,
        };

        let n = ts_exchange.len();
        if price.len() != n || qty.len() != n || side.len() != n {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "column lengths mismatch",
            ));
        }
        if let Some(seq) = seq_list {
            if seq.len() != n {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "seq length mismatch",
                ));
            }
        }

        for i in 0..n {
            let ts: i64 = ts_exchange.get_item(i)?.extract()?;
            let p: i64 = price.get_item(i)?.extract()?;
            let q: i64 = qty.get_item(i)?.extract()?;
            let s: i64 = side.get_item(i)?.extract()?;
            checksum += ts as i128;
            checksum += p as i128;
            checksum += q as i128;
            checksum += s as i128;
            if let Some(seq) = seq_list {
                let seq_i: i64 = seq.get_item(i)?.extract()?;
                checksum += seq_i as i128;
            }
        }
    }

    Ok(checksum as i64)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Backtester>()?;
    m.add_function(wrap_pyfunction!(call_strategy_on_ticks, m)?)?;
    Ok(())
}
