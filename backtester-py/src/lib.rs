#![allow(unsafe_op_in_unsafe_fn)]

use std::collections::BTreeMap;

mod arrow_utils;

use arrow::array::{ArrayRef, Int8Builder, Int64Builder, UInt32Builder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_utils::get_arrow_stream;
use backtester_core::engine::{EngineConfig, EngineMode, Strategy as CoreStrategy};
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::ConservativeQueue;
use backtester_core::stats::{equity_curve_from_pnl_deltas, full_pnl_history};
use backtester_core::tick_source::ArrowTickSource; // Import TickSource types
use backtester_core::types::{Order, OrderReport, OrderType, Side, Tick};
use backtester_core::{BacktestStats, TradeFill, TradeLogMode};
use backtester_core::{Context as CoreContext, Engine, EventKind};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};
use rayon::prelude::*;
use std::sync::Arc;

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyTick {
    #[pyo3(get)]
    ts_exchange: i64,
    #[pyo3(get)]
    ts_local: i64,
    #[pyo3(get)]
    seq: u64,
    #[pyo3(get)]
    symbol_id: u32,
    #[pyo3(get)]
    price: i64,
    #[pyo3(get)]
    qty: i64,
    #[pyo3(get)]
    side: i8,
    #[pyo3(get)]
    flags: u8,
}

#[pymethods]
impl PyTick {
    fn __getitem__(&self, key: &str) -> PyResult<PyObject> {
        Python::with_gil(|py| match key {
            "ts_exchange" => Ok(self.ts_exchange.into_py(py)),
            "ts_local" => Ok(self.ts_local.into_py(py)),
            "seq" => Ok(self.seq.into_py(py)),
            "symbol_id" => Ok(self.symbol_id.into_py(py)),
            "price" => Ok(self.price.into_py(py)),
            "qty" => Ok(self.qty.into_py(py)),
            "side" => Ok(self.side.into_py(py)),
            "flags" => Ok(self.flags.into_py(py)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                key.to_string(),
            )),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "Tick(ts_exchange={}, symbol_id={}, price={}, qty={}, side={})",
            self.ts_exchange, self.symbol_id, self.price, self.qty, self.side
        )
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct PyOrderReport {
    #[pyo3(get)]
    order_id: u64,
    #[pyo3(get)]
    symbol_id: u32,
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    last_fill_qty: i64,
    #[pyo3(get)]
    last_fill_price: i64,
    #[pyo3(get)]
    filled_qty: i64,
    #[pyo3(get)]
    remaining_qty: i64,
    #[pyo3(get)]
    reason: Option<String>,
}

#[pymethods]
impl PyOrderReport {
    fn __getitem__(&self, key: &str) -> PyResult<PyObject> {
        Python::with_gil(|py| match key {
            "order_id" => Ok(self.order_id.into_py(py)),
            "symbol_id" => Ok(self.symbol_id.into_py(py)),
            "status" => Ok(self.status.clone().into_py(py)),
            "last_fill_qty" => Ok(self.last_fill_qty.into_py(py)),
            "last_fill_price" => Ok(self.last_fill_price.into_py(py)),
            "filled_qty" => Ok(self.filled_qty.into_py(py)),
            "remaining_qty" => Ok(self.remaining_qty.into_py(py)),
            "reason" => Ok(self.reason.clone().into_py(py)),
            _ => Err(PyErr::new::<pyo3::exceptions::PyKeyError, _>(
                key.to_string(),
            )),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "OrderReport(order_id={}, symbol_id={}, status={}, last_fill_qty={}, filled_qty={}, remaining_qty={})",
            self.order_id,
            self.symbol_id,
            self.status,
            self.last_fill_qty,
            self.filled_qty,
            self.remaining_qty
        )
    }

    /// Dict-like get method for backward compatibility
    #[pyo3(signature = (key, default=None))]
    fn get(&self, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        Python::with_gil(|py| match key {
            "order_id" => Ok(self.order_id.into_py(py)),
            "symbol_id" => Ok(self.symbol_id.into_py(py)),
            "status" => Ok(self.status.clone().into_py(py)),
            "last_fill_qty" => Ok(self.last_fill_qty.into_py(py)),
            "last_fill_price" => Ok(self.last_fill_price.into_py(py)),
            "filled_qty" => Ok(self.filled_qty.into_py(py)),
            "remaining_qty" => Ok(self.remaining_qty.into_py(py)),
            "reason" => Ok(self.reason.clone().into_py(py)),
            _ => Ok(default.unwrap_or_else(|| py.None())),
        })
    }
}

#[pyclass]
pub struct Backtester {
    data: Py<PyAny>,
    feed_latency_ns: i64,
    order_update_latency_ns: i64,
    python_mode: String,
    batch_ms: i64,
    seed: u64,
    trade_log_mode: String,
}

#[pyclass]
pub struct BacktestResult {
    trades: Vec<TradeFill>,
    stats: BacktestStats,
    equity_curve: Vec<(i64, i64)>,
}

#[pymethods]
impl BacktestResult {
    pub fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        backtest_stats_to_pydict(py, &self.stats)
    }

    pub fn trades<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty_bound(py);
        for t in &self.trades {
            out.append(trade_fill_to_pydict(py, t)?)?;
        }
        Ok(out)
    }

    /// Return trades as a PyArrow-compatible dict of arrays for zero-copy access.
    /// Schema: ts_exchange (i64), symbol_id (u32), order_id (u64), side (i8), price (i64), qty (i64)
    pub fn trades_df<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let n = self.trades.len();

        let mut ts_builder = Int64Builder::with_capacity(n);
        let mut symbol_builder = UInt32Builder::with_capacity(n);
        let mut order_builder = UInt64Builder::with_capacity(n);
        let mut side_builder = Int8Builder::with_capacity(n);
        let mut price_builder = Int64Builder::with_capacity(n);
        let mut qty_builder = Int64Builder::with_capacity(n);

        for t in &self.trades {
            ts_builder.append_value(t.ts_exchange);
            symbol_builder.append_value(t.symbol_id);
            order_builder.append_value(t.order_id);
            side_builder.append_value(t.side.as_i8());
            price_builder.append_value(t.price);
            qty_builder.append_value(t.qty);
        }

        let ts_array: ArrayRef = Arc::new(ts_builder.finish());
        let symbol_array: ArrayRef = Arc::new(symbol_builder.finish());
        let order_array: ArrayRef = Arc::new(order_builder.finish());
        let side_array: ArrayRef = Arc::new(side_builder.finish());
        let price_array: ArrayRef = Arc::new(price_builder.finish());
        let qty_array: ArrayRef = Arc::new(qty_builder.finish());

        let d = PyDict::new_bound(py);
        d.set_item("ts_exchange", ts_array.to_data().to_pyarrow(py)?)?;
        d.set_item("symbol_id", symbol_array.to_data().to_pyarrow(py)?)?;
        d.set_item("order_id", order_array.to_data().to_pyarrow(py)?)?;
        d.set_item("side", side_array.to_data().to_pyarrow(py)?)?;
        d.set_item("price", price_array.to_data().to_pyarrow(py)?)?;
        d.set_item("qty", qty_array.to_data().to_pyarrow(py)?)?;
        d.set_item("_len", n)?;
        Ok(d)
    }

    /// Return equity curve as a PyArrow-compatible dict of arrays for zero-copy access.
    /// Schema: ts_exchange (i64), equity (i64)
    pub fn equity_curve_df<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let n = self.equity_curve.len();

        let mut ts_builder = Int64Builder::with_capacity(n);
        let mut equity_builder = Int64Builder::with_capacity(n);

        for (ts, eq) in &self.equity_curve {
            ts_builder.append_value(*ts);
            equity_builder.append_value(*eq);
        }

        let ts_array: ArrayRef = Arc::new(ts_builder.finish());
        let equity_array: ArrayRef = Arc::new(equity_builder.finish());

        let d = PyDict::new_bound(py);
        d.set_item("ts_exchange", ts_array.to_data().to_pyarrow(py)?)?;
        d.set_item("equity", equity_array.to_data().to_pyarrow(py)?)?;
        d.set_item("_len", n)?;
        Ok(d)
    }
}
#[pymethods]
impl Backtester {
    #[new]
    #[pyo3(signature = (data, feed_latency_ns=0, order_update_latency_ns=0, python_mode="tick", batch_ms=0, seed=42, trade_log_mode="all"))]
    pub fn new(
        data: Py<PyAny>,
        feed_latency_ns: i64,
        order_update_latency_ns: i64,
        python_mode: &str,
        batch_ms: i64,
        seed: u64,
        trade_log_mode: &str,
    ) -> Self {
        Backtester {
            data,
            feed_latency_ns,
            order_update_latency_ns,
            python_mode: python_mode.to_string(),
            batch_ms,
            seed,
            trade_log_mode: trade_log_mode.to_string(),
        }
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

    /// Run a backtest by feeding ticks into the Rust engine and invoking the given Python strategy.
    ///
    /// Notes (Phase 2 WIP):
    /// - In tick mode, calls `strategy.on_tick(tick, ctx)` for each delivered tick.
    /// - `tick` is passed as a Python `dict` of primitive fields.
    /// - `ctx` is currently `None` (order submission plumbing is added in later tasks).
    #[pyo3(signature = (strategy))]
    pub fn run(&self, py: Python<'_>, strategy: Py<PyAny>) -> PyResult<BacktestResult> {
        let config = EngineConfig {
            feed_latency_ns: self.feed_latency_ns,
            order_update_latency_ns: self.order_update_latency_ns,
            mode: match self.python_mode.as_str() {
                "batch" => EngineMode::Batch,
                _ => EngineMode::Tick,
            },
            max_batch_ns: self.batch_ms.saturating_mul(1_000_000),
            auto_tune: false,
            seed: self.seed,
            trade_log_mode: match self.trade_log_mode.to_lowercase().as_str() {
                "all" => TradeLogMode::All,
                "summaryonly" => TradeLogMode::SummaryOnly,
                "none" => TradeLogMode::None,
                s if s.starts_with("ringbuffer") => TradeLogMode::RingBuffer(10000),
                _ => TradeLogMode::All,
            },
        };

        let strat = PyStrategy { obj: strategy };
        let latency_model = ConstantLatency {
            feed_latency_ns: self.feed_latency_ns,
            order_latency_ns: 0,
        };
        let mut engine: Engine<ConservativeQueue, PyStrategy, ConstantLatency> =
            Engine::new(ConservativeQueue, strat, config, latency_model);

        // Zero-copy ingestion path
        let data_any = self.data.bind(py);
        let data_dict = data_any.downcast::<PyDict>()?;

        let mut keys: Vec<String> = Vec::with_capacity(data_dict.len());
        for (k, _v) in data_dict.iter() {
            keys.push(k.extract::<String>()?);
        }
        keys.sort();

        let mut symbol_ids: BTreeMap<String, u32> = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            symbol_ids.insert(k.clone(), (i as u32) + 1);
        }

        for k in keys {
            let symbol_id = *symbol_ids.get(&k).unwrap();
            let lf_any = data_dict
                .get_item(&k)?
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing key"))?;

            // Convert Polars LazyFrame -> DataFrame -> Arrow Table -> Arrow Stream
            let df = lf_any.call_method0("collect")?;
            let table = df.call_method0("to_arrow")?;
            let stream = get_arrow_stream(&table)?;

            let source = ArrowTickSource::new(symbol_id, stream);
            engine.add_tick_source(Box::new(source));
        }

        engine.run();

        let trades = engine.trade_log().fills_vec();
        let stats = engine.stats();
        let all_pnl = full_pnl_history(engine.trade_log());
        let equity_curve = equity_curve_from_pnl_deltas(&all_pnl);
        Ok(BacktestResult {
            trades,
            stats,
            equity_curve,
        })
    }

    /// Run multiple backtests in parallel.
    ///
    /// Args:
    ///     strategies: List of strategy objects (must implement on_tick/on_ticks etc).
    ///
    /// Returns:
    ///     List of BacktestResult in the same order as inputs.
    ///
    /// The seed for each run is derived deterministically: `self.seed + index`.
    /// Data is parsed once and shared across runs (zero-copy for the engine, but cloned Ticks for queue).
    #[pyo3(signature = (strategies))]
    pub fn run_many(
        &self,
        py: Python<'_>,
        strategies: Vec<Py<PyAny>>,
    ) -> PyResult<Vec<BacktestResult>> {
        // 1. Pre-load data to avoid GIL during parallel execution setup.
        let events = parse_polars_data(py, &self.data, self.feed_latency_ns)?;

        // 2. Prepare configurations for each run.
        let n = strategies.len();

        let configs: Vec<EngineConfig> = (0..n)
            .map(|i| {
                EngineConfig {
                    feed_latency_ns: self.feed_latency_ns,
                    order_update_latency_ns: self.feed_latency_ns,
                    mode: match self.python_mode.as_str() {
                        "batch" => EngineMode::Batch,
                        _ => EngineMode::Tick,
                    },
                    max_batch_ns: self.batch_ms.saturating_mul(1_000_000),
                    auto_tune: false,
                    // Deterministic seed derivation
                    seed: self.seed.wrapping_add(i as u64),
                    trade_log_mode: match self.trade_log_mode.to_lowercase().as_str() {
                        "all" => TradeLogMode::All,
                        "summaryonly" => TradeLogMode::SummaryOnly,
                        "none" => TradeLogMode::None,
                        s if s.starts_with("ringbuffer") => TradeLogMode::RingBuffer(10000),
                        _ => TradeLogMode::All,
                    },
                }
            })
            .collect();

        let latency_ns = self.feed_latency_ns;

        // 3. Parallel execution releasing GIL.
        let results: Result<Vec<BacktestResult>, String> = py.allow_threads(move || {
            strategies
                .into_par_iter()
                .zip(configs.into_par_iter())
                .map(|(strategy, config)| {
                    let strat = PyStrategy { obj: strategy };
                    let latency_model = ConstantLatency {
                        feed_latency_ns: latency_ns,
                        order_latency_ns: 0,
                    };

                    let mut engine: Engine<ConservativeQueue, PyStrategy, ConstantLatency> =
                        Engine::new(ConservativeQueue, strat, config, latency_model);

                    // Push pre-loaded events.
                    for (ts, kind) in &events {
                        engine.push_event(*ts, kind.clone());
                    }

                    engine.run();

                    let trades = engine.trade_log().fills_vec();
                    let stats = engine.stats();
                    // full_pnl_history is not available here? It's a helper function, should be available if imported.
                    // But we are in a closure. 'full_pnl_history' is a function item, so it's global.
                    // However, we need to make sure 'full_pnl_history' is public/accessible in this module.
                    // It is imported at top of file.
                    let all_pnl = full_pnl_history(engine.trade_log());
                    let equity_curve = equity_curve_from_pnl_deltas(&all_pnl);

                    Ok(BacktestResult {
                        trades,
                        stats,
                        equity_curve,
                    })
                })
                .collect()
        });

        results.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))
    }

    /// Run backtest using an Arrow RecordBatch stream (zero-copy ingestion).
    ///
    /// Expects `stream` to implement the Arrow PyCapsule Interface (`__arrow_c_stream__`).
    #[pyo3(signature = (stream, strategy))]
    pub fn run_arrow(
        &self,
        py: Python<'_>,
        stream: Py<PyAny>,
        strategy: Py<PyAny>,
    ) -> PyResult<BacktestResult> {
        let config = EngineConfig {
            feed_latency_ns: self.feed_latency_ns,
            order_update_latency_ns: self.feed_latency_ns,
            mode: match self.python_mode.as_str() {
                "batch" => EngineMode::Batch,
                _ => EngineMode::Tick,
            },
            max_batch_ns: self.batch_ms.saturating_mul(1_000_000),
            auto_tune: false,
            seed: self.seed,
            trade_log_mode: match self.trade_log_mode.to_lowercase().as_str() {
                "all" => TradeLogMode::All,
                "summaryonly" => TradeLogMode::SummaryOnly,
                "none" => TradeLogMode::None,
                s if s.starts_with("ringbuffer") => TradeLogMode::RingBuffer(10000),
                _ => TradeLogMode::All,
            },
        };

        let strat = PyStrategy { obj: strategy };
        let latency_model = ConstantLatency {
            feed_latency_ns: self.feed_latency_ns,
            order_latency_ns: 0,
        };
        let mut engine: Engine<ConservativeQueue, PyStrategy, ConstantLatency> =
            Engine::new(ConservativeQueue, strat, config, latency_model);

        // Zero-copy ingestion
        let stream_bound = stream.bind(py);
        let arrow_stream = get_arrow_stream(stream_bound)?;

        // For now, assume single stream with symbol_id=1.
        // TODO: support multi-stream or map metadata.
        // Note: We currently do NOT use AsyncBatchIter here because calling PyArrow-backed
        // streams from a background thread can be unsafe (GIL/refcounting) depending on the
        // underlying implementation.
        let source = ArrowTickSource::new(1, arrow_stream);
        engine.add_tick_source(Box::new(source));

        engine.run();

        let trades = engine.trade_log().fills_vec();
        let stats = engine.stats();
        let all_pnl = full_pnl_history(engine.trade_log());
        let equity_curve = equity_curve_from_pnl_deltas(&all_pnl);
        Ok(BacktestResult {
            trades,
            stats,
            equity_curve,
        })
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

#[derive(Debug)]
struct PyStrategy {
    obj: Py<PyAny>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PyCommand {
    SubmitOrder {
        symbol_id: u32,
        side: i8,
        price: i64,
        qty: i64,
        seq: u64,
    },
}

#[pyclass]
#[derive(Debug)]
struct PyContext {
    ts_local: i64,
    next_seq: u64,
    commands: Vec<PyCommand>,
}

#[pymethods]
impl PyContext {
    pub fn ts_local(&self) -> i64 {
        self.ts_local
    }

    pub fn submit_order(&mut self, symbol_id: u32, side: i8, price: i64, qty: i64) -> PyResult<()> {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.commands.push(PyCommand::SubmitOrder {
            symbol_id,
            side,
            price,
            qty,
            seq,
        });
        Ok(())
    }
}

impl CoreStrategy for PyStrategy {
    fn on_tick(&mut self, tick: &Tick, _ctx: &mut backtester_core::Context<'_>) {
        Python::with_gil(|py| {
            let tick_obj = tick_to_pyobject(py, tick)?;
            let strategy = self.obj.bind(py);

            // Provide a minimal ctx object to support order submission and timestamp introspection.
            let py_ctx = Py::new(
                py,
                PyContext {
                    ts_local: _ctx.ts_local(),
                    next_seq: 0,
                    commands: Vec::new(),
                },
            )?;

            // Tick mode: prefer `on_tick`; fall back to `on_ticks([tick], ctx)` for compatibility.
            if strategy.hasattr("on_tick")? {
                strategy.call_method1("on_tick", (tick_obj, py_ctx.clone_ref(py)))?;
            } else if strategy.hasattr("on_ticks")? {
                let ticks = PyList::new_bound(py, [tick_obj]);
                strategy.call_method1("on_ticks", (ticks, py_ctx.clone_ref(py)))?;
            }

            apply_py_ctx_commands(py, py_ctx, _ctx)?;
            Ok::<(), PyErr>(())
        })
        .expect("python on_tick failed");
    }

    fn on_order_update(&mut self, report: &OrderReport, _ctx: &mut backtester_core::Context<'_>) {
        Python::with_gil(|py| {
            let report_obj = order_report_to_pyobject(py, report)?;
            let strategy = self.obj.bind(py);

            let py_ctx = Py::new(
                py,
                PyContext {
                    ts_local: _ctx.ts_local(),
                    next_seq: 0,
                    commands: Vec::new(),
                },
            )?;

            if strategy.hasattr("on_order_update")? {
                strategy.call_method1("on_order_update", (report_obj, py_ctx.clone_ref(py)))?;
            } else if strategy.hasattr("on_order_updates")? {
                let reports = PyList::new_bound(py, [report_obj]);
                strategy.call_method1("on_order_updates", (reports, py_ctx.clone_ref(py)))?;
            }

            apply_py_ctx_commands(py, py_ctx, _ctx)?;
            Ok::<(), PyErr>(())
        })
        .expect("python on_order_update failed");
    }

    fn on_ticks(&mut self, ticks: &[Tick], ctx: &mut CoreContext<'_>) {
        Python::with_gil(|py| {
            let strategy = self.obj.bind(py);
            let py_ctx = Py::new(
                py,
                PyContext {
                    ts_local: ctx.ts_local(),
                    next_seq: 0,
                    commands: Vec::new(),
                },
            )?;

            if strategy.hasattr("on_ticks")? {
                // Zero-copy optimization: Convert ticks to Arrow RecordBatch
                let n = ticks.len();
                let mut ts_builder = Int64Builder::with_capacity(n);
                let mut local_builder = Int64Builder::with_capacity(n);
                let mut seq_builder = UInt64Builder::with_capacity(n);
                let mut sym_builder = UInt32Builder::with_capacity(n);
                let mut price_builder = Int64Builder::with_capacity(n);
                let mut qty_builder = Int64Builder::with_capacity(n);
                let mut side_builder = Int8Builder::with_capacity(n);
                let mut flags_builder = arrow::array::UInt8Builder::with_capacity(n);

                for t in ticks {
                    ts_builder.append_value(t.ts_exchange);
                    local_builder.append_value(t.ts_local);
                    seq_builder.append_value(t.seq);
                    sym_builder.append_value(t.symbol_id);
                    price_builder.append_value(t.price);
                    qty_builder.append_value(t.qty);
                    side_builder.append_value(t.side.as_i8());
                    flags_builder.append_value(t.flags);
                }

                let schema = Schema::new(vec![
                    Field::new("ts_exchange", DataType::Int64, false),
                    Field::new("ts_local", DataType::Int64, false),
                    Field::new("seq", DataType::UInt64, false),
                    Field::new("symbol_id", DataType::UInt32, false),
                    Field::new("price", DataType::Int64, false),
                    Field::new("qty", DataType::Int64, false),
                    Field::new("side", DataType::Int8, false),
                    Field::new("flags", DataType::UInt8, false),
                ]);

                let batch = RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        Arc::new(ts_builder.finish()),
                        Arc::new(local_builder.finish()),
                        Arc::new(seq_builder.finish()),
                        Arc::new(sym_builder.finish()),
                        Arc::new(price_builder.finish()),
                        Arc::new(qty_builder.finish()),
                        Arc::new(side_builder.finish()),
                        Arc::new(flags_builder.finish()),
                    ],
                )
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

                let py_batch = batch.to_pyarrow(py)?;
                strategy.call_method1("on_ticks", (py_batch, py_ctx.clone_ref(py)))?;
            } else {
                // Fallback: call per-tick
                for t in ticks {
                    let obj = tick_to_pyobject(py, t)?;
                    if strategy.hasattr("on_tick")? {
                        strategy.call_method1("on_tick", (obj, py_ctx.clone_ref(py)))?;
                    }
                }
            }

            apply_py_ctx_commands(py, py_ctx, ctx)?;
            Ok::<(), PyErr>(())
        })
        .expect("python on_ticks failed");
    }

    fn on_order_updates(&mut self, reports: &[OrderReport], ctx: &mut CoreContext<'_>) {
        Python::with_gil(|py| {
            let strategy = self.obj.bind(py);
            let py_ctx = Py::new(
                py,
                PyContext {
                    ts_local: ctx.ts_local(),
                    next_seq: 0,
                    commands: Vec::new(),
                },
            )?;

            if strategy.hasattr("on_order_updates")? {
                let report_objs: Vec<Py<PyAny>> = reports
                    .iter()
                    .map(|r| order_report_to_pyobject(py, r))
                    .collect::<PyResult<_>>()?;
                let reports_list = PyList::new_bound(py, report_objs);
                strategy.call_method1("on_order_updates", (reports_list, py_ctx.clone_ref(py)))?;
            } else {
                // Fallback: call per-report
                for r in reports {
                    let obj = order_report_to_pyobject(py, r)?;
                    if strategy.hasattr("on_order_update")? {
                        strategy.call_method1("on_order_update", (obj, py_ctx.clone_ref(py)))?;
                    }
                }
            }

            apply_py_ctx_commands(py, py_ctx, ctx)?;
            Ok::<(), PyErr>(())
        })
        .expect("python on_order_updates failed");
    }
}

fn apply_py_ctx_commands(
    py: Python<'_>,
    py_ctx: Py<PyContext>,
    ctx: &mut CoreContext<'_>,
) -> PyResult<()> {
    let cmds = py_ctx.borrow(py).commands.clone();
    for c in cmds {
        match c {
            PyCommand::SubmitOrder {
                symbol_id,
                side,
                price,
                qty,
                seq,
            } => {
                let side = Side::try_from(side).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("invalid side: {e}"))
                })?;
                ctx.submit_order(Order {
                    order_id: 0,
                    ts_submit: ctx.ts_local(),
                    seq,
                    symbol_id,
                    side,
                    order_type: OrderType::Limit,
                    price,
                    qty,
                });
            }
        }
    }
    Ok(())
}

fn tick_to_pyobject(py: Python<'_>, tick: &Tick) -> PyResult<Py<PyAny>> {
    let t = PyTick {
        ts_exchange: tick.ts_exchange,
        ts_local: tick.ts_local,
        seq: tick.seq,
        symbol_id: tick.symbol_id,
        price: tick.price,
        qty: tick.qty,
        side: tick.side.as_i8(),
        flags: tick.flags,
    };
    Ok(Py::new(py, t)?.into_any())
}

fn trade_fill_to_pydict<'py>(py: Python<'py>, t: &TradeFill) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("ts_exchange", t.ts_exchange)?;
    d.set_item("symbol_id", t.symbol_id)?;
    d.set_item("order_id", t.order_id)?;
    d.set_item("side", t.side.as_i8())?;
    d.set_item("price", t.price)?;
    d.set_item("qty", t.qty)?;
    Ok(d)
}

fn backtest_stats_to_pydict<'py>(
    py: Python<'py>,
    s: &BacktestStats,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("total_trades", s.total_trades)?;
    d.set_item("win_rate", s.win_rate)?;
    d.set_item("profit_factor", s.profit_factor)?;
    d.set_item("sharpe_ratio", s.sharpe_ratio)?;
    d.set_item("sortino_ratio", s.sortino_ratio)?;
    d.set_item("max_drawdown", s.max_drawdown)?;
    d.set_item("max_drawdown_duration", s.max_drawdown_duration)?;
    d.set_item("calmar_ratio", s.calmar_ratio)?;
    d.set_item("total_pnl", s.total_pnl)?;
    d.set_item("avg_trade_pnl", s.avg_trade_pnl)?;
    d.set_item("avg_holding_period", s.avg_holding_period)?;
    d.set_item("total_fees_paid", s.total_fees_paid)?;
    Ok(d)
}

fn order_report_to_pyobject(py: Python<'_>, r: &OrderReport) -> PyResult<Py<PyAny>> {
    let rep = PyOrderReport {
        order_id: r.order_id,
        symbol_id: r.symbol_id,
        status: format!("{:?}", r.status),
        last_fill_qty: r.last_fill_qty,
        last_fill_price: r.last_fill_price,
        filled_qty: r.filled_qty,
        remaining_qty: r.remaining_qty,
        reason: r.reason.map(|s| s.to_string()),
    };
    Ok(Py::new(py, rep)?.into_any())
}

// Helper to parse data once.
fn parse_polars_data(
    py: Python<'_>,
    data: &Py<PyAny>,
    feed_latency_ns: i64,
) -> PyResult<Vec<(i64, EventKind)>> {
    let data_any = data.bind(py);
    let data_dict = data_any.downcast::<PyDict>()?;

    // Deterministic: do not rely on Python dict iteration order.
    let mut keys: Vec<String> = Vec::with_capacity(data_dict.len());
    for (k, _v) in data_dict.iter() {
        keys.push(k.extract::<String>()?);
    }
    keys.sort();

    // Deterministic mapping: symbol string -> u32 id.
    let mut symbol_ids: BTreeMap<String, u32> = BTreeMap::new();
    for (i, k) in keys.iter().enumerate() {
        symbol_ids.insert(k.clone(), (i as u32) + 1);
    }

    let mut events = Vec::new();

    for k in keys {
        let lf_any = data_dict
            .get_item(&k)?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing key"))?;

        // Collect LazyFrame -> DataFrame in Python (materialization strategy TBD).
        let df = lf_any.call_method0("collect")?;

        // Extract columns as Python lists for now.
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("as_series", false)?;
        let dict_any = df.call_method("to_dict", (), Some(&kwargs))?;
        let dict = dict_any.downcast::<PyDict>()?;

        let ts_exchange_any = dict
            .get_item("ts_exchange")?
            .or_else(|| dict.get_item("ts_event").ok().flatten())
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing ts_exchange"))?;
        let price_any = dict
            .get_item("price")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing price"))?;
        let qty_any = dict
            .get_item("qty")?
            .or_else(|| dict.get_item("size").ok().flatten())
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing qty"))?;
        let side_any = dict
            .get_item("side")?
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyKeyError, _>("missing side"))?;
        let seq_any = dict.get_item("seq")?;
        let ts_local_any = dict.get_item("ts_local")?;

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
        let ts_local_list = match ts_local_any.as_ref() {
            Some(any) => Some(any.downcast::<PyList>().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyTypeError, _>("ts_local must be a list")
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
        if let Some(ts_local) = ts_local_list {
            if ts_local.len() != n {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "ts_local length mismatch",
                ));
            }
        }

        let symbol_id = *symbol_ids.get(&k).expect("symbol_id");
        for i in 0..n {
            let ts_ex: i64 = ts_exchange.get_item(i)?.extract()?;
            let ts_local: i64 = match ts_local_list.as_ref() {
                Some(tsl) => tsl.get_item(i)?.extract()?,
                None => ts_ex + feed_latency_ns,
            };
            let p: i64 = price.get_item(i)?.extract()?;
            let q: i64 = qty.get_item(i)?.extract()?;
            let s: i8 = side.get_item(i)?.extract()?;
            let side = Side::try_from(s).map_err(|e| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("invalid side: {e}"))
            })?;
            let row_seq: u64 = match seq_list.as_ref() {
                Some(seq) => seq.get_item(i)?.extract()?,
                None => i as u64,
            };

            let truth_tick = Tick {
                ts_exchange: ts_ex,
                ts_local: ts_ex,
                seq: row_seq,
                symbol_id,
                price: p,
                qty: q,
                side,
                flags: 0x01, // trade-only for now
            };
            let delivered_tick = Tick {
                ts_exchange: ts_ex,
                ts_local,
                ..truth_tick
            };

            events.push((ts_ex, EventKind::Tick(truth_tick)));
            events.push((ts_local, EventKind::TickDelivery(delivered_tick)));
        }
    }
    events.sort_by_key(|(ts, _)| *ts);
    Ok(events)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Backtester>()?;
    m.add_class::<BacktestResult>()?;
    m.add_class::<PyTick>()?;
    m.add_class::<PyOrderReport>()?;
    m.add_function(wrap_pyfunction!(call_strategy_on_ticks, m)?)?;
    Ok(())
}
