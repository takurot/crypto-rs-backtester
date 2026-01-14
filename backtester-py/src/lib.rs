#![allow(unsafe_op_in_unsafe_fn)]

use std::collections::BTreeMap;

mod arrow_utils;

use arrow_utils::get_arrow_stream;
use backtester_core::engine::{EngineConfig, EngineMode, Strategy as CoreStrategy};
use backtester_core::latency_model::ConstantLatency;
use backtester_core::queue_model::ConservativeQueue;
use backtester_core::stats::{equity_curve_from_pnl_deltas, pnl_deltas_from_fills};
use backtester_core::tick_source::ArrowTickSource; // Import TickSource types
use backtester_core::types::{Order, OrderReport, OrderType, Side, Tick};
use backtester_core::{BacktestStats, TradeFill};
use backtester_core::{Context as CoreContext, Engine, EventKind};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList, PyModule};

#[pyclass]
pub struct Backtester {
    data: Py<PyAny>,
    seed: u64,
    python_mode: String,
    batch_ms: i64,
    feed_latency_ns: i64,
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
        let ts_exchange: Vec<i64> = self.trades.iter().map(|f| f.ts_exchange).collect();
        let symbol_id: Vec<u32> = self.trades.iter().map(|f| f.symbol_id).collect();
        let order_id: Vec<u64> = self.trades.iter().map(|f| f.order_id).collect();
        let side: Vec<i8> = self.trades.iter().map(|f| f.side.as_i8()).collect();
        let price: Vec<i64> = self.trades.iter().map(|f| f.price).collect();
        let qty: Vec<i64> = self.trades.iter().map(|f| f.qty).collect();

        let d = PyDict::new_bound(py);
        d.set_item("ts_exchange", ts_exchange)?;
        d.set_item("symbol_id", symbol_id)?;
        d.set_item("order_id", order_id)?;
        d.set_item("side", side)?;
        d.set_item("price", price)?;
        d.set_item("qty", qty)?;
        d.set_item("_len", n)?;
        Ok(d)
    }

    /// Return equity curve as a PyArrow-compatible dict of arrays for zero-copy access.
    /// Schema: ts_exchange (i64), equity (i64)
    pub fn equity_curve_df<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let n = self.equity_curve.len();
        let ts_exchange: Vec<i64> = self.equity_curve.iter().map(|(ts, _)| *ts).collect();
        let equity: Vec<i64> = self.equity_curve.iter().map(|(_, eq)| *eq).collect();

        let d = PyDict::new_bound(py);
        d.set_item("ts_exchange", ts_exchange)?;
        d.set_item("equity", equity)?;
        d.set_item("_len", n)?;
        Ok(d)
    }
}

#[pymethods]
impl Backtester {
    #[new]
    #[pyo3(signature = (data, seed, python_mode="tick".to_string(), batch_ms=100, feed_latency_ns=0))]
    pub fn new(
        data: Py<PyAny>,
        seed: u64,
        python_mode: String,
        batch_ms: i64,
        feed_latency_ns: i64,
    ) -> Self {
        Self {
            data,
            seed,
            python_mode,
            batch_ms,
            feed_latency_ns,
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
            order_update_latency_ns: self.feed_latency_ns,
            mode: match self.python_mode.as_str() {
                "batch" => EngineMode::Batch,
                _ => EngineMode::Tick,
            },
            max_batch_ns: self.batch_ms.saturating_mul(1_000_000),
            seed: self.seed,
            ..Default::default()
        };

        let strat = PyStrategy { obj: strategy };
        let latency_model = ConstantLatency {
            feed_latency_ns: self.feed_latency_ns,
            order_latency_ns: 0,
        };
        let mut engine: Engine<ConservativeQueue, PyStrategy, ConstantLatency> =
            Engine::new(ConservativeQueue, strat, config, latency_model);

        schedule_ticks_from_python_polars(py, &self.data, self.feed_latency_ns, &mut engine)?;
        engine.run();

        let trades = engine.trade_log().fills().to_vec();
        let stats = engine.stats();
        let pnl_deltas = pnl_deltas_from_fills(&trades);
        let all_pnl: Vec<_> = pnl_deltas.iter().map(|d| (d.ts_exchange, d.pnl)).collect();
        let equity_curve = equity_curve_from_pnl_deltas(&all_pnl);
        Ok(BacktestResult {
            trades,
            stats,
            equity_curve,
        })
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
            seed: self.seed,
            ..Default::default()
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
        let source = ArrowTickSource::new(1, arrow_stream);
        engine.add_tick_source(Box::new(source));

        engine.run();

        let trades = engine.trade_log().fills().to_vec();
        let stats = engine.stats();
        let pnl_deltas = pnl_deltas_from_fills(&trades);
        let all_pnl: Vec<_> = pnl_deltas.iter().map(|d| (d.ts_exchange, d.pnl)).collect();
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
            let tick_dict = tick_to_pydict(py, tick)?;
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
                strategy.call_method1("on_tick", (tick_dict, py_ctx.clone_ref(py)))?;
            } else if strategy.hasattr("on_ticks")? {
                let ticks = PyList::new_bound(py, [tick_dict]);
                strategy.call_method1("on_ticks", (ticks, py_ctx.clone_ref(py)))?;
            }

            apply_py_ctx_commands(py, py_ctx, _ctx)?;
            Ok::<(), PyErr>(())
        })
        .expect("python on_tick failed");
    }

    fn on_order_update(&mut self, report: &OrderReport, _ctx: &mut backtester_core::Context<'_>) {
        Python::with_gil(|py| {
            let report_dict = order_report_to_pydict(py, report)?;
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
                strategy.call_method1("on_order_update", (report_dict, py_ctx.clone_ref(py)))?;
            } else if strategy.hasattr("on_order_updates")? {
                let reports = PyList::new_bound(py, [report_dict]);
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
                let tick_dicts: Vec<Bound<'_, PyDict>> = ticks
                    .iter()
                    .map(|t| tick_to_pydict(py, t))
                    .collect::<PyResult<_>>()?;
                let ticks_list = PyList::new_bound(py, tick_dicts);
                strategy.call_method1("on_ticks", (ticks_list, py_ctx.clone_ref(py)))?;
            } else {
                // Fallback: call per-tick
                for t in ticks {
                    let d = tick_to_pydict(py, t)?;
                    if strategy.hasattr("on_tick")? {
                        strategy.call_method1("on_tick", (d, py_ctx.clone_ref(py)))?;
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
                let report_dicts: Vec<Bound<'_, PyDict>> = reports
                    .iter()
                    .map(|r| order_report_to_pydict(py, r))
                    .collect::<PyResult<_>>()?;
                let reports_list = PyList::new_bound(py, report_dicts);
                strategy.call_method1("on_order_updates", (reports_list, py_ctx.clone_ref(py)))?;
            } else {
                // Fallback: call per-report
                for r in reports {
                    let d = order_report_to_pydict(py, r)?;
                    if strategy.hasattr("on_order_update")? {
                        strategy.call_method1("on_order_update", (d, py_ctx.clone_ref(py)))?;
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

fn tick_to_pydict<'py>(py: Python<'py>, tick: &Tick) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("ts_exchange", tick.ts_exchange)?;
    d.set_item("ts_local", tick.ts_local)?;
    d.set_item("seq", tick.seq)?;
    d.set_item("symbol_id", tick.symbol_id)?;
    d.set_item("price", tick.price)?;
    d.set_item("qty", tick.qty)?;
    d.set_item("side", tick.side.as_i8())?;
    d.set_item("flags", tick.flags)?;
    Ok(d)
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

fn order_report_to_pydict<'py>(py: Python<'py>, r: &OrderReport) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    d.set_item("order_id", r.order_id)?;
    d.set_item("symbol_id", r.symbol_id)?;
    d.set_item("status", format!("{:?}", r.status))?;
    d.set_item("last_fill_qty", r.last_fill_qty)?;
    d.set_item("last_fill_price", r.last_fill_price)?;
    d.set_item("filled_qty", r.filled_qty)?;
    d.set_item("remaining_qty", r.remaining_qty)?;
    d.set_item("reason", r.reason.map(|s| s.to_string()))?;
    Ok(d)
}

fn schedule_ticks_from_python_polars(
    py: Python<'_>,
    data: &Py<PyAny>,
    feed_latency_ns: i64,
    engine: &mut Engine<ConservativeQueue, PyStrategy, ConstantLatency>,
) -> PyResult<()> {
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

            engine.push_event(ts_ex, EventKind::Tick(truth_tick));
            engine.push_event(ts_local, EventKind::TickDelivery(delivered_tick));
        }
    }

    Ok(())
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Backtester>()?;
    m.add_class::<BacktestResult>()?;
    m.add_function(wrap_pyfunction!(call_strategy_on_ticks, m)?)?;
    Ok(())
}
