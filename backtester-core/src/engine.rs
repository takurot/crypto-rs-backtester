use std::collections::BTreeMap;

use crate::account::Account;
use crate::event::{Event, EventId, EventKind};
use crate::event_queue::EventQueue;
use crate::exchange_simulator::ExchangeSimulator;
use crate::latency_model::LatencyModel;
use crate::queue_model::QueueModel;
use crate::rng::make_small_rng;
use crate::stats::{BacktestStats, TradeFill, TradeLog, calculate_stats};
use crate::types::{FundingEvent, Order, OrderReport, Tick, TsLocalNs, TsSimNs};
use rand::rngs::SmallRng;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineMode {
    Tick,
    Batch,
}

#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    /// Constant feed latency (ns). Used to translate `ts_exchange -> ts_local` when missing,
    /// and as the default delivery latency for order updates.
    pub feed_latency_ns: i64,
    /// Delivery latency (ns) applied to exchange-side order updates (fills/cancels/rejects).
    pub order_update_latency_ns: i64,
    pub mode: EngineMode,
    /// Maximum batch duration in nanoseconds (Batch mode only).
    pub max_batch_ns: i64,
    /// RNG seed for all stochastic components owned by the engine.
    pub seed: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            feed_latency_ns: 0,
            order_update_latency_ns: 0,
            mode: EngineMode::Tick,
            max_batch_ns: 0,
            seed: 0,
        }
    }
}

/// Feed-delayed market state visible to strategies.
#[derive(Debug, Default, Clone)]
pub struct MarketView {
    last_trade_by_symbol: BTreeMap<u32, Tick>,
}

impl MarketView {
    pub fn last_trade(&self, symbol_id: u32) -> Option<&Tick> {
        self.last_trade_by_symbol.get(&symbol_id)
    }

    pub fn on_tick_delivery(&mut self, tick: Tick) {
        self.last_trade_by_symbol.insert(tick.symbol_id, tick);
    }
}

/// Strategy interface (Rust-native; Python will be adapted via a wrapper in `backtester-py`).
pub trait Strategy {
    fn on_tick(&mut self, tick: &Tick, ctx: &mut Context<'_>);

    fn on_ticks(&mut self, ticks: &[Tick], ctx: &mut Context<'_>) {
        for t in ticks {
            self.on_tick(t, ctx);
        }
    }

    fn on_order_update(&mut self, report: &OrderReport, ctx: &mut Context<'_>);

    fn on_order_updates(&mut self, reports: &[OrderReport], ctx: &mut Context<'_>) {
        for r in reports {
            self.on_order_update(r, ctx);
        }
    }

    fn on_funding(&mut self, _event: &FundingEvent, _ctx: &mut Context<'_>) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    SubmitOrder(Order),
    CancelOrder { order_id: u64 },
}

#[derive(Debug)]
pub struct Context<'a> {
    ts_local: TsLocalNs,
    market: &'a MarketView,
    commands: Vec<Command>,
}

impl<'a> Context<'a> {
    pub fn new(ts_local: TsLocalNs, market: &'a MarketView) -> Self {
        Self {
            ts_local,
            market,
            commands: Vec::new(),
        }
    }

    pub fn ts_local(&self) -> TsLocalNs {
        self.ts_local
    }

    pub fn market(&self) -> &'a MarketView {
        self.market
    }

    pub fn submit_order(&mut self, order: Order) {
        self.commands.push(Command::SubmitOrder(order));
    }

    pub fn cancel_order(&mut self, order_id: u64) {
        self.commands.push(Command::CancelOrder { order_id });
    }

    pub fn into_commands(self) -> Vec<Command> {
        self.commands
    }
}

/// Discrete-event simulation engine.
#[derive(Debug)]
pub struct Engine<Q: QueueModel + Clone, S: Strategy, L: LatencyModel> {
    config: EngineConfig,
    queue: EventQueue,
    /// Prototype queue model used when instantiating a new per-symbol exchange simulator.
    queue_model: Q,
    /// One exchange simulator per `(exchange, symbol)` stream (represented by `symbol_id`).
    exchanges: BTreeMap<u32, ExchangeSimulator<Q>>,
    /// Route `order_id -> symbol_id` so ACK/cancel events can find the right exchange instance.
    order_symbol_by_id: BTreeMap<u64, u32>,
    next_order_id: u64,
    strategy: S,
    latency_model: L,
    rng: SmallRng,
    account: Account,
    trade_log: TradeLog,
    market: MarketView,
    truth_last_trade_by_symbol: BTreeMap<u32, Tick>,
    next_event_seq: u64,
    now_ts_sim: TsSimNs,
    // Batch-mode buffering (Phase 2).
    tick_buffer: Vec<Tick>,
    report_buffer: Vec<OrderReport>,
    active_batch_timer_id: Option<u64>,
    next_timer_id: u64,
}

impl<Q: QueueModel + Clone, S: Strategy, L: LatencyModel> Engine<Q, S, L> {
    pub fn new(queue_model: Q, strategy: S, config: EngineConfig, latency_model: L) -> Self {
        Self {
            config,
            queue: EventQueue::new(),
            queue_model,
            exchanges: BTreeMap::new(),
            order_symbol_by_id: BTreeMap::new(),
            next_order_id: 1,
            strategy,
            latency_model,
            rng: make_small_rng(config.seed),
            account: Account::default(),
            trade_log: TradeLog::default(),
            market: MarketView::default(),
            truth_last_trade_by_symbol: BTreeMap::new(),
            next_event_seq: 0,
            now_ts_sim: 0,
            tick_buffer: Vec::new(),
            report_buffer: Vec::new(),
            active_batch_timer_id: None,
            next_timer_id: 1,
        }
    }

    pub fn strategy(&self) -> &S {
        &self.strategy
    }

    pub fn market_view(&self) -> &MarketView {
        &self.market
    }

    pub fn account(&self) -> &Account {
        &self.account
    }

    pub fn trade_log(&self) -> &TradeLog {
        &self.trade_log
    }

    pub fn stats(&self) -> BacktestStats {
        calculate_stats(&self.trade_log)
    }

    fn exchange_mut(&mut self, symbol_id: u32) -> &mut ExchangeSimulator<Q> {
        let qm = self.queue_model.clone();
        self.exchanges
            .entry(symbol_id)
            .or_insert_with(|| ExchangeSimulator::new(qm))
    }

    pub fn push_event(&mut self, ts_sim: TsSimNs, kind: EventKind) {
        let id = EventId {
            ts_sim,
            seq: self.next_event_seq,
        };
        self.next_event_seq = self.next_event_seq.wrapping_add(1);
        self.queue.push(Event { id, kind });
    }

    pub fn run(&mut self) {
        loop {
            while self.step().is_some() {}

            // Final flush for batch mode: if we have buffered deliveries, wake the strategy once.
            if self.config.mode == EngineMode::Batch
                && (!self.tick_buffer.is_empty() || !self.report_buffer.is_empty())
            {
                self.flush_strategy(self.now_ts_sim);
                // flushing may schedule new events (orders), so keep running until stable
                continue;
            }
            break;
        }
    }

    pub fn step(&mut self) -> Option<Event> {
        let event = self.queue.pop()?;
        self.now_ts_sim = event.ts_sim();

        let mut wakeup_requested = false;
        match event.kind {
            EventKind::Tick(tick) => {
                self.truth_last_trade_by_symbol.insert(tick.symbol_id, tick);
                // Market truth drives the exchange simulator only.
                let mut fills: Vec<(Order, i64)> = Vec::new();
                let mut trade_fills: Vec<TradeFill> = Vec::new();
                let reports = {
                    // Scope the mutable borrow of `self.exchanges` to avoid borrow conflicts.
                    let ex = self.exchange_mut(tick.symbol_id);
                    let reports = ex.on_trade(tick);
                    for r in &reports {
                        if r.last_fill_qty > 0
                            && let Some(order) = ex.get_order(r.order_id)
                        {
                            fills.push((order, r.last_fill_qty));
                            trade_fills.push(TradeFill {
                                ts_exchange: tick.ts_exchange,
                                symbol_id: r.symbol_id,
                                order_id: r.order_id,
                                side: order.side,
                                price: r.last_fill_price,
                                qty: r.last_fill_qty,
                            });
                        }
                    }
                    reports
                };

                for f in trade_fills {
                    self.trade_log.push_fill(f);
                }

                for (order, fill_qty) in fills {
                    self.account.on_fill(&order, fill_qty);
                }

                let ts_delivery = tick.ts_exchange + self.config.order_update_latency_ns;
                for r in reports {
                    self.push_event(ts_delivery, EventKind::OrderReport(r));
                }
            }
            EventKind::TickDelivery(tick) => {
                // Strategy view updates only on delivered ticks.
                self.market.on_tick_delivery(tick);

                match self.config.mode {
                    EngineMode::Tick => {
                        let mut ctx = Context::new(tick.ts_local, &self.market);
                        self.strategy.on_tick(&tick, &mut ctx);
                        self.handle_commands(ctx.into_commands(), tick.ts_local);
                    }
                    EngineMode::Batch => {
                        self.tick_buffer.push(tick);

                        // Ensure time-based wakeup even if no future tick crosses the boundary.
                        if self.active_batch_timer_id.is_none() && self.config.max_batch_ns > 0 {
                            let timer_id = self.next_timer_id;
                            self.next_timer_id = self.next_timer_id.wrapping_add(1);
                            self.active_batch_timer_id = Some(timer_id);
                            let ts_deadline = tick.ts_local + self.config.max_batch_ns;
                            self.push_event(ts_deadline, EventKind::Timer { timer_id });
                        }
                    }
                }
            }
            EventKind::L2Update(update) => {
                self.exchange_mut(update.symbol_id).apply_l2_update(&update);
            }
            EventKind::Order(order) => {
                let order_id = order.order_id;
                self.exchange_mut(order.symbol_id).submit_order(order);
                let dt = self
                    .latency_model
                    .sample_order_latency(self.now_ts_sim, &mut self.rng)
                    .max(0);
                let ts_ack = self.now_ts_sim.saturating_add(dt);
                self.push_event(ts_ack, EventKind::OrderAck { order_id });
            }
            EventKind::OrderAck { order_id } => {
                if let Some(&symbol_id) = self.order_symbol_by_id.get(&order_id) {
                    let _ = self.exchange_mut(symbol_id).ack_new(order_id);
                }
            }
            EventKind::OrderCancel { order_id } => {
                let Some(&symbol_id) = self.order_symbol_by_id.get(&order_id) else {
                    // Unknown order_id: ignore.
                    return Some(event);
                };
                if self.exchange_mut(symbol_id).cancel_order(order_id).is_ok() {
                    let dt = self
                        .latency_model
                        .sample_order_latency(self.now_ts_sim, &mut self.rng)
                        .max(0);
                    let ts_ack = self.now_ts_sim.saturating_add(dt);
                    self.push_event(ts_ack, EventKind::OrderCancelAck { order_id });
                }
            }
            EventKind::OrderCancelAck { order_id } => {
                if let Some(&symbol_id) = self.order_symbol_by_id.get(&order_id)
                    && let Ok(report) = self.exchange_mut(symbol_id).ack_cancel(order_id)
                {
                    let ts_delivery = self.now_ts_sim + self.config.order_update_latency_ns;
                    self.push_event(ts_delivery, EventKind::OrderReport(report));
                }
            }
            EventKind::OrderReport(report) => {
                if report.status.is_terminal() {
                    if let Some(&symbol_id) = self.order_symbol_by_id.get(&report.order_id) {
                        self.exchange_mut(symbol_id).remove_order(report.order_id);
                    }
                    self.order_symbol_by_id.remove(&report.order_id);
                }

                match self.config.mode {
                    EngineMode::Tick => {
                        let mut ctx = Context::new(self.now_ts_sim, &self.market);
                        self.strategy.on_order_update(&report, &mut ctx);
                        self.handle_commands(ctx.into_commands(), self.now_ts_sim);
                    }
                    EngineMode::Batch => {
                        self.report_buffer.push(report);
                        wakeup_requested = true;
                    }
                }
            }
            EventKind::Funding(event) => {
                let mark_price = self
                    .truth_last_trade_by_symbol
                    .get(&event.symbol_id)
                    .map(|t| t.price)
                    .unwrap_or(0);
                let pnl = self.account.apply_funding(&event, mark_price);
                self.trade_log.push_pnl_event(crate::stats::PnlEvent {
                    ts_exchange: event.ts_exchange,
                    pnl,
                });

                match self.config.mode {
                    EngineMode::Tick => {
                        let mut ctx = Context::new(self.now_ts_sim, &self.market);
                        self.strategy.on_funding(&event, &mut ctx);
                        self.handle_commands(ctx.into_commands(), self.now_ts_sim);
                    }
                    EngineMode::Batch => {
                        // Funding is treated as a wakeup condition in Phase 2.
                        let mut ctx = Context::new(self.now_ts_sim, &self.market);
                        self.strategy.on_funding(&event, &mut ctx);
                        self.handle_commands(ctx.into_commands(), self.now_ts_sim);
                        wakeup_requested = true;
                    }
                }
            }
            EventKind::Timer { timer_id } => {
                if self.config.mode == EngineMode::Batch
                    && self.active_batch_timer_id == Some(timer_id)
                {
                    // Time-based batch wakeup.
                    self.active_batch_timer_id = None;
                    wakeup_requested = true;
                }
            }
        }

        if self.config.mode == EngineMode::Batch && wakeup_requested {
            let next_ts = self.queue.peek().map(|e| e.ts_sim());
            if next_ts != Some(self.now_ts_sim) {
                self.flush_strategy(self.now_ts_sim);
            }
        }

        Some(event)
    }

    fn handle_commands(&mut self, commands: Vec<Command>, ts_local: TsLocalNs) {
        for c in commands {
            match c {
                Command::SubmitOrder(mut order) => {
                    // Default: schedule the order to arrive at the exchange immediately at `ts_local`.
                    // `order_id` is assigned by the exchange simulator.
                    order.ts_submit = ts_local;
                    // Engine-assigned, globally unique order_id for deterministic routing.
                    let order_id = self.next_order_id;
                    self.next_order_id = self.next_order_id.wrapping_add(1);
                    order.order_id = order_id;
                    self.order_symbol_by_id.insert(order_id, order.symbol_id);
                    self.push_event(ts_local, EventKind::Order(order));
                }
                Command::CancelOrder { order_id } => {
                    self.push_event(ts_local, EventKind::OrderCancel { order_id });
                }
            }
        }
    }

    fn flush_strategy(&mut self, ts_local: TsLocalNs) {
        if self.tick_buffer.is_empty() && self.report_buffer.is_empty() {
            return;
        }

        // Any early flush invalidates the pending time-based wakeup timer.
        self.active_batch_timer_id = None;

        let mut ctx = Context::new(ts_local, &self.market);

        if !self.tick_buffer.is_empty() {
            self.strategy.on_ticks(&self.tick_buffer, &mut ctx);
            self.tick_buffer.clear();
        }

        if !self.report_buffer.is_empty() {
            self.strategy
                .on_order_updates(&self.report_buffer, &mut ctx);
            self.report_buffer.clear();
        }

        self.handle_commands(ctx.into_commands(), ts_local);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures;
    use crate::latency_model::ConstantLatency;
    use crate::queue_model::ConservativeQueue;
    use crate::types::{OrderState, OrderType, Side, Tick};

    #[derive(Debug, Default)]
    struct RecordingStrategy {
        submitted: bool,
        reports: Vec<OrderReport>,
    }

    impl Strategy for RecordingStrategy {
        fn on_tick(&mut self, tick: &Tick, ctx: &mut Context<'_>) {
            if self.submitted {
                return;
            }
            self.submitted = true;
            ctx.submit_order(Order {
                order_id: 0,
                ts_submit: ctx.ts_local(),
                seq: 0,
                symbol_id: tick.symbol_id,
                side: Side::Buy,
                order_type: OrderType::Limit,
                price: tick.price,
                qty: tick.qty,
            });
        }

        fn on_order_update(&mut self, report: &OrderReport, _ctx: &mut Context<'_>) {
            self.reports.push(*report);
        }
    }

    #[derive(Debug, Default)]
    struct NoopStrategy;

    impl Strategy for NoopStrategy {
        fn on_tick(&mut self, _tick: &Tick, _ctx: &mut Context<'_>) {}
        fn on_order_update(&mut self, _report: &OrderReport, _ctx: &mut Context<'_>) {}
    }

    #[test]
    fn test_engine_run_smoke_deterministic_sequence() {
        let config = EngineConfig {
            feed_latency_ns: 1_000,
            order_update_latency_ns: 1_000, // deliver fills at the same latency as the feed
            mode: EngineMode::Tick,
            max_batch_ns: 0,
            seed: 42,
        };

        let strategy = RecordingStrategy::default();
        let latency_model = ConstantLatency {
            feed_latency_ns: config.feed_latency_ns,
            order_latency_ns: 0,
        };
        let mut eng = Engine::new(ConservativeQueue, strategy, config, latency_model);

        // Tick #0 (truth at ts_exchange=1_000, delivered at ts_local=2_000)
        let t0_truth = fixtures::tick_trade(1_000, 1_000, 0);
        let t0_delivery = Tick {
            ts_exchange: t0_truth.ts_exchange,
            ts_local: t0_truth.ts_exchange + config.feed_latency_ns,
            ..t0_truth
        };
        eng.push_event(1_000, EventKind::Tick(t0_truth));
        eng.push_event(2_000, EventKind::TickDelivery(t0_delivery));

        // Tick #1 triggers the fill (truth at 3_000, delivered at 4_000)
        let t1_truth = Tick {
            ts_exchange: 3_000,
            ts_local: 3_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100_00000000,
            qty: 1_00000000,
            side: Side::Sell, // against the buy order
            flags: 0x01,
        };
        let t1_delivery = Tick {
            ts_exchange: t1_truth.ts_exchange,
            ts_local: t1_truth.ts_exchange + config.feed_latency_ns,
            ..t1_truth
        };
        eng.push_event(3_000, EventKind::Tick(t1_truth));
        eng.push_event(4_000, EventKind::TickDelivery(t1_delivery));

        eng.run();

        let reports = &eng.strategy.reports;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].order_id, 1);
        assert_eq!(reports[0].status, OrderState::Filled);
        assert_eq!(reports[0].last_fill_qty, 1_00000000);
        assert_eq!(reports[0].remaining_qty, 0);
    }

    #[test]
    fn test_marketview_no_lookahead_with_feed_latency() {
        let config = EngineConfig {
            feed_latency_ns: 1_000,
            order_update_latency_ns: 1_000,
            mode: EngineMode::Tick,
            max_batch_ns: 0,
            seed: 42,
        };
        let strategy = NoopStrategy;
        let latency_model = ConstantLatency {
            feed_latency_ns: config.feed_latency_ns,
            order_latency_ns: 0,
        };
        let mut eng = Engine::new(ConservativeQueue, strategy, config, latency_model);

        let t0_truth = fixtures::tick_trade(1_000, 1_000, 0);
        let t0_delivery = Tick {
            ts_exchange: t0_truth.ts_exchange,
            ts_local: t0_truth.ts_exchange + config.feed_latency_ns,
            ..t0_truth
        };
        eng.push_event(1_000, EventKind::Tick(t0_truth));
        eng.push_event(2_000, EventKind::TickDelivery(t0_delivery));

        // Process truth tick first: MarketView must not update.
        eng.step().expect("truth tick");
        assert_eq!(
            eng.market_view().last_trade(fixtures::SYMBOL_ID_BTC_USDT),
            None
        );

        // Process delivery: MarketView updates exactly at ts_local.
        eng.step().expect("delivery tick");
        let last = eng
            .market_view()
            .last_trade(fixtures::SYMBOL_ID_BTC_USDT)
            .expect("last trade");
        assert_eq!(last.ts_exchange, 1_000);
        assert_eq!(last.ts_local, 2_000);
    }

    #[test]
    fn test_funding_applied_at_scheduled_ts_exchange() {
        let config = EngineConfig {
            feed_latency_ns: 0,
            order_update_latency_ns: 0,
            mode: EngineMode::Tick,
            max_batch_ns: 0,
            seed: 42,
        };
        let strategy = RecordingStrategy::default();
        let latency_model = ConstantLatency {
            feed_latency_ns: config.feed_latency_ns,
            order_latency_ns: 0,
        };
        let mut eng = Engine::new(ConservativeQueue, strategy, config, latency_model);

        // Tick #0: submit the order on delivery at ts=1_000.
        let t0_truth = fixtures::tick_trade(1_000, 1_000, 0);
        let t0_delivery = Tick {
            ts_exchange: t0_truth.ts_exchange,
            ts_local: t0_truth.ts_exchange,
            ..t0_truth
        };
        eng.push_event(1_000, EventKind::Tick(t0_truth));
        eng.push_event(1_000, EventKind::TickDelivery(t0_delivery));

        // Tick #1: fills the buy order (against = sell).
        let t1_truth = Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100_00000000,
            qty: 1_00000000,
            side: Side::Sell,
            flags: 0x01,
        };
        eng.push_event(2_000, EventKind::Tick(t1_truth));

        // Funding at 3_000 with rate=0.0001.
        let f = FundingEvent {
            ts_exchange: 3_000,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            rate: 10_000,
        };
        eng.push_event(3_000, EventKind::Funding(f));

        // Before funding event is processed, funding PnL must be 0.
        while let Some(ev) = eng.step() {
            if ev.ts_sim() < 3_000 {
                assert_eq!(eng.account().total_funding_pnl(), 0);
                continue;
            }
            match ev.kind {
                EventKind::Funding(_) => break,
                _ => continue,
            }
        }

        // Notional=100.00, rate=0.0001 => pay 0.01
        assert_eq!(eng.account().total_funding_pnl(), -1_000_000);
    }

    #[test]
    fn test_engine_order_lifecycle_cleanup() {
        let config = EngineConfig {
            feed_latency_ns: 0,
            order_update_latency_ns: 0,
            mode: EngineMode::Tick,
            max_batch_ns: 0,
            seed: 42,
        };
        let strategy = RecordingStrategy::default();
        let mut eng = Engine::new(ConservativeQueue, strategy, config, ConstantLatency {
            feed_latency_ns: 0,
            order_latency_ns: 0,
        });

        // 1. Submit order
        let t0 = fixtures::tick_trade(1_000, 1_000, 0);
        eng.push_event(1_000, EventKind::TickDelivery(t0));
        eng.step().expect("tick delivery");
        eng.step().expect("order arrival at exchange");
        
        assert_eq!(eng.order_symbol_by_id.len(), 1);
        let order_id = 1;
        assert!(eng.exchanges.get(&fixtures::SYMBOL_ID_BTC_USDT).unwrap().get_order(order_id).is_some());

        // 2. ACK order
        eng.step().expect("ack");
        
        // 3. Fill order
        let t1 = Tick {
            ts_exchange: 2_000,
            ts_local: 2_000,
            seq: 1,
            symbol_id: fixtures::SYMBOL_ID_BTC_USDT,
            price: 100_00000000,
            qty: 1_00000000,
            side: Side::Sell,
            flags: 0x01,
        };
        eng.push_event(2_000, EventKind::Tick(t1));
        eng.step().expect("tick truth");
        
        // At this point OrderReport(Filled) is scheduled but not processed.
        assert_eq!(eng.order_symbol_by_id.len(), 1);

        // 4. Process OrderReport
        eng.step().expect("report");
        
        // Cleanup should have happened.
        assert_eq!(eng.order_symbol_by_id.len(), 0);
        assert!(eng.exchanges.get(&fixtures::SYMBOL_ID_BTC_USDT).unwrap().get_order(order_id).is_none());
    }
}
