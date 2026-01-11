use std::collections::BTreeMap;

use crate::event::{Event, EventId, EventKind};
use crate::event_queue::EventQueue;
use crate::exchange_simulator::ExchangeSimulator;
use crate::queue_model::QueueModel;
use crate::types::{FundingEvent, Order, OrderReport, Tick, TsLocalNs, TsSimNs};

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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            feed_latency_ns: 0,
            order_update_latency_ns: 0,
            mode: EngineMode::Tick,
            max_batch_ns: 0,
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
pub struct Engine<Q: QueueModel, S: Strategy> {
    config: EngineConfig,
    queue: EventQueue,
    exchange: ExchangeSimulator<Q>,
    strategy: S,
    market: MarketView,
    next_event_seq: u64,
    now_ts_sim: TsSimNs,
    // Batch-mode buffering (Phase 2).
    tick_buffer: Vec<Tick>,
    report_buffer: Vec<OrderReport>,
    active_batch_timer_id: Option<u64>,
    next_timer_id: u64,
}

impl<Q: QueueModel, S: Strategy> Engine<Q, S> {
    pub fn new(exchange: ExchangeSimulator<Q>, strategy: S, config: EngineConfig) -> Self {
        Self {
            config,
            queue: EventQueue::new(),
            exchange,
            strategy,
            market: MarketView::default(),
            next_event_seq: 0,
            now_ts_sim: 0,
            tick_buffer: Vec::new(),
            report_buffer: Vec::new(),
            active_batch_timer_id: None,
            next_timer_id: 1,
        }
    }

    pub fn market_view(&self) -> &MarketView {
        &self.market
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
                // Market truth drives the exchange simulator only.
                let reports = self.exchange.on_trade(tick);
                for r in reports {
                    let ts_delivery = tick.ts_exchange + self.config.order_update_latency_ns;
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
                self.exchange.apply_l2_update(&update);
            }
            EventKind::Order(order) => {
                let order_id = self.exchange.submit_order(order);
                let _ = self.exchange.ack_new(order_id);
            }
            EventKind::OrderAck { order_id } => {
                let _ = self.exchange.ack_new(order_id);
            }
            EventKind::OrderCancel { order_id: _ } => {
                // Cancel path is implemented in later phases (latency + cancel ACK race).
            }
            EventKind::OrderCancelAck { order_id: _ } => {
                // Cancel path is implemented in later phases (latency + cancel ACK race).
            }
            EventKind::OrderReport(report) => {
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
                    self.push_event(ts_local, EventKind::Order(order));
                }
                Command::CancelOrder { order_id: _ } => {
                    // Not yet implemented.
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
            self.strategy.on_order_updates(&self.report_buffer, &mut ctx);
            self.report_buffer.clear();
        }

        self.handle_commands(ctx.into_commands(), ts_local);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::exchange_simulator::ExchangeSimulator;
    use crate::fixtures;
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
        };

        let exchange = ExchangeSimulator::new(ConservativeQueue);
        let strategy = RecordingStrategy::default();
        let mut eng = Engine::new(exchange, strategy, config);

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
        };
        let exchange = ExchangeSimulator::new(ConservativeQueue);
        let strategy = NoopStrategy::default();
        let mut eng = Engine::new(exchange, strategy, config);

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
        assert_eq!(eng.market_view().last_trade(fixtures::SYMBOL_ID_BTC_USDT), None);

        // Process delivery: MarketView updates exactly at ts_local.
        eng.step().expect("delivery tick");
        let last = eng
            .market_view()
            .last_trade(fixtures::SYMBOL_ID_BTC_USDT)
            .expect("last trade");
        assert_eq!(last.ts_exchange, 1_000);
        assert_eq!(last.ts_local, 2_000);
    }
}

