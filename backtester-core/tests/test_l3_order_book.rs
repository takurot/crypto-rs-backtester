use backtester_core::orderbook_l3::{L3_ADD, L3_DELETE, OrderBookL3};
use backtester_core::queue_model::{ConservativeQueue, L3ExactQueue, QueueModel};
use backtester_core::{L2Update, L3Update, Order, OrderBookL2, OrderType, Side, Tick};

const SYMBOL_ID: u32 = 1;

fn l3_update(order_id: u64, price: i64, qty: i64, side: Side, action: u8) -> L3Update {
    L3Update {
        ts_exchange: 0,
        symbol_id: SYMBOL_ID,
        order_id,
        price,
        qty,
        side,
        action,
    }
}

#[test]
fn l3_book_tracks_fifo_queue_and_exact_qty_ahead() {
    let mut book = OrderBookL3::new();
    book.apply_l3(&l3_update(10, 100, 5, Side::Buy, L3_ADD))
        .expect("add 10");
    book.apply_l3(&l3_update(20, 100, 7, Side::Buy, L3_ADD))
        .expect("add 20");
    book.apply_l3(&l3_update(30, 100, 3, Side::Buy, L3_ADD))
        .expect("add 30");

    assert_eq!(book.level_qty(Side::Buy, 100), 15);
    assert_eq!(book.qty_ahead(Side::Buy, 100, 10), 0);
    assert_eq!(book.qty_ahead(Side::Buy, 100, 20), 5);
    assert_eq!(book.qty_ahead(Side::Buy, 100, 30), 12);

    book.apply_l3(&l3_update(10, 100, 0, Side::Buy, L3_DELETE))
        .expect("delete 10");

    assert_eq!(book.level_qty(Side::Buy, 100), 10);
    assert_eq!(book.qty_ahead(Side::Buy, 100, 30), 7);
    assert_eq!(book.qty_ahead(Side::Buy, 100, 999), 10);
}

#[test]
fn l3_exact_queue_matches_manual_calculation_and_beats_conservative() {
    let mut l3_book = OrderBookL3::new();
    l3_book
        .apply_l3(&l3_update(20, 100, 5, Side::Buy, L3_ADD))
        .expect("add visible order ahead");

    let mut l2_book = OrderBookL2::new();
    l2_book.apply_l2(&L2Update {
        ts_exchange: 0,
        seq: 0,
        symbol_id: SYMBOL_ID,
        price: 100,
        qty: 10,
        side: Side::Buy,
    });

    let order = Order {
        order_id: 99,
        ts_submit: 1_000,
        seq: 0,
        symbol_id: SYMBOL_ID,
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: 100,
        qty: 3,
    };
    let trade = Tick {
        ts_exchange: 2_000,
        ts_local: 2_000,
        seq: 0,
        symbol_id: SYMBOL_ID,
        side: Side::Sell,
        price: 100,
        qty: 6,
        flags: 0x01,
    };

    let mut l3_model = L3ExactQueue;
    let mut l3_state = l3_model.register_order(&order, &l3_book);
    assert_eq!(l3_state.qty_ahead, 5);

    let mut conservative = ConservativeQueue;
    let mut conservative_state = conservative.register_order(&order, &l2_book);
    assert_eq!(conservative_state.qty_ahead, 10);

    let l3_fill = l3_model.check_fill(&order, 3, &trade, &mut l3_state);
    let conservative_fill = conservative.check_fill(&order, 3, &trade, &mut conservative_state);

    assert_eq!(l3_fill, 1);
    assert_eq!(conservative_fill, 0);
    assert!(l3_fill > conservative_fill);
}
