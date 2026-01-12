### crypto-rs-backtester

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/takurot/crypto-rs-backtester/blob/main/example/colab_backtester_demo.ipynb)

Rust × Python(Polars) で動く、研究者フレンドリーなティックレベル高精度バックテスター（WIP）。

- 目的: Python の機敏さと Rust の決定論的・高性能シミュレーションを融合し、性能ボトルネック／先読み偏り／再現性欠如を解消。
- 対象: 主に暗号資産スポット/先物（CEX）。複数取引所・レイテンシ・キュー位置などマイクロストラクチャを検証可能。

---

### 特長（現状の実装）

- ハイブリッド構成: Rust コア（イベント駆動、固定小数点 i64）＋ Python 戦略インターフェイス。
- 時間軸の分離: `ts_exchange`(真実) / `ts_local`(戦略が観測) / `ts_sim`(全イベントの順序付け)。
- 先読み防止: 戦略は feed 遅延後の `MarketView` のみ参照。注文到達・ACK も `ts_sim` で厳密順序化。
- 実行モード: Tick モード（`on_tick`）と Batch モード（`on_ticks` / `on_order_updates`）。
- データ取り込み: Polars LazyFrame 経由（必要最低限のコピー）、または Arrow C Stream のゼロコピー（`run_arrow`）。
- 決定論: RNG のシード固定、安定タイブレーク（同時刻イベント）、辞書順でのシンボル ID 割当。

詳細は `docs/SPEC.md` を参照してください。

---

### リポジトリ構成

- `backtester-core/`: Rust シミュレーションコア（`src/*.rs`, `tests/`, `benches/`）
- `backtester-py/`: PyO3 ラッパー（Python からコアを呼び出し）
- `python/`: Python パッケージ `rust_backtester/` とテスト `python/tests/`
- `docs/`: 仕様・計画（`SPEC.md`, `PLAN.md` ほか）
- ルート `Cargo.toml`: Rust ワークスペース, `pyproject.toml`: maturin ビルド設定

---

### インストールとビルド（開発）

前提: Python 3.9+ / Rust ツールチェイン / maturin

```bash
# 仮想環境
python -m venv .venv && source .venv/bin/activate

# 開発インストール（Rust 拡張をビルド）
pip install -e .[dev]

# 代替: 直接ビルド
maturin develop
```

---

### クイックスタート（Python）

必須列: `ts_exchange:Int64`, `price:Int64`, `qty:Int64`, `side:Int8`
推奨列: `seq:Int64`（同時刻の安定順序）, `ts_local:Int64`（無い場合は `ts_exchange + feed_latency_ns` を適用）
エイリアス: `ts_event` ≒ `ts_exchange`, `size` ≒ `qty`

```python
import polars as pl
from rust_backtester import Backtester

# 小さな決定論データ（1e-8 固定小数点: 100.0 => 100_00000000）
lf = pl.DataFrame({
    "ts_exchange": [1_000, 2_000, 3_000, 4_000],
    "price": [100_00000000, 101_00000000, 99_00000000, 100_00000000],
    "qty":   [  1_00000000,   1_00000000,  1_00000000,   1_00000000],
    "side":  [            1,           -1,           1,           -1],
    "seq": list(range(4)),
}).lazy()

class MyStrategy:
    def on_tick(self, tick: dict, ctx):
        # 例: 受け取ったティックを使って受動注文を送る
        ctx.submit_order(
            symbol_id=int(tick["symbol_id"]),
            side=1,  # 1=Buy, -1=Sell
            price=int(tick["price"]),
            qty=1_00000000,
        )

bt = Backtester(
    data={"binance:BTC/USDT": lf},
    seed=42,
    python_mode="tick",    # または "batch"
    batch_ms=100,
    feed_latency_ns=1_000,  # ts_local = ts_exchange + 1_000 (ns) を適用
)

result = bt.run(MyStrategy())
print(result.stats())   # dict
print(result.trades())  # list[dict]
```

バッチモード（高スループット）

```python
class MyBatch:
    def on_ticks(self, ticks: list[dict], ctx):
        for t in ticks:
            ctx.submit_order(symbol_id=t["symbol_id"], side=1, price=t["price"], qty=1_00000000)

bt = Backtester(data={"binance:BTC/USDT": lf}, seed=42, python_mode="batch", batch_ms=50)
res = bt.run(MyBatch())
```

Arrow ゼロコピー経路（大規模データ向け）

```python
# __arrow_c_stream__ を実装する PyArrow RecordBatchReader を渡す
res = bt.run_arrow(stream=rb_reader, strategy=MyBatch())
```

---

### ビルド・テスト・ベンチマーク

- Python テスト: `pytest -q`
  - ベンチのみ: `pytest -m bench -q`
- Rust ビルド: `cargo build -p backtester-core`
- Rust テスト: `cargo test -p backtester-core`
- Rust ベンチ: `cargo bench -p backtester-core`

補足: `python/tests/conftest.py` が拡張未インストール時に `maturin develop` を自動実行します。

---

### サンプル（example/）

- `example/colab_backtester_demo.ipynb`
  - 最小の E2E デモノートブック。上部の Colab バッジからブラウザで即実行できます。
  - ローカルで開く場合は Jupyter 環境で本レポのルートから起動し、`example/` 配下のノートブックを開いてください。
- `example/crypto_researcher_adoption_guide.md`
  - 研究者向けの導入手順・データスキーマ・戦略実装モード（tick/batch）・パフォーマンス調整をまとめた実践ガイドです。

注: 大規模データは同梱していません。最初は Python テストが生成する最小データ（`python/tests/` のヘルパ）や Colab デモで動作確認するのがおすすめです。

---

### コーディングスタイルとコア原則

- Rust: edition 2024, `cargo fmt` / `cargo clippy`。命名は関数/モジュール `snake_case`, 型 `CamelCase`。
- Python: PEP8, 4 スペースインデント。新規/変更コードは型ヒント必須。
- 決定論最優先: シード固定 RNG、安定順序。金額ロジックに `f64` を使わない（I/O 境界のみ許可）。

---

### 貢献とコミット規約

- まず `docs/SPEC.md` と `docs/PLAN.md` を参照。
- Conventional Commits: 例 `feat(core): add queue model`, `chore(fmt): rustfmt`
- ブランチ: `feature/...`, `fix/...`, `chore/...`
- PR には What/Why・関連 Issue・テスト手順（実行コマンドと結果）・パフォーマンス影響を含める。API/設計変更時は `docs/` を更新。

---

### 現状と今後

本プロジェクトはアクティブ開発中（WIP）です。API/内部構造は変更される可能性があります。

- 技術仕様: `docs/SPEC.md`
- 実装計画/テスト/ベンチ: `docs/PLAN.md`
- 研究者向け導入ガイド: `example/crypto_researcher_adoption_guide.md`
- Colab デモ: `example/colab_backtester_demo.ipynb`
