CREATE TABLE IF NOT EXISTS stock_prices (
    symbol TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    source TEXT DEFAULT 'alpha_vantage',
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, ts)
);