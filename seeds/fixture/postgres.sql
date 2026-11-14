-- Parameterised fixture seed: values are formulas over the row number, ids are explicit, no clock or random source.
\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT,
    balance NUMERIC(12,2),
    is_active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    notes TEXT,
    ordered_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id),
    event_type VARCHAR(50) NOT NULL,
    payload JSONB,
    ip_address VARCHAR(45),
    created_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id);
CREATE TABLE IF NOT EXISTS page_views (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(36) NOT NULL,
    user_id INT,
    url TEXT NOT NULL,
    referrer TEXT,
    user_agent TEXT,
    ip_address VARCHAR(45),
    country_code CHAR(2),
    region VARCHAR(100),
    city VARCHAR(100),
    device_type VARCHAR(20),
    browser VARCHAR(50),
    os VARCHAR(50),
    screen_width INT,
    screen_height INT,
    viewport_width INT,
    viewport_height INT,
    page_load_ms INT,
    dom_ready_ms INT,
    time_on_page_ms INT,
    scroll_depth_pct SMALLINT,
    click_count SMALLINT,
    is_bounce BOOLEAN NOT NULL DEFAULT false,
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100),
    utm_campaign VARCHAR(200),
    utm_term VARCHAR(200),
    utm_content VARCHAR(200),
    custom_props JSONB,
    created_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS content_items (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    raw_html TEXT NOT NULL,
    metadata JSONB,
    tags TEXT,
    author_name VARCHAR(100) NOT NULL,
    author_email VARCHAR(200) NOT NULL,
    source_url TEXT,
    category VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    priority INT NOT NULL DEFAULT 0,
    view_count INT NOT NULL DEFAULT 0,
    comment_count INT NOT NULL DEFAULT 0,
    word_count INT NOT NULL DEFAULT 0,
    language CHAR(2) NOT NULL DEFAULT 'en',
    published_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    extra_data JSONB
);
CREATE TABLE IF NOT EXISTS orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_coalesce (
    id BIGSERIAL PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    updated_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_coalesce_updated_at ON orders_coalesce(updated_at);
CREATE INDEX IF NOT EXISTS idx_orders_coalesce_created_at ON orders_coalesce(created_at);

TRUNCATE orders_coalesce, orders_sparse, content_items, page_views, events, orders, users RESTART IDENTITY CASCADE;

INSERT INTO users (id, name, email, age, balance, is_active, bio, created_at, updated_at)
SELECT
    i,
    'User ' || i,
    'user' || i || '@example.com',
    18 + (i % 48),
    round(((i % 200000) + 0.99)::numeric, 2),
    (i % 10 <> 0),
    CASE WHEN i % 3 <> 0 THEN 'seed bio ' || i ELSE NULL END,
    timestamp '2023-01-01' + ((i % 730) * interval '1 hour'),
    timestamp '2023-01-01' + ((i % 910) * interval '1 hour')
FROM generate_series(1::bigint, :users) AS i;

INSERT INTO orders (id, user_id, product, quantity, price, status, notes, ordered_at, updated_at)
SELECT
    i,
    1 + ((i - 1) / :orders_per_user),
    (ARRAY['MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair',
           'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'])[(i % 10) + 1],
    1 + (i % 10),
    round((5 + (i % 4995))::numeric, 2),
    (ARRAY['pending','shipped','delivered','cancelled'])[(i % 4) + 1],
    CASE WHEN i % 3 = 0 THEN 'note ' || i ELSE NULL END,
    timestamp '2023-01-01' + ((i % 730) * interval '1 minute'),
    timestamp '2023-01-01' + ((i % 760) * interval '1 minute')
FROM generate_series(1::bigint, :users * :orders_per_user) AS i;

INSERT INTO events (id, user_id, event_type, payload, ip_address, created_at)
SELECT
    i,
    1 + ((i - 1) / :events_per_user),
    (ARRAY['login','logout','page_view','purchase','signup','settings_change',
           'password_reset','search','export','api_call'])[(i % 10) + 1],
    jsonb_build_object('seed', true, 'i', i),
    '10.' || (i % 255)::text || '.' || ((i * 7) % 255)::text || '.1',
    timestamp '2023-01-01' + ((i % 730) * interval '1 minute')
FROM generate_series(1::bigint, :users * :events_per_user) AS i;

INSERT INTO page_views (
    id, session_id, user_id, url, referrer, user_agent, ip_address,
    country_code, region, city, device_type, browser, os,
    screen_width, screen_height, viewport_width, viewport_height,
    page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
    is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
    custom_props, created_at
)
SELECT
    i,
    lpad(to_hex(i), 32, '0'),
    CASE WHEN i % 3 = 0 THEN NULL ELSE 1 + (i % :users) END,
    '/page/' || (i % 26),
    CASE WHEN i % 4 = 0 THEN 'https://google.com' ELSE NULL END,
    'Mozilla/5.0 seed',
    '192.168.' || (i % 255)::text || '.' || ((i * 3) % 254 + 1)::text,
    (ARRAY['US','GB','DE','FR','CA'])[(i % 5) + 1],
    'Region ' || (i % 10),
    'City ' || (i % 20),
    (ARRAY['desktop','mobile','tablet'])[(i % 3) + 1],
    (ARRAY['chrome','firefox','safari'])[(i % 3) + 1],
    (ARRAY['macOS','Windows','Linux'])[(i % 3) + 1],
    1280 + (i % 2560),
    720 + (i % 1440),
    800 + (i % 1200),
    600 + (i % 900),
    100 + (i % 5000),
    50 + (i % 2500),
    500 + (i % 60000),
    (i % 101)::smallint,
    (i % 51)::smallint,
    (i % 3 = 0),
    CASE WHEN i % 4 = 0 THEN 'google' ELSE NULL END,
    CASE WHEN i % 4 = 0 THEN 'cpc' ELSE NULL END,
    CASE WHEN i % 8 = 0 THEN 'spring_sale' ELSE NULL END,
    NULL,
    NULL,
    CASE WHEN i % 5 = 0 THEN jsonb_build_object('seed', true) ELSE NULL END,
    timestamp '2023-01-01' + ((i % 730) * interval '1 minute')
FROM generate_series(1::bigint, :page_views) AS i;

INSERT INTO content_items (
    id, title, body, raw_html, metadata, tags, author_name, author_email,
    source_url, category, status, priority, view_count, comment_count,
    word_count, language, published_at, updated_at, created_at, extra_data
)
SELECT
    i,
    'Seed title ' || i,
    repeat('lorem ipsum ', 200),
    '<p>' || repeat('lorem ipsum ', 200) || '</p>',
    jsonb_build_object('seed', true, 'i', i),
    'rust,postgres,data',
    'Author ' || (i % 1000),
    'author' || (i % 1000) || '@example.com',
    'https://blog.example.com/posts/' || i,
    (ARRAY['engineering','product','tutorial'])[(i % 3) + 1],
    (ARRAY['draft','review','published'])[(i % 3) + 1],
    (i % 5),
    (i % 100000),
    (i % 500),
    200,
    'en',
    CASE WHEN i % 3 <> 0 THEN timestamp '2024-01-01' + ((i % 365) * interval '1 day') ELSE NULL END,
    timestamp '2024-01-01' + ((i % 400) * interval '1 day'),
    timestamp '2023-01-01' + ((i % 730) * interval '1 day'),
    jsonb_build_object('revisions', 1)
FROM generate_series(1::bigint, :content_items) AS i;

INSERT INTO orders_sparse (id, payload)
SELECT 1 + (i - 1) * :sparse_gap, 's' || (i - 1)
FROM generate_series(1::bigint, :sparse_rows) AS i;

INSERT INTO orders_coalesce (id, product, quantity, price, updated_at, created_at)
SELECT
    i,
    (ARRAY['MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair',
           'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'])[(i % 10) + 1],
    1 + (i % 10),
    round((5 + ((i * 7919) % 499500) / 100.0)::numeric, 2),
    CASE WHEN i % 20 < 7 THEN NULL
         ELSE timestamp '2025-01-01' + (((i * 613) % 525600) * interval '1 minute') END,
    timestamp '2024-01-01' + (((i * 977) % 525600) * interval '1 minute')
FROM generate_series(1::bigint, :coalesce_rows) AS i;

SELECT setval(pg_get_serial_sequence('users', 'id'), greatest(:users, 1), :users > 0);
SELECT setval(pg_get_serial_sequence('orders', 'id'), greatest(:users * :orders_per_user, 1), :users * :orders_per_user > 0);
SELECT setval(pg_get_serial_sequence('events', 'id'), greatest(:users * :events_per_user, 1), :users * :events_per_user > 0);
SELECT setval(pg_get_serial_sequence('page_views', 'id'), greatest(:page_views, 1), :page_views > 0);
SELECT setval(pg_get_serial_sequence('content_items', 'id'), greatest(:content_items, 1), :content_items > 0);
SELECT setval(pg_get_serial_sequence('orders_coalesce', 'id'), greatest(:coalesce_rows, 1), :coalesce_rows > 0);
