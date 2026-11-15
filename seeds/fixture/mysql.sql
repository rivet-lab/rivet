-- Parameterised fixture seed: values are formulas over the row number, ids are explicit, no clock or random source.
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;

CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT,
    balance DECIMAL(12,2),
    is_active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    notes TEXT,
    ordered_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    INDEX idx_orders_updated_at (updated_at),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSON,
    ip_address VARCHAR(45),
    created_at DATETIME NOT NULL,
    INDEX idx_events_created_at (created_at),
    INDEX idx_events_user_id (user_id),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE IF NOT EXISTS page_views (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
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
    custom_props JSON,
    created_at DATETIME NOT NULL
);
CREATE TABLE IF NOT EXISTS content_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    title TEXT NOT NULL,
    body LONGTEXT NOT NULL,
    raw_html LONGTEXT NOT NULL,
    metadata JSON,
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
    published_at DATETIME,
    updated_at DATETIME NOT NULL,
    created_at DATETIME NOT NULL,
    extra_data JSON
);
CREATE TABLE IF NOT EXISTS orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders_coalesce (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    updated_at DATETIME NULL,
    created_at DATETIME NOT NULL,
    INDEX idx_orders_coalesce_updated_at (updated_at),
    INDEX idx_orders_coalesce_created_at (created_at)
);

TRUNCATE TABLE orders_coalesce;
TRUNCATE TABLE orders_sparse;
TRUNCATE TABLE content_items;
TRUNCATE TABLE page_views;
TRUNCATE TABLE events;
TRUNCATE TABLE orders;
TRUNCATE TABLE users;

DROP TEMPORARY TABLE IF EXISTS rivet_seed_n;
CREATE TEMPORARY TABLE rivet_seed_n (n INT NOT NULL PRIMARY KEY);
INSERT INTO rivet_seed_n
SELECT t0.d + 10 * t1.d + 100 * t2.d + 1000 * t3.d + 10000 * t4.d
FROM (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t0,
     (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
     (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
     (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
     (SELECT 0 AS d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t4;

DROP PROCEDURE IF EXISTS rivet_seed_fixture;
DELIMITER //
CREATE PROCEDURE rivet_seed_fixture()
BEGIN
  DECLARE v_lo BIGINT;
  DECLARE v_hi BIGINT;
  SET v_lo = 1;
  WHILE v_lo <= @users DO
    SET v_hi = LEAST(v_lo + 99999, @users);
    INSERT INTO users (id, name, email, age, balance, is_active, bio, created_at, updated_at)
      SELECT i, CONCAT('User ', i), CONCAT('user', i, '@example.com'), 18 + (i MOD 48),
             ROUND((i MOD 200000) + 0.99, 2), (i MOD 10 <> 0),
             IF(i MOD 3 <> 0, CONCAT('seed bio ', i), NULL),
             '2023-01-01 00:00:00' + INTERVAL (i MOD 730) HOUR,
             '2023-01-01 00:00:00' + INTERVAL (i MOD 910) HOUR
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
  SET v_lo = 1;
  WHILE v_lo <= (@users * @orders_per_user) DO
    SET v_hi = LEAST(v_lo + 99999, (@users * @orders_per_user));
    INSERT INTO orders (id, user_id, product, quantity, price, status, notes, ordered_at, updated_at)
      SELECT i, 1 + ((i - 1) DIV @orders_per_user),
             ELT((i MOD 10) + 1, 'MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair',
                 'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'),
             1 + (i MOD 10), ROUND(5 + (i MOD 4995), 2),
             ELT((i MOD 4) + 1, 'pending','shipped','delivered','cancelled'),
             IF(i MOD 3 = 0, CONCAT('note ', i), NULL),
             '2023-01-01 00:00:00' + INTERVAL (i MOD 730) MINUTE,
             '2023-01-01 00:00:00' + INTERVAL (i MOD 760) MINUTE
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
  SET v_lo = 1;
  WHILE v_lo <= (@users * @events_per_user) DO
    SET v_hi = LEAST(v_lo + 99999, (@users * @events_per_user));
    INSERT INTO events (id, user_id, event_type, payload, ip_address, created_at)
      SELECT i, 1 + ((i - 1) DIV @events_per_user),
             ELT((i MOD 10) + 1, 'login','logout','page_view','purchase','signup',
                 'settings_change','password_reset','search','export','api_call'),
             JSON_OBJECT('seed', TRUE, 'i', i),
             CONCAT('10.', i MOD 255, '.', (i * 7) MOD 255, '.1'),
             '2023-01-01 00:00:00' + INTERVAL (i MOD 730) MINUTE
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
  SET v_lo = 1;
  WHILE v_lo <= @page_views DO
    SET v_hi = LEAST(v_lo + 99999, @page_views);
    INSERT INTO page_views (
        id, session_id, user_id, url, referrer, user_agent, ip_address,
        country_code, region, city, device_type, browser, os,
        screen_width, screen_height, viewport_width, viewport_height,
        page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
        is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
        custom_props, created_at)
      SELECT i, LPAD(HEX(i), 32, '0'),
             IF(i MOD 3 = 0, NULL, 1 + (i MOD @users)),
             CONCAT('/page/', i MOD 26),
             IF(i MOD 4 = 0, 'https://google.com', NULL),
             'Mozilla/5.0 seed',
             CONCAT('192.168.', i MOD 255, '.', (i * 3) MOD 254 + 1),
             ELT((i MOD 5) + 1, 'US','GB','DE','FR','CA'),
             CONCAT('Region ', i MOD 10), CONCAT('City ', i MOD 20),
             ELT((i MOD 3) + 1, 'desktop','mobile','tablet'),
             ELT((i MOD 3) + 1, 'chrome','firefox','safari'),
             ELT((i MOD 3) + 1, 'macOS','Windows','Linux'),
             1280 + (i MOD 2560), 720 + (i MOD 1440), 800 + (i MOD 1200), 600 + (i MOD 900),
             100 + (i MOD 5000), 50 + (i MOD 2500), 500 + (i MOD 60000),
             i MOD 101, i MOD 51, (i MOD 3 = 0),
             IF(i MOD 4 = 0, 'google', NULL), IF(i MOD 4 = 0, 'cpc', NULL),
             IF(i MOD 8 = 0, 'spring_sale', NULL), NULL, NULL,
             IF(i MOD 5 = 0, JSON_OBJECT('seed', TRUE), NULL),
             '2023-01-01 00:00:00' + INTERVAL (i MOD 730) MINUTE
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
  SET v_lo = 1;
  WHILE v_lo <= @content_items DO
    SET v_hi = LEAST(v_lo + 99999, @content_items);
    INSERT INTO content_items (
        id, title, body, raw_html, metadata, tags, author_name, author_email,
        source_url, category, status, priority, view_count, comment_count,
        word_count, language, published_at, updated_at, created_at, extra_data)
      SELECT i, CONCAT('Seed title ', i), REPEAT('lorem ipsum ', 200),
             CONCAT('<p>', REPEAT('lorem ipsum ', 200), '</p>'),
             JSON_OBJECT('seed', TRUE, 'i', i), 'rust,postgres,data',
             CONCAT('Author ', i MOD 1000), CONCAT('author', i MOD 1000, '@example.com'),
             CONCAT('https://blog.example.com/posts/', i),
             ELT((i MOD 3) + 1, 'engineering','product','tutorial'),
             ELT((i MOD 3) + 1, 'draft','review','published'),
             i MOD 5, i MOD 100000, i MOD 500, 200, 'en',
             IF(i MOD 3 <> 0, '2024-01-01 00:00:00' + INTERVAL (i MOD 365) DAY, NULL),
             '2024-01-01 00:00:00' + INTERVAL (i MOD 400) DAY,
             '2023-01-01 00:00:00' + INTERVAL (i MOD 730) DAY,
             JSON_OBJECT('revisions', 1)
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
  SET v_lo = 1;
  WHILE v_lo <= @sparse_rows DO
    SET v_hi = LEAST(v_lo + 99999, @sparse_rows);
    INSERT INTO orders_sparse (id, payload)
      SELECT 1 + (i - 1) * @sparse_gap, CONCAT('s', i - 1)
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
  SET v_lo = 1;
  WHILE v_lo <= @coalesce_rows DO
    SET v_hi = LEAST(v_lo + 99999, @coalesce_rows);
    INSERT INTO orders_coalesce (id, product, quantity, price, updated_at, created_at)
      SELECT i,
             ELT((i MOD 10) + 1, 'MacBook Pro 16"','Dell XPS 15','ThinkPad X1 Carbon','Surface Laptop','Ergonomic Chair',
                 'Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'),
             1 + (i MOD 10), ROUND(5 + ((i * 7919) MOD 499500) / 100.0, 2),
             IF(i MOD 20 < 7, NULL, '2025-01-01 00:00:00' + INTERVAL ((i * 613) MOD 525600) MINUTE),
             '2024-01-01 00:00:00' + INTERVAL ((i * 977) MOD 525600) MINUTE
      FROM (SELECT v_lo + n AS i FROM rivet_seed_n WHERE v_lo + n <= v_hi) q;
    SET v_lo = v_hi + 1;
  END WHILE;
END //
DELIMITER ;
CALL rivet_seed_fixture();
DROP PROCEDURE rivet_seed_fixture;
DROP TEMPORARY TABLE rivet_seed_n;
SET UNIQUE_CHECKS = 1;
SET FOREIGN_KEY_CHECKS = 1;
