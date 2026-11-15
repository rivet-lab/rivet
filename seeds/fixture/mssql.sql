-- Parameterised fixture seed (SQL Server 2022+, GENERATE_SERIES): values are formulas over the row number, ids are explicit, no clock or random source.
SET NOCOUNT ON;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS page_views;
DROP TABLE IF EXISTS content_items;
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    email NVARCHAR(200) NOT NULL,
    age INT NULL,
    balance DECIMAL(12,2) NULL,
    is_active BIT NOT NULL,
    bio NVARCHAR(MAX) NULL,
    created_at DATETIME2(6) NOT NULL,
    updated_at DATETIME2(6) NOT NULL
);
GO
CREATE TABLE orders (
    id INT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    product NVARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    notes NVARCHAR(MAX) NULL,
    ordered_at DATETIME2(6) NOT NULL,
    updated_at DATETIME2(6) NOT NULL
);
GO
CREATE TABLE events (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    event_type NVARCHAR(50) NOT NULL,
    payload NVARCHAR(MAX) NULL,
    ip_address NVARCHAR(45) NULL,
    created_at DATETIME2(6) NOT NULL
);
GO
CREATE TABLE page_views (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    session_id NVARCHAR(36) NOT NULL,
    user_id INT NULL,
    url NVARCHAR(MAX) NOT NULL,
    referrer NVARCHAR(MAX) NULL,
    user_agent NVARCHAR(MAX) NULL,
    ip_address NVARCHAR(45) NULL,
    country_code CHAR(2) NULL,
    region NVARCHAR(100) NULL,
    city NVARCHAR(100) NULL,
    device_type NVARCHAR(20) NULL,
    browser NVARCHAR(50) NULL,
    os NVARCHAR(50) NULL,
    screen_width INT NULL,
    screen_height INT NULL,
    viewport_width INT NULL,
    viewport_height INT NULL,
    page_load_ms INT NULL,
    dom_ready_ms INT NULL,
    time_on_page_ms INT NULL,
    scroll_depth_pct SMALLINT NULL,
    click_count SMALLINT NULL,
    is_bounce BIT NOT NULL,
    utm_source NVARCHAR(100) NULL,
    utm_medium NVARCHAR(100) NULL,
    utm_campaign NVARCHAR(200) NULL,
    utm_term NVARCHAR(200) NULL,
    utm_content NVARCHAR(200) NULL,
    custom_props NVARCHAR(MAX) NULL,
    created_at DATETIME2(6) NOT NULL
);
GO
CREATE TABLE content_items (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    title NVARCHAR(MAX) NOT NULL,
    body NVARCHAR(MAX) NOT NULL,
    raw_html NVARCHAR(MAX) NOT NULL,
    metadata NVARCHAR(MAX) NULL,
    tags NVARCHAR(MAX) NULL,
    author_name NVARCHAR(100) NOT NULL,
    author_email NVARCHAR(200) NOT NULL,
    source_url NVARCHAR(MAX) NULL,
    category NVARCHAR(50) NULL,
    status NVARCHAR(20) NOT NULL,
    priority INT NOT NULL,
    view_count INT NOT NULL,
    comment_count INT NOT NULL,
    word_count INT NOT NULL,
    language CHAR(2) NOT NULL,
    published_at DATETIME2(6) NULL,
    updated_at DATETIME2(6) NOT NULL,
    created_at DATETIME2(6) NOT NULL,
    extra_data NVARCHAR(MAX) NULL
);
GO
GO
SET IDENTITY_INSERT users ON;
DECLARE @s BIGINT = 1, @e BIGINT;
WHILE @s <= $(users)
BEGIN
    SET @e = CASE WHEN @s + 199999 < $(users) THEN @s + 199999 ELSE $(users) END;
    INSERT INTO users (id, name, email, age, balance, is_active, bio, created_at, updated_at)
    SELECT value,
        CONCAT(N'User ', value),
        CONCAT('user', value, '@example.com'),
        18 + (value % 48),
        ROUND((value % 200000) + 0.99, 2),
        CAST(CASE WHEN value % 10 <> 0 THEN 1 ELSE 0 END AS BIT),
        CASE WHEN value % 3 <> 0 THEN CONCAT(N'seed bio ', value) ELSE NULL END,
        DATEADD(HOUR, value % 730, CONVERT(DATETIME2(6), '2023-01-01')),
        DATEADD(HOUR, value % 910, CONVERT(DATETIME2(6), '2023-01-01'))
    FROM GENERATE_SERIES(@s, @e);
    SET @s = @e + 1;
END;
SET IDENTITY_INSERT users OFF;
GO
SET IDENTITY_INSERT orders ON;
DECLARE @s BIGINT = 1, @e BIGINT;
WHILE @s <= ($(users) * $(orders_per_user))
BEGIN
    SET @e = CASE WHEN @s + 199999 < ($(users) * $(orders_per_user)) THEN @s + 199999 ELSE ($(users) * $(orders_per_user)) END;
    INSERT INTO orders (id, user_id, product, quantity, price, status, notes, ordered_at, updated_at)
    SELECT value,
        CAST(1 + (value - 1) / $(orders_per_user) AS INT),
        CASE value % 10
            WHEN 0 THEN N'MacBook Pro 16"' WHEN 1 THEN N'Dell XPS 15'
            WHEN 2 THEN N'ThinkPad X1 Carbon' WHEN 3 THEN N'Surface Laptop'
            WHEN 4 THEN N'Ergonomic Chair' WHEN 5 THEN N'Standing Desk'
            WHEN 6 THEN N'Monitor Arm' WHEN 7 THEN N'USB-C Hub'
            WHEN 8 THEN N'Mechanical Keyboard' ELSE N'Magic Mouse' END,
        1 + (value % 10),
        ROUND(5 + (value % 4995) + 0.0, 2),
        CASE value % 4 WHEN 0 THEN N'pending' WHEN 1 THEN N'shipped'
                       WHEN 2 THEN N'delivered' ELSE N'cancelled' END,
        CASE WHEN value % 3 = 0 THEN CONCAT(N'note ', value) ELSE NULL END,
        DATEADD(MINUTE, value % 730, CONVERT(DATETIME2(6), '2023-01-01')),
        DATEADD(MINUTE, value % 760, CONVERT(DATETIME2(6), '2023-01-01'))
    FROM GENERATE_SERIES(@s, @e);
    SET @s = @e + 1;
END;
SET IDENTITY_INSERT orders OFF;
GO
SET IDENTITY_INSERT events ON;
DECLARE @s BIGINT = 1, @e BIGINT;
WHILE @s <= ($(users) * $(events_per_user))
BEGIN
    SET @e = CASE WHEN @s + 199999 < ($(users) * $(events_per_user)) THEN @s + 199999 ELSE ($(users) * $(events_per_user)) END;
    INSERT INTO events (id, user_id, event_type, payload, ip_address, created_at)
    SELECT value,
        CAST(1 + (value - 1) / $(events_per_user) AS INT),
        CASE value % 10
            WHEN 0 THEN N'login' WHEN 1 THEN N'logout' WHEN 2 THEN N'page_view'
            WHEN 3 THEN N'purchase' WHEN 4 THEN N'signup' WHEN 5 THEN N'settings_change'
            WHEN 6 THEN N'password_reset' WHEN 7 THEN N'search'
            WHEN 8 THEN N'export' ELSE N'api_call' END,
        CONCAT(N'{"seed":true,"i":', value, N'}'),
        CONCAT('10.', value % 255, '.', (value * 7) % 255, '.1'),
        DATEADD(MINUTE, value % 730, CONVERT(DATETIME2(6), '2023-01-01'))
    FROM GENERATE_SERIES(@s, @e);
    SET @s = @e + 1;
END;
SET IDENTITY_INSERT events OFF;
GO
SET IDENTITY_INSERT page_views ON;
DECLARE @s BIGINT = 1, @e BIGINT;
WHILE @s <= $(page_views)
BEGIN
    SET @e = CASE WHEN @s + 199999 < $(page_views) THEN @s + 199999 ELSE $(page_views) END;
    INSERT INTO page_views (id, session_id, user_id, url, referrer, user_agent, ip_address,
        country_code, region, city, device_type, browser, os,
        screen_width, screen_height, viewport_width, viewport_height,
        page_load_ms, dom_ready_ms, time_on_page_ms, scroll_depth_pct, click_count,
        is_bounce, utm_source, utm_medium, utm_campaign, utm_term, utm_content,
        custom_props, created_at)
    SELECT value,
        LOWER(RIGHT(CONCAT(REPLICATE('0', 32), CONVERT(VARCHAR(16), CONVERT(VARBINARY(8), value), 2)), 32)),
        CASE WHEN value % 3 = 0 THEN NULL ELSE CAST(1 + (value % $(users)) AS INT) END,
        CONCAT('/page/', value % 26),
        CASE WHEN value % 4 = 0 THEN 'https://google.com' ELSE NULL END,
        N'Mozilla/5.0 seed',
        CONCAT('192.168.', value % 255, '.', (value * 3) % 254 + 1),
        CASE value % 5 WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE'
                       WHEN 3 THEN 'FR' ELSE 'CA' END,
        CONCAT(N'Region ', value % 10),
        CONCAT(N'City ', value % 20),
        CASE value % 3 WHEN 0 THEN N'desktop' WHEN 1 THEN N'mobile' ELSE N'tablet' END,
        CASE value % 3 WHEN 0 THEN N'chrome' WHEN 1 THEN N'firefox' ELSE N'safari' END,
        CASE value % 3 WHEN 0 THEN N'macOS' WHEN 1 THEN N'Windows' ELSE N'Linux' END,
        1280 + (value % 2560), 720 + (value % 1440),
        800 + (value % 1200), 600 + (value % 900),
        100 + (value % 5000), 50 + (value % 2500), 500 + (value % 60000),
        CAST(value % 101 AS SMALLINT), CAST(value % 51 AS SMALLINT),
        CAST(CASE WHEN value % 3 = 0 THEN 1 ELSE 0 END AS BIT),
        CASE WHEN value % 4 = 0 THEN N'google' ELSE NULL END,
        CASE WHEN value % 4 = 0 THEN N'cpc' ELSE NULL END,
        CASE WHEN value % 8 = 0 THEN N'spring_sale' ELSE NULL END,
        NULL, NULL,
        CASE WHEN value % 5 = 0 THEN N'{"seed":true}' ELSE NULL END,
        DATEADD(MINUTE, value % 730, CONVERT(DATETIME2(6), '2023-01-01'))
    FROM GENERATE_SERIES(@s, @e);
    SET @s = @e + 1;
END;
SET IDENTITY_INSERT page_views OFF;
GO
SET IDENTITY_INSERT content_items ON;
DECLARE @s BIGINT = 1, @e BIGINT;
WHILE @s <= $(content_items)
BEGIN
    SET @e = CASE WHEN @s + 199999 < $(content_items) THEN @s + 199999 ELSE $(content_items) END;
    INSERT INTO content_items (id, title, body, raw_html, metadata, tags, author_name, author_email,
        source_url, category, status, priority, view_count, comment_count,
        word_count, language, published_at, updated_at, created_at, extra_data)
    SELECT value,
        CONCAT(N'Seed title ', value),
        REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)), 200),
        CONCAT(N'<p>', REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)), 200), N'</p>'),
        CONCAT(N'{"seed":true,"i":', value, N'}'),
        N'rust,postgres,data',
        CONCAT(N'Author ', value % 1000),
        CONCAT('author', value % 1000, '@example.com'),
        CONCAT('https://blog.example.com/posts/', value),
        CASE value % 3 WHEN 0 THEN N'engineering' WHEN 1 THEN N'product' ELSE N'tutorial' END,
        CASE value % 3 WHEN 0 THEN N'draft' WHEN 1 THEN N'review' ELSE N'published' END,
        value % 5, value % 100000, value % 500, 200, 'en',
        CASE WHEN value % 3 <> 0 THEN DATEADD(DAY, value % 365, CONVERT(DATETIME2(6), '2024-01-01')) ELSE NULL END,
        DATEADD(DAY, value % 400, CONVERT(DATETIME2(6), '2024-01-01')),
        DATEADD(DAY, value % 730, CONVERT(DATETIME2(6), '2023-01-01')),
        N'{"revisions":1}'
    FROM GENERATE_SERIES(@s, @e);
    SET @s = @e + 1;
END;
SET IDENTITY_INSERT content_items OFF;
GO
