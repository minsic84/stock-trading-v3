-- ===================================================================
-- MySQL 환경 설정 스크립트
-- 파일 경로: scripts/setup_mysql.sql
-- ===================================================================

-- 1. 데이터베이스 생성
DROP DATABASE IF EXISTS stock_trading_db;
CREATE DATABASE stock_trading_db
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE stock_trading_db;

-- 2. 사용자 생성 및 권한 부여
DROP USER IF EXISTS 'stock_user'@'localhost';
CREATE USER 'stock_user'@'localhost' IDENTIFIED BY 'StockPass2025!';
GRANT ALL PRIVILEGES ON stock_trading_db.* TO 'stock_user'@'localhost';
FLUSH PRIVILEGES;

-- 3. 주식 기본정보 테이블 (stocks)
CREATE TABLE stocks (
    code VARCHAR(10) PRIMARY KEY COMMENT '종목코드',
    name VARCHAR(100) NOT NULL COMMENT '종목명',
    market VARCHAR(10) COMMENT '시장구분(KOSPI/KOSDAQ)',

    -- OPT10001 주식기본정보 데이터
    current_price INT COMMENT '현재가',
    prev_day_diff INT COMMENT '전일대비',
    change_rate INT COMMENT '등락률(소수점2자리*100)',
    volume BIGINT COMMENT '거래량',
    open_price INT COMMENT '시가',
    high_price INT COMMENT '고가',
    low_price INT COMMENT '저가',
    upper_limit INT COMMENT '상한가',
    lower_limit INT COMMENT '하한가',
    market_cap BIGINT COMMENT '시가총액',
    market_cap_size VARCHAR(20) COMMENT '시가총액규모',
    listed_shares BIGINT COMMENT '상장주수',
    per_ratio INT COMMENT 'PER(소수점2자리*100)',
    pbr_ratio INT COMMENT 'PBR(소수점2자리*100)',

    -- 메타 정보
    data_source VARCHAR(20) DEFAULT 'OPT10001' COMMENT '데이터 출처',
    last_updated DATETIME COMMENT '마지막 업데이트',
    is_active TINYINT(1) DEFAULT 1 COMMENT '활성 여부(1:활성, 0:비활성)',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

    -- 인덱스
    INDEX idx_market (market),
    INDEX idx_last_updated (last_updated),
    INDEX idx_is_active (is_active),
    INDEX idx_market_cap (market_cap),
    INDEX idx_current_price (current_price),

    -- 전문 검색용 인덱스
    FULLTEXT(name)
) ENGINE=InnoDB
COMMENT='주식 기본정보 테이블';

-- 4. 통합 일봉 데이터 테이블 (파티셔닝 적용)
CREATE TABLE daily_prices (
    id BIGINT AUTO_INCREMENT,
    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
    date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',

    -- 가격 정보
    open_price INT COMMENT '시가',
    high_price INT COMMENT '고가',
    low_price INT COMMENT '저가',
    close_price INT COMMENT '종가/현재가',

    -- 거래 정보
    volume BIGINT COMMENT '거래량',
    trading_value BIGINT COMMENT '거래대금',

    -- 변동 정보
    prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
    change_rate INT DEFAULT 0 COMMENT '등락율(소수점2자리*100)',

    -- 메타 정보
    data_source VARCHAR(20) DEFAULT 'OPT10081' COMMENT '데이터 출처',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',

    -- 기본키 (종목코드 + 날짜)
    PRIMARY KEY (stock_code, date, id),

    -- 인덱스
    INDEX idx_date (date),
    INDEX idx_stock_code (stock_code),
    INDEX idx_close_price (close_price),
    INDEX idx_volume (volume),
    INDEX idx_trading_value (trading_value),

    -- 외래키
    FOREIGN KEY (stock_code) REFERENCES stocks(code) ON DELETE CASCADE
) ENGINE=InnoDB
COMMENT='통합 일봉 데이터 테이블'
PARTITION BY RANGE (CAST(SUBSTR(date, 1, 6) AS UNSIGNED)) (
    -- 2020년 이전 데이터
    PARTITION p_before_2020 VALUES LESS THAN (202001),

    -- 2020년 데이터
    PARTITION p_2020_q1 VALUES LESS THAN (202004),
    PARTITION p_2020_q2 VALUES LESS THAN (202007),
    PARTITION p_2020_q3 VALUES LESS THAN (202010),
    PARTITION p_2020_q4 VALUES LESS THAN (202101),

    -- 2021년 데이터
    PARTITION p_2021_q1 VALUES LESS THAN (202104),
    PARTITION p_2021_q2 VALUES LESS THAN (202107),
    PARTITION p_2021_q3 VALUES LESS THAN (202110),
    PARTITION p_2021_q4 VALUES LESS THAN (202201),

    -- 2022년 데이터
    PARTITION p_2022_q1 VALUES LESS THAN (202204),
    PARTITION p_2022_q2 VALUES LESS THAN (202207),
    PARTITION p_2022_q3 VALUES LESS THAN (202210),
    PARTITION p_2022_q4 VALUES LESS THAN (202301),

    -- 2023년 데이터
    PARTITION p_2023_q1 VALUES LESS THAN (202304),
    PARTITION p_2023_q2 VALUES LESS THAN (202307),
    PARTITION p_2023_q3 VALUES LESS THAN (202310),
    PARTITION p_2023_q4 VALUES LESS THAN (202401),

    -- 2024년 데이터
    PARTITION p_2024_q1 VALUES LESS THAN (202404),
    PARTITION p_2024_q2 VALUES LESS THAN (202407),
    PARTITION p_2024_q3 VALUES LESS THAN (202410),
    PARTITION p_2024_q4 VALUES LESS THAN (202501),

    -- 2025년 데이터
    PARTITION p_2025_q1 VALUES LESS THAN (202504),
    PARTITION p_2025_q2 VALUES LESS THAN (202507),
    PARTITION p_2025_q3 VALUES LESS THAN (202510),
    PARTITION p_2025_q4 VALUES LESS THAN (202601),

    -- 미래 데이터 (자동 확장 가능)
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 5. 수집 진행상황 테이블
CREATE TABLE collection_progress (
    stock_code VARCHAR(10) PRIMARY KEY,
    stock_name VARCHAR(100) COMMENT '종목명',
    status VARCHAR(20) DEFAULT 'pending' COMMENT '상태(pending, processing, completed, failed, skipped)',
    attempt_count INT DEFAULT 0 COMMENT '시도 횟수',
    last_attempt_time DATETIME COMMENT '마지막 시도 시간',
    success_time DATETIME COMMENT '성공 시간',
    error_message TEXT COMMENT '오류 메시지',
    data_count INT DEFAULT 0 COMMENT '수집된 데이터 개수',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

    -- 인덱스
    INDEX idx_status (status),
    INDEX idx_attempt_count (attempt_count),
    INDEX idx_last_attempt_time (last_attempt_time),

    -- 외래키
    FOREIGN KEY (stock_code) REFERENCES stocks(code) ON DELETE CASCADE
) ENGINE=InnoDB
COMMENT='수집 진행상황 추적 테이블';

-- 6. 향후 확장을 위한 테이블들 (기본 구조만)

-- 수급 데이터 테이블
CREATE TABLE supply_demand_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,
    date VARCHAR(8) NOT NULL,

    -- 수급 정보 (향후 TR 조사 후 필드 추가)
    institution_buy BIGINT DEFAULT 0 COMMENT '기관 매수',
    institution_sell BIGINT DEFAULT 0 COMMENT '기관 매도',
    foreign_buy BIGINT DEFAULT 0 COMMENT '외국인 매수',
    foreign_sell BIGINT DEFAULT 0 COMMENT '외국인 매도',
    individual_buy BIGINT DEFAULT 0 COMMENT '개인 매수',
    individual_sell BIGINT DEFAULT 0 COMMENT '개인 매도',

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_stock_date (stock_code, date),
    INDEX idx_date (date),
    FOREIGN KEY (stock_code) REFERENCES stocks(code) ON DELETE CASCADE
) ENGINE=InnoDB
COMMENT='일일 수급 데이터 테이블'
PARTITION BY RANGE (CAST(SUBSTR(date, 1, 6) AS UNSIGNED)) (
    PARTITION p_2024_q1 VALUES LESS THAN (202404),
    PARTITION p_2024_q2 VALUES LESS THAN (202407),
    PARTITION p_2024_q3 VALUES LESS THAN (202410),
    PARTITION p_2024_q4 VALUES LESS THAN (202501),
    PARTITION p_2025_q1 VALUES LESS THAN (202504),
    PARTITION p_2025_q2 VALUES LESS THAN (202507),
    PARTITION p_2025_q3 VALUES LESS THAN (202510),
    PARTITION p_2025_q4 VALUES LESS THAN (202601),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 분봉 데이터 테이블 (지정 종목용)
CREATE TABLE minute_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,
    datetime DATETIME NOT NULL COMMENT '일시(YYYY-MM-DD HH:MM:SS)',
    minute_type TINYINT NOT NULL COMMENT '분봉 타입(1:1분, 3:3분, 5:5분)',

    -- 가격 정보
    open_price INT,
    high_price INT,
    low_price INT,
    close_price INT,
    volume BIGINT,

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_stock_datetime_type (stock_code, datetime, minute_type),
    INDEX idx_datetime (datetime),
    INDEX idx_minute_type (minute_type),
    FOREIGN KEY (stock_code) REFERENCES stocks(code) ON DELETE CASCADE
) ENGINE=InnoDB
COMMENT='분봉 데이터 테이블 (지정 종목)'
PARTITION BY RANGE (YEAR(datetime)) (
    PARTITION p_2024 VALUES LESS THAN (2025),
    PARTITION p_2025 VALUES LESS THAN (2026),
    PARTITION p_2026 VALUES LESS THAN (2027),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 7. 유용한 뷰 생성

-- 최신 주식 정보 뷰
CREATE VIEW v_latest_stocks AS
SELECT
    s.code,
    s.name,
    s.market,
    s.current_price,
    s.prev_day_diff,
    s.change_rate / 100.0 AS change_rate_percent,
    s.volume,
    s.market_cap,
    s.last_updated,
    CASE
        WHEN s.last_updated >= DATE_SUB(NOW(), INTERVAL 2 DAY) THEN 'FRESH'
        WHEN s.last_updated >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 'RECENT'
        ELSE 'OLD'
    END AS data_freshness
FROM stocks s
WHERE s.is_active = 1
ORDER BY s.market_cap DESC;

-- 일봉 요약 뷰
CREATE VIEW v_daily_summary AS
SELECT
    dp.stock_code,
    s.name,
    s.market,
    COUNT(*) AS total_records,
    MIN(dp.date) AS first_date,
    MAX(dp.date) AS last_date,
    AVG(dp.close_price) AS avg_close_price,
    AVG(dp.volume) AS avg_volume
FROM daily_prices dp
JOIN stocks s ON dp.stock_code = s.code
GROUP BY dp.stock_code, s.name, s.market;

-- 8. 저장 프로시저 생성

-- 종목별 최근 N일 데이터 조회
DELIMITER //
CREATE PROCEDURE GetRecentDailyData(
    IN p_stock_code VARCHAR(10),
    IN p_days INT DEFAULT 30
)
BEGIN
    SELECT
        date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        trading_value,
        prev_day_diff,
        change_rate / 100.0 AS change_rate_percent
    FROM daily_prices
    WHERE stock_code = p_stock_code
    ORDER BY date DESC
    LIMIT p_days;
END //
DELIMITER ;

-- 시장별 상위 종목 조회
DELIMITER //
CREATE PROCEDURE GetTopStocksByMarket(
    IN p_market VARCHAR(10) DEFAULT 'KOSPI',
    IN p_limit INT DEFAULT 20
)
BEGIN
    SELECT
        code,
        name,
        current_price,
        prev_day_diff,
        change_rate / 100.0 AS change_rate_percent,
        volume,
        market_cap
    FROM stocks
    WHERE market = p_market AND is_active = 1
    ORDER BY market_cap DESC
    LIMIT p_limit;
END //
DELIMITER ;

-- 9. 데이터베이스 설정 최적화
SET GLOBAL innodb_buffer_pool_size = 1073741824; -- 1GB (메모리에 따라 조정)
SET GLOBAL max_connections = 200;
SET GLOBAL innodb_log_file_size = 268435456; -- 256MB

-- 10. 완료 메시지
SELECT 'MySQL 데이터베이스 설정 완료!' AS message;
SELECT 'stock_trading_db 데이터베이스가 생성되었습니다.' AS status;
SELECT 'stock_user 계정으로 접속 가능합니다.' AS user_info;