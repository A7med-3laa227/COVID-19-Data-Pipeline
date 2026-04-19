-- ============================================================
-- COVID-19 Star Schema for PostgreSQL
-- ============================================================

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE        NOT NULL UNIQUE,
    year        SMALLINT    NOT NULL,
    month       SMALLINT    NOT NULL,
    day         SMALLINT    NOT NULL,
    quarter     SMALLINT    NOT NULL,
    month_name  VARCHAR(10) NOT NULL,
    day_of_week VARCHAR(10) NOT NULL
);

-- Dimension: Location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id    SERIAL PRIMARY KEY,
    country_region VARCHAR(255) NOT NULL,
    combined_key   VARCHAR(255) NOT NULL UNIQUE,
    lat            NUMERIC(10, 6),
    long_          NUMERIC(10, 6)
);

-- Fact: COVID Cases
CREATE TABLE IF NOT EXISTS fact_covid_cases (
    fact_id         SERIAL PRIMARY KEY,
    date_id         INT NOT NULL REFERENCES dim_date(date_id),
    location_id     INT NOT NULL REFERENCES dim_location(location_id),
    confirmed       BIGINT  DEFAULT 0,
    deaths          BIGINT  DEFAULT 0,
    recovered       BIGINT  DEFAULT 0,
    active          BIGINT  DEFAULT 0,
    incident_rate   NUMERIC(15, 6),
    case_fatality   NUMERIC(10, 6),
    UNIQUE (date_id, location_id)
);
