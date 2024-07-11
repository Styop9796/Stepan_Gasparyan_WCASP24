CREATE TABLE IF NOT EXISTS BL_DM.DIM_DATE (
    event_date_surr_id DATE PRIMARY KEY,
    day_of_week INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL
);


INSERT INTO BL_DM.DIM_DATE (event_date, day_of_week, day_of_month, day_of_year, week_of_year, month, quarter, year)
SELECT
    dt,
    EXTRACT(dow FROM dt) AS day_of_week,
    EXTRACT(day FROM dt) AS day_of_month,
    EXTRACT(doy FROM dt) AS day_of_year,
    EXTRACT(week FROM dt) AS week_of_year,
    EXTRACT(month FROM dt) AS month,
    EXTRACT(quarter FROM dt) AS quarter,
    EXTRACT(year FROM dt) AS year
FROM generate_series('2021-10-01'::date, '2024-02-01'::date, '1 day'::interval) AS dt;
