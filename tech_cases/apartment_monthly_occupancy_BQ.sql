CREATE TABLE
  `case-tech-382003.case_tables.prod_occupancy_rate` AS (
  WITH
    date_array_bookings AS (
    SELECT
      apartment_id,
      booking_id,
      check_in,
      check_out,
      GENERATE_DATE_ARRAY(check_in, check_out, INTERVAL 1 DAY) AS occupied_dates
    FROM
      `case-tech-382003.case_tables.root_bookings` ),
    dates_bookings AS (
    SELECT
      booking_id,
      apartment_id,
      check_in,
      check_out,
      COUNT(occupied_date) OVER(PARTITION BY (apartment_id)) AS occupied_days,
      occupied_date,
      EXTRACT(YEAR
      FROM
        occupied_date) AS year,
      EXTRACT(MONTH
      FROM
        occupied_date) AS month,
    FROM
      date_array_bookings
    CROSS JOIN
      UNNEST(occupied_dates) AS occupied_date
    ORDER BY
      occupied_date )
  SELECT
    DISTINCT booking_id,
    apartment_id,
    check_in,
    check_out,
    year,
    month,
    DATE_TRUNC(CAST(CONCAT(year,'-',month) AS date format 'YYYY-MM'),MONTH) AS reference_date,
    COUNT(occupied_date) OVER(PARTITION BY booking_id, apartment_id, check_in, check_out, year, month ORDER BY booking_id) AS occupied_days,
  IF
    ( DATE_TRUNC(a.check_in,MONTH) = DATE_TRUNC(a.check_out,MONTH)
      AND CAST(FORMAT_DATE('%m',a.check_in)AS int64)=CAST(month AS int64)
      AND CAST(FORMAT_DATE('%m',a.check_out) AS int64) = CAST(month AS int64),CAST((DATE_DIFF(a.check_out,a.check_in,DAY )+1)AS INT64)/(DATE_DIFF(LAST_DAY(a.check_in,MONTH),DATE_TRUNC(a.check_in, MONTH),DAY)+1),
    IF
      (CAST(FORMAT_DATE('%m',a.check_out)AS INT64) = CAST(month AS int64),(CAST(FORMAT_DATE('%d',a.check_out) AS INT64)+1)/((DATE_DIFF(LAST_DAY(a.check_out,MONTH),DATE_TRUNC(a.check_out, MONTH),DAY)+1) -(IFNULL(
            IF
              (a.apartment_id IN (
                SELECT
                  apartment_id
                FROM
                  `case-tech-382003.case_tables.stg_maintenance_days`),(
                SELECT
                  maintenance_days
                FROM
                  `case-tech-382003.case_tables.stg_maintenance_days` AS m
                WHERE
                  m.apartment_id = a.apartment_id
                  AND m.month = CAST(FORMAT_DATE('%m',a.check_out) AS int64)),0),0))),
      IF
        (LAST_DAY(CAST(CONCAT(year,'-',month) AS date format 'YYYY-MM')) < a.check_out
          AND DATE_TRUNC(CAST(CONCAT(year,'-',month) AS date format 'YYYY-MM'),MONTH) > a.check_in,1,
        IF
          (CAST(FORMAT_DATE('%m',a.check_in) AS INT64) = CAST(month AS int64),((CAST(FORMAT_DATE('%d',LAST_DAY(a.check_in)) AS INT64)- CAST(FORMAT_DATE('%d',a.check_in) AS INT64))+1) / ((DATE_DIFF(LAST_DAY(a.check_in,MONTH),DATE_TRUNC(a.check_in, MONTH),DAY)+1)-(IFNULL(
                IF
                  (a.apartment_id IN (
                    SELECT
                      apartment_id
                    FROM
                      `case-tech-382003.case_tables.stg_maintenance_days`),(
                    SELECT
                      maintenance_days
                    FROM
                      `case-tech-382003.case_tables.stg_maintenance_days` AS m
                    WHERE
                      m.apartment_id = a.apartment_id
                      AND m.month = CAST(FORMAT_DATE('%m',a.check_in) AS int64)),0),0))),0)))) AS rate
  FROM
    dates_bookings AS a
  ORDER BY
    booking_id,
    year,
    month )