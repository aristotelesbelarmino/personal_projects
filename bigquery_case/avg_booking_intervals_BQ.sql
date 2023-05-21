CREATE TABLE
  `case-tech-382003.case_tables.prod_avg_intervals` AS (
  WITH
    initial_data AS(
    SELECT
      apartment_id,
      booking_id,
      booking_date,
      check_in,
    FROM
      `case-tech-382003.case_tables.root_bookings`
    WHERE
      booking_status = 'Approved' ),
    booking_count_table AS(
    SELECT
      DISTINCT apartment_id,
      SUM(DATE_DIFF(check_in,booking_date, DAY)) AS Interval_booking_checkin,
      COUNT(booking_id) AS booking_count,
    FROM
      initial_data
    GROUP BY
      apartment_id )
  SELECT
    *,
    CAST(ROUND((CAST(interval_booking_checkin AS int64)/CAST(booking_count AS int64))) AS INT64) AS avg_time
  FROM
    booking_count_table
  ORDER BY
    apartment_id )
