CREATE TABLE
  `case-tech-382003.case_tables.prod_weekend_booking_frequency` AS (
  WITH
    check_count AS(
    SELECT
      apartment_id,
      check_in,
      CASE
        WHEN FORMAT_DATE('%A',check_in) IN ('Friday', 'Saturday', 'Monday') THEN 1
      ELSE
      0
    END
      AS check_in_weekend,
      check_out,
      CASE
        WHEN FORMAT_DATE('%A',check_out) IN ('Friday', 'Saturday', 'Monday') THEN 1
      ELSE
      0
    END
      AS check_out_weekend
    FROM
      `case-tech-382003.case_tables.root_bookings`)
  SELECT
    apartment_id,
    SUM(check_in_weekend) AS check_in_weekend_count,
    SUM(check_out_weekend) AS check_out_weekend_count
  FROM
    check_count
  GROUP BY
    apartment_id
  ORDER BY
    COUNT(apartment_id) DESC )