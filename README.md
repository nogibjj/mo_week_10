### Data Manipulation

The notebook manipulates the data in several ways, including:

- Converting the date column to a date format.
- Adding columns for the hour, day of week, and month.
- Adding window functions to calculate ranking and statistical measures.

### Data Visualization

The notebook visualizes the data using several charts, including:

- A bar chart of the top device categories.
- A line chart of daily visitors.
- A bar chart of average daily visitors by weekday.
- A histogram of bounce rate.

### Example Plot 



### Example Spark SQL Query

The notebook includes one Spark SQL query that is used to calculate the ranking and statistical measures. The query is: This query calculates several ranking and statistical measures, including the row number, rank, dense rank, count, first, last, minimum, maximum, nth value, lag, lead, percent rank, and ntile.


```SQL
SELECT *,
  date_format(to_timestamp(timestamp), "H") AS hour,
  date_format(to_timestamp(timestamp), "E") AS dayofweek,
  row_number() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS row_num,
  rank() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS rank,
  dense_rank() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS dense_rank,
  count(*) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS count,
  first(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS first,
  last(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS last,
  min(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS min,
  max(`# of Visitors`) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS max,
  nth_value(`# of Visitors`, 2) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS nth,
  lag(`# of Visitors`, 1) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS lag,
  lead(`# of Visitors`, 1) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS lead,
  percent_rank() OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS percent,
  ntile(2) OVER (PARTITION BY `Device Category` ORDER BY timestamp) AS ntile
FROM la_tr
ORDER BY `Device Category`, timestamp

```

### Conclusion

This notebook provides a comprehensive analysis of the website traffic for LA City Infrastructure & Service Requests. The analysis includes insights into key metrics, data manipulation techniques, and visualizations. The notebook can be used as a template for other data analysis projects.
