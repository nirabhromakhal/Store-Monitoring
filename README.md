# Store-Monitoring

In order to start the flask application, run `loop.py`:
```
$ python loop.py
```
`loop.py` contains two APIs, `trigger_report`, and `get_report`. The report ID is generated using the current system time in seconds, hence, it is unique, and the report generation process takes place in a separate thread. The report ID is mapped to the thread ID in a dictionary, and unmapped when the thread has finished saving the report as `<report_id>.csv`. This allows us to check whether a particular report is stil being generated or has completed.

If the report generation is completed, we send the binary content of `<report_id>.csv` encoded as a base64 string, in the response of the API `get_report`. This base64 string can be easily decoded and saved as the original file. This is shown in `test.py`.


# Report Generation

Report generation is done using a function `trigger_report_generation`, which is present in `trigger_report.py`. This also contains some helper functions which are required to compute uptime and downtime.

We have 3 data sources:

* `store_status.csv` - This has 3 columns `(store_id, timestamp_utc, status)` where status is active or inactive. All timestamps are in UTC
* `store_hours.csv` - This contains the business hours of all the stores. The times are in local time zone. Schema of this data is `(store_id, dayOfWeek(0=Monday, 6=Sunday), start_time_local, end_time_local)`. 
* `store_time_zones.csv` - This contains the timezone for the stores, schema is `(store_id, timezone_str)`

First, we obtain the `current_timestamp` in UTC as the max of all given timestamps in `store_status.csv`. Then we compute the `last_hour_timestamp`, `last_day_timestamp`, and `last_week_timestamp`, going backward from the current timestamp, all in UTC.

## Computing business hours timestamps for the last week for each store

For a particular store, we convert the `current_timestamp` to the timezone of the store, and set time to 00:00:00 (midnight). This gives us the corresponding `date_in_store_tz` in the timezone of the store. Starting from the current timestamp, we iterate over the previous 7 dates (total 8 dates), and for each date, we obtain the business hours, depending on the day of the week. Next, we convert the business hours into business start and end timestamps in UTC. These two timestamps are appended to a `business_hours_timestamp_ranges` array in the form `[business_start_timestamp_utc, business_end_timestamp_utc]` for each date.

## Poll Interpolation

We only consider the polls having timestamps higher than `last_week_timestamp`. For each store, we sort the polls in order of their timestamps. In a week, we can have around 24*7 polls for one store. So sorting this, for every store, takes minimal time.

Now, because it is given that the stores are polled roughly every hour, we have chosen a parameter `CHOSEN_MARGIN` which is set to 30 minutes. This means that for an *active* poll with timestamp T, we assume that the store was active from `T-30` to `T+30`, provided that no other poll was taken in this range `[T-30,T+30]`. For an inactive poll with timestamp T, we assume that the store was inactive in the range `[T-30,T+30]`.

**Note**: We do not consider the polls not falling within a `business_hour_timestamp_range`.

For each poll, we compute two margins, `upper_margin` and `lower_margin`. These margins are basically timestamps upto which we assume the poll status to be valid, i.e.  between these timestamps we consider the store to have the same status as the poll.

If T1, T2, and T3 are the timestamps of 3 polls taken consecutively during the same day i.e. within a single `business_hour_timestamp_range` which is `[BS,BE]`,
`lower_margin` of T1 is `max(T1-CHOSEN_MARGIN, BS)`
`upper_margin` of T1 is `min(T1+CHOSEN_MARGIN, (T1+T2)/2)`
`lower_margin` of T2 is `max(T2-CHOSEN_MARGIN, (T1+T2)/2)`
`upper_margin` of T2 is `min(T2+CHOSEN_MARGIN, (T2+T3)/2)`
`lower_margin` of T3 is `max(T3-CHOSEN_MARGIN, (T2+T3)/2)`
`upper_margin` of T3 is `min(T3+CHOSEN_MARGIN, BE)`

After computing the `upper_margin` and `lower_margin` for each poll, we compute `uptime_ranges`, and `downtime_ranges`. For an active poll with margins S and T, we add `[S,T]` to `uptime_ranges`, and for inactive poll, we add `[S,T]` to `downtime_ranges`.

## Computing uptime and downtime between two timestamps

Now, we have a function `compute_uptime_downtime(start, end, business_ranges, uptime_ranges, downtime_ranges)` which computes the aggregate time delta of the `uptime_ranges` and `downtime_ranges` overlapping partly or fully between the timestamps start and end (consider start and end to be `last_week_timestamp` and `current_timestamp` for instance), in two variables `uptime` and `downtime`. It also computes the aggregate time delta of the `business_ranges` overlapping partly or fully between the timestamps start and end, in a variable `business_time`. 

Now, `uptime` and `downtime` refers to the total times for which we have assumed the store to be active or inactive. If, however, there was no poll for a particular business day, then, this time is being called `unassumed_time`
```
unassumed_time = business_time - uptime - downtime
```
We consider the store is active only 50% of this unassumed time. So we add half of `unassumed_time` to both `uptime` and `downtime`. This fraction 50% can be changed and tested with other values.

# Final Thoughts

The report generation process takes around 10 minutes for ~14,000 stores and ~1,000,000 polls.
