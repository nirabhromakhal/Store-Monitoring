import sys
import threading
import time
import pandas as pd
import datetime
import pytz
from line_profiler_pycharm import profile

# Map report IDs to thread IDs
get_thread_from_report = {}


# Ranges are lists containing pairs of timestamps eg: [[t1,t2], [t3,t4]]
def merge_ranges_if_possible(ranges):
    merged_ranges = [ranges[0]]
    for index in range(len(ranges)):
        if merged_ranges[-1][1] == ranges[index][0]:
            merged_ranges[-1][1] = ranges[index][1]
        elif merged_ranges[-1][1] < ranges[index][0]:
            merged_ranges.append(ranges[index])
    return merged_ranges


def remove_microseconds_from_date(date):
    if '.' in date:
        return date[0:date.find('.')] + date[date.find(' ', date.find('.')):]
    else:
        return date


# Input two timestamps <start> and <end> and compute the total uptime and downtime as timedelta objects
def compute_uptime_downtime(start, end, business_ranges, uptime_ranges, downtime_ranges):
    business_time = datetime.timedelta()
    uptime = datetime.timedelta()
    downtime = datetime.timedelta()

    for business_range in business_ranges:
        if business_range[0] < end and business_range[1] > start:
            business_time += min(business_range[1], end) - max(business_range[0], start)

    for uptime_range in uptime_ranges:
        if uptime_range[0] < end and uptime_range[1] > start:
            uptime += min(uptime_range[1], end) - max(uptime_range[0], start)

    for downtime_range in downtime_ranges:
        if downtime_range[0] < end and downtime_range[1] > start:
            downtime += min(downtime_range[1], end) - max(downtime_range[0], start)

    # Unassumed time is the time for which there was no poll to predict where store was active or inactive,
    # so we consider 50% of this time to be active.
    unassumed_time = business_time - uptime - downtime
    uptime += unassumed_time / 2
    downtime += unassumed_time / 2

    return uptime, downtime


def find_business_ranges(store, current_timestamp, store_hours, store_time_zones):

    # Get store timezone
    store_time_zone = "America/Chicago"
    if store_time_zones['store_id'].isin([store]).any():
        store_time_zone = store_time_zones.loc[store_time_zones['store_id'].isin([store]), 'timezone_str'].values[0]

    # Get business hours for this store
    this_store_hours = store_hours.loc[store_hours['store_id'].isin([store])]

    # Convert business hours start and end times, for the past 7 days from current timestamp, to UTC timestamps
    timestamp_in_store_tz = current_timestamp.astimezone(pytz.timezone(store_time_zone))
    date_in_store_tz = timestamp_in_store_tz.replace(hour=0, minute=0, second=0, microsecond=0)
    business_hours_timestamp_ranges = []

    for i in range(8):

        day = date_in_store_tz.weekday()
        business_24x7 = True

        if (this_store_hours['day'].isin([day])).any():
            business_24x7 = False
            business_hours = this_store_hours.loc[this_store_hours['day'].isin([day])]
            business_start_time = datetime.datetime.strptime(business_hours.loc[:, 'start_time_local'].values[0],
                                                             "%H:%M:%S")
            business_end_time = datetime.datetime.strptime(business_hours.loc[:, 'end_time_local'].values[0],
                                                           "%H:%M:%S")

        if business_24x7:
            business_start_timestamp = date_in_store_tz
            business_end_timestamp = date_in_store_tz + datetime.timedelta(days=1)
        else:
            business_start_timestamp = date_in_store_tz.replace(
                hour=business_start_time.hour,
                minute=business_start_time.minute,
                second=business_start_time.second
            )
            business_end_timestamp = date_in_store_tz.replace(
                hour=business_end_time.hour,
                minute=business_end_time.minute,
                second=business_end_time.second
            )

        business_hours_timestamp_ranges.insert(0, [
            business_start_timestamp.astimezone(pytz.utc), business_end_timestamp.astimezone(pytz.utc)
        ])

        date_in_store_tz = date_in_store_tz - datetime.timedelta(days=1)

    # Merge ranges if possible
    business_hours_timestamp_ranges = merge_ranges_if_possible(business_hours_timestamp_ranges)

    return business_hours_timestamp_ranges


def find_uptime_downtime_ranges(this_store_status_polls, business_hours_timestamp_ranges):

    # Changing this effects the time period we assume from a single poll
    CHOSEN_MARGIN = datetime.timedelta(minutes=30)

    # Find uptime and downtime ranges for every business_hours_timestamp_range
    uptime_ranges = []
    downtime_ranges = []

    for business_hours_timestamp_range in business_hours_timestamp_ranges:

        # Find polls within this business_hours_timestamp_range
        polls_within_range = this_store_status_polls.loc[
            (this_store_status_polls['timestamp_utc'] >= business_hours_timestamp_range[0]) &
            (this_store_status_polls['timestamp_utc'] <= business_hours_timestamp_range[1])
            ]

        # Sort the status polls by timestamp
        polls_within_range = polls_within_range.sort_values('timestamp_utc')

        # Add upper and lower margins for each poll,
        # i.e. the timestamps on either side of the poll, up to which we assume that the poll status is true
        polls_within_range['upper_timestamp'] = ''
        polls_within_range['lower_timestamp'] = ''

        #polls_within_range.reset_index(drop=True, inplace=True)  # reset row labels to start from 0,1...
        # Find indices of the columns timestamp_utc, lower_timestamp, and upper_timestamp
        status_index = polls_within_range.columns.get_loc('status')
        timestamp_utc_index = polls_within_range.columns.get_loc('timestamp_utc')
        lower_timestamp_index = polls_within_range.columns.get_loc('lower_timestamp')
        upper_timestamp_index = polls_within_range.columns.get_loc('upper_timestamp')

        for i in range(len(polls_within_range)):
            present_poll_timestamp = polls_within_range.iat[i, timestamp_utc_index]
            if i == 0:
                polls_within_range.iat[i, lower_timestamp_index] = business_hours_timestamp_range[0]
            if i == len(polls_within_range) - 1:
                polls_within_range.iat[i, upper_timestamp_index] = business_hours_timestamp_range[1]
            if i > 0:
                previous_poll_timestamp = polls_within_range.iat[i - 1, timestamp_utc_index]
                polls_within_range.iat[i, lower_timestamp_index] = previous_poll_timestamp + \
                                                              (present_poll_timestamp - previous_poll_timestamp) / 2
            if i < len(polls_within_range) - 1:
                next_poll_timestamp = polls_within_range.iat[i + 1, timestamp_utc_index]
                polls_within_range.iat[i, upper_timestamp_index] = present_poll_timestamp + \
                                                              (next_poll_timestamp - present_poll_timestamp) / 2
            polls_within_range.iat[i, lower_timestamp_index] = max(polls_within_range.iat[i, lower_timestamp_index],
                                                              present_poll_timestamp - CHOSEN_MARGIN)
            polls_within_range.iat[i, upper_timestamp_index] = min(polls_within_range.iat[i, upper_timestamp_index],
                                                              present_poll_timestamp + CHOSEN_MARGIN)

        # Find the uptime and downtime ranges for all polls within this business_hours_timestamp_range
        for i in range(len(polls_within_range)):
            if polls_within_range.iat[i, status_index] == "active":
                uptime_ranges.append(
                    [polls_within_range.iat[i, lower_timestamp_index], polls_within_range.iat[i, upper_timestamp_index]])
            else:
                downtime_ranges.append(
                    [polls_within_range.iat[i, lower_timestamp_index], polls_within_range.iat[i, upper_timestamp_index]])

    return uptime_ranges, downtime_ranges


# Trigger Report function to be executed in a different thread
def trigger_report_function(report_id):

    # Note start time
    execution_start_time = time.time()

    # Add mapping from report ID to thread ID in dictionary
    get_thread_from_report[report_id] = threading.get_ident()

    # Read the csv files into pandas Dataframes
    store_status = pd.read_csv("store_status.csv")
    store_hours = pd.read_csv("store_hours.csv")
    store_time_zones = pd.read_csv("store_time_zones.csv")

    # Convert timestamp strings to datetime objects
    store_status['timestamp_utc'] = store_status['timestamp_utc'].map(remove_microseconds_from_date)
    store_status['timestamp_utc'] = pd.to_datetime(store_status['timestamp_utc'], format="%Y-%m-%d %H:%M:%S %Z")

    # Select max datetime as current timestamp
    current_timestamp = store_status['timestamp_utc'].max()
    print("\n", current_timestamp)

    # Find the last hour, day, and week timestamp in utc
    last_hour_timestamp = current_timestamp - datetime.timedelta(hours=1)
    last_day_timestamp = current_timestamp - datetime.timedelta(days=1)
    last_week_timestamp = current_timestamp - datetime.timedelta(weeks=1)

    print("\n", last_week_timestamp, last_hour_timestamp, last_day_timestamp, "\n")

    # Get all store IDs
    store_IDs = store_status['store_id'].unique()

    # Distribute the polls into different dataframes for each store
    polls_for_each_store = store_status \
        .loc[store_status['timestamp_utc'] >= (last_week_timestamp - datetime.timedelta(days=1))] \
        .groupby('store_id')

    # Generate report for all stores
    report = pd.DataFrame(columns=[
        'store_id',
        'uptime_last_hour',
        'uptime_last_day',
        'uptime_last_week',
        'downtime_last_hour',
        'downtime_last_day',
        'downtime_last_week'
    ])

    # Keep status of how much is completed
    completion_percent = 0

    for store in store_IDs:

        # Get business ranges in UTC up to last week
        business_hours_timestamp_ranges = find_business_ranges(store, current_timestamp, store_hours, store_time_zones)

        # Status polls of this store
        this_store_status_polls = polls_for_each_store.get_group(store)

        # Get uptime and downtime ranges in UTC
        uptime_ranges, downtime_ranges = find_uptime_downtime_ranges(this_store_status_polls, business_hours_timestamp_ranges)

        # Find uptime and downtime for last hour
        uptime_last_hour, downtime_last_hour = compute_uptime_downtime(
            last_hour_timestamp,
            current_timestamp,
            business_hours_timestamp_ranges,
            uptime_ranges,
            downtime_ranges
        )

        # Find uptime and downtime for last day
        uptime_last_day, downtime_last_day = compute_uptime_downtime(
            last_day_timestamp,
            current_timestamp,
            business_hours_timestamp_ranges,
            uptime_ranges,
            downtime_ranges
        )

        # Find uptime and downtime for last week
        uptime_last_week, downtime_last_week = compute_uptime_downtime(
            last_week_timestamp,
            current_timestamp,
            business_hours_timestamp_ranges,
            uptime_ranges,
            downtime_ranges
        )

        # Append store uptime downtime information to dataframe
        store_uptime_downtime = pd.Series([
            store,
            round(uptime_last_hour.total_seconds() / 60),
            round(uptime_last_day.total_seconds() / 3600),
            round(uptime_last_week.total_seconds() / 3600),
            round(downtime_last_hour.total_seconds() / 60),
            round(downtime_last_day.total_seconds() / 3600),
            round(downtime_last_week.total_seconds() / 3600)
        ], index=report.columns)
        report = pd.concat([report, store_uptime_downtime.to_frame().T], ignore_index=True)

        # Update completion percentage
        completion_percent += 1.0 / len(store_IDs)

        sys.stdout.write("\r")
        sys.stdout.write("Completion: " + str(round(100 * completion_percent, 2)) + " %")

    # Save report using current_timestamp as ID
    report.to_csv(str(report_id) + ".csv", index=False)

    # Note end time
    execution_end_time = time.time()
    print("\n", (execution_end_time-execution_start_time)/60, " minutes taken.\n")

    # Delete mapping from report ID to thread ID
    del get_thread_from_report[report_id]
