import pandas as pd
from pandas.tseries.offsets import MonthEnd


def create_survival_dataframe(df_in, start_event_column, primary_event_column,
                              competing_event_column, censor_date_column,
                              id_column='NEWNHSNO', end_event_priority='first',
                              additional_columns=None):
    """
    Processes the input DataFrame to a survival dataframe
    Adds start_date, primary_event_date, competing_event_date, end_date,
     and time_days columns, considering a censor date.

    Parameters:
    - df_in: DataFrame containing the data.
    - start_event_column: String, column name for the start event date.
    - primary_event_column: String, column name for the primary event date.
    - competing_event_column: String, column name for the competing event date.
    - censor_date_column: String, column name for the censor date.
    - end_event_priority: String, either 'primary', 'competing', or 'first',
        indicating which event should be considered as end_date if both occur.
        Default is 'first'.

    Returns:
    - DataFrame with the new columns added.
    """
    # Copy to avoid in place edits
    df = df_in.copy(deep=True)

    # Check end_date_priority argument
    assert (
        end_event_priority in ['first', 'competing', 'primary']
    ), f"Unexpected argument {end_event_priority}"

    # Ensure dates are in datetime format for all event and censor columns
    date_columns = [start_event_column, primary_event_column, competing_event_column,
                    censor_date_column]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])

    # Initialize new columns based on existing data
    df['start_date'] = df[start_event_column]
    df['primary_event_date'] = df[primary_event_column]
    df['competing_event_date'] = df[competing_event_column]
    df['censor_date'] = df[censor_date_column]

    # Determine end_date based on events and censor date
    def determine_end_date(row):
        events = {
            "primary": row['primary_event_date'],
            "competing": row['competing_event_date'],
            "censor": row['censor_date']
        }

        # Filter out None values
        events = {event: date for event, date in events.items() if not pd.isna(date)}

        if not events:
            raise ValueError("Row with no primary, competing or censor date found")

        # Set priority date if required
        if "primary" in events and "competing" in events:
            if end_event_priority == "primary":
                events.pop("competing")
            elif end_event_priority == 'competing':
                events.pop("primary")
        return min([date for date in events.values()])

    df['end_date'] = df.apply(determine_end_date, axis=1)

    # Ensure all end_dates calculated
    assert (
        not df['end_date'].isna().any()
    ), "Error calculating end dates: Missing end dates"

    # Ensure end_date is not before start_date
    assert (
        (df['end_date'] >= df['start_date']).all()
    ), "Error calculating end dates: End dates before start"

    # Calculate the time in days from start to end
    df['time_at_risk'] = (df['end_date'] - df['start_date']).dt.days

    # Column ordering
    if not additional_columns:
        additional_columns = []
    out_cols = [
        id_column, *additional_columns, "start_date",
        "primary_event_date", "competing_event_date",
        "censor_date", "end_date", "time_at_risk"
    ]

    return df[out_cols]


def expand_to_monthly(
        df_survival, id_column, start_date_column='start_date',
        end_date_column='end_date', additional_columns=None,
        time_checksum=None
):
    """
    Expands a survival DataFrame to monthly records,
    calculating the number of days contributed by each record in each month.

    Parameters:
    - df_survival: DataFrame, the survival data with start and end dates.
    - start_date_column: String, the column name for start dates.
    - end_date_column: String, the column name for end dates.
    - id_column: String, the column name for the identifier.
    - additional_columns: List of strings, additional column names to be included.
    - time_checksum: Int - expected sum of time

    Returns:
    - DataFrame, expanded to monthly records with calculated days contributed per month.
    """
    monthly_records = []

    for index, row in df_survival.iterrows():
        start_month = row[start_date_column].replace(day=1)
        end_month = row[end_date_column] + MonthEnd(0)
        date_range = pd.date_range(start=start_month, end=end_month, freq='MS')

        for date in date_range:
            # Initial assumption: contribute all days of the month
            days_in_month = (date + MonthEnd(0)).day
            contributed_days = days_in_month

            # Adjust for the start month
            if date.year == row[start_date_column].year and date.month == row[start_date_column].month:
                contributed_days = days_in_month - row[start_date_column].day + 1

            # Adjust for the end month
            if date.year == row[end_date_column].year and date.month == row[end_date_column].month:
                if date == start_month:
                    contributed_days = row[end_date_column].day - row[
                        start_date_column].day + 1
                else:
                    contributed_days = row[end_date_column].day

            # Zero out contributed days for months after an event has occurred
            if date > row[end_date_column]:
                contributed_days = 0

            monthly_record = {
                id_column: row[id_column],
                start_date_column: row[start_date_column],
                end_date_column: row[end_date_column],
                'month': date.strftime('%b-%y'),
                'time': contributed_days
            }

            if additional_columns:
                for col in additional_columns:
                    monthly_record[col] = row[col]

            monthly_records.append(monthly_record)

    output_df = pd.DataFrame(monthly_records)

    # Reorder Columns
    output_df = output_df[
        [id_column, *additional_columns, end_date_column, 'month', 'time']]

    # Assertion tests
    assert (
            output_df['time'].max() <= 31
    ), f"Process failure: Invalid Time exceeding 31 days ({output_df['time'].max()})"
    assert (
            output_df['time'].min() >= 1
    ), f"Process failure: Invalid Time of less than 1 days ({output_df['time'].min()})"
    if time_checksum:
        assert (
                output_df['time'].sum() != time_checksum
        ), f"Process failure: Checksum ({output_df['time'].sum()}) != ({time_checksum})"

    return output_df
