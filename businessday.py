import pandas as pd
from workalendar.america import Canada
from pandas.tseries.offsets import CustomBusinessDay

def get_9th_business_day(year):
    cal = Canada()
    # Create a custom business day offset that includes Canadian holidays
    can_bus_day = CustomBusinessDay(calendar=cal)
    
    dates = []
    for month in range(1, 13):  # Loop through all months
        first_day_of_month = pd.Timestamp(year=year, month=month, day=1)
        # Calculate the 9th business day
        ninth_business_day = first_day_of_month + 8 * can_bus_day
        dates.append(ninth_business_day.date())
    
    return pd.Series(dates, index=pd.date_range(start=f'{year}-01-01', end=f'{year}-12-01', freq='MS')).rename('9th Business Day')

# Example use for the current year
current_year = 2025
result = get_9th_business_day(current_year)
print(result)