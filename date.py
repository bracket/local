from datetime import date, datetime, time, timedelta;

def to_datetime(t):
    if type(t) == datetime: return t;
    return datetime(t.year, t.month, t.day);

def to_date(d):
    if type(d) == date: return d;
    return date(d.year, d.month, d.day);

def to_time(t):
    if type(t) == time: return t;
    return time(t.hour, t.seconds / 60, t.seconds % 60, t.microseconds);

def to_timedelta(d):
    if type(d) == timedelta: return d;
    return timedelta(hours = t.hour, seconds = t.minute * 60 + t.second, microseconds = t.microsecond);

def broadcast_day_inc(t): return start_of_broadcast_day(t + timedelta(1));

def date_inc(d): return d + timedelta(1);

def date_dec(d): return d - timedelta(1);

def week_inc(d): return start_of_week(d + timedelta(7));

def week_dec(d): return start_of_week(d - timedelta(7));

def month_inc(d):
    dyear, month = divmod(d.month, 12);
    return date(d.year + dyear, month + 1, 1);

def month_dec(d):
    dyear, month = divmod(d.month - 2, 12)
    return date(d.year + dyear, month + 1, 1);

def broadcast_month_inc(d):
    return start_of_broadcast_month(month_inc(broadcast_month(d)));

def broadcast_month_dec(d):
    return start_of_broadcast_month(month_dec(broadcast_month(d)));

def date_period_range(from_date, to_date, inc):
    while from_date <= to_date:
        yield from_date;
        from_date = inc(from_date);
    
def dates_in_range(from_date, to_date):
    return date_period_range(from_date, to_date, date_inc);

def weeks_in_range(from_date, to_date):
    return date_period_range(start_of_week(from_date), to_date, week_inc);

def months_in_range(from_date, to_date):
    return date_period_range(start_of_month(from_date), to_date, month_inc);

def broadcast_months_in_range(from_date, to_date):
    return (broadcast_month(d)
        for d in date_period_range(start_of_broadcast_month(from_date), to_date, broadcast_month_inc));

def start_of_day(t): return to_datetime(to_date(t));

def end_of_day(t): return start_of_day(t) + timedelta(days = 1, microseconds = -1);

def broadcast_day(d):
    if isinstance(d, date): return d;
    d -= timedelta(hours = 6);
    return date(d.year, d.month, d.day);

def start_of_broadcast_day(t):
    return to_datetime(broadcast_day(t)) + timedelta(hours = 6);

def end_of_broadcast_day(t):
    return to_datetime(broadcast_day(t)) + timedelta(days = 1, hours = 6, microseconds = -1);

def start_of_week(d): return d - timedelta(d.weekday())

def end_of_week(d): return d + timedelta(6 - d.weekday());

def start_of_month(d): return date(d.year, d.month, 1);

def end_of_month(d): return month_inc(start_of_month(d)) - timedelta(1);

def broadcast_month(d): return start_of_month(end_of_week(d));

def start_of_broadcast_month(d): return start_of_week(broadcast_month(d));

def end_of_broadcast_month(d): return broadcast_month_inc(d) - timedelta(1);

def dates_in_week(d):
    return dates_in_range(start_of_week(d), end_of_week(d));

def dates_in_month(d):
    return dates_in_range(start_of_month(d), end_of_month(d));

def dates_in_broadcast_month(d):
    return dates_in_range(start_of_broadcast_month(d), end_of_broadcast_month(d));

def segment_to_time_period(inc, from_time, to_time):
    while from_time <= to_time:
        next_time = inc(from_time);
        yield (from_time, min(to_time, next_time - timedelta(microseconds = 1)));
        from_time = next_time;

def segment_to_day(from_time, to_time):
    if isinstance(from_time, date): from_time = start_of_day(from_time);
    if isinstance(to_time, date): to_time = end_of_day(to_time);

    return segment_to_time_period(date_inc, from_time, to_time);

def segment_to_broadcast_day(from_time, to_time):
    if isinstance(from_time, date): from_time = start_of_broadcast_day(from_time);
    if isinstance(to_time, date): to_time = end_of_broadcast_day(to_time);

    return segment_to_time_period(broadcast_day_inc, from_time, to_time);

def segment_to_date_period(inc, from_date, to_date):
    while from_date <= to_date:
        next_date = inc(from_date);
        yield (from_date, min(to_date, next_date - timedelta(1)));
        from_date = next_date;

def segment_to_week(from_date, to_date):
    return segment_to_date_period(week_inc, from_date, to_date);

def segment_to_month(from_date, to_date):
    return segment_to_date_period(month_inc, from_date, to_date);

def segment_to_broadcast_month(from_date, to_date):
    return segment_to_date_period(broadcast_month_inc, from_date, to_date);

def align_to_days(from_time, to_time):
    return (start_of_day(from_time), end_of_day(to_time));

def align_to_broadcast_days(from_time, to_time):
    return (start_of_broadcast_day(from_time), end_of_broadcast_day(to_time));

def align_to_weeks(from_date, to_date):
    return (start_of_week(from_date), end_of_week(to_date));

def align_to_months(from_date, to_date):
    return (start_of_month(from_date), end_of_month(to_date));

def align_to_broadcast_months(from_date, to_date):
    return (start_of_broadcast_month(from_date), end_of_broadcast_month(to_date));

def broadcast_day_range(d):
    return align_to_broadcast_days(d, d);

def week_range(d):
    return align_to_weeks(d, d);

def month_range(d):
    return align_to_months(d, d);

def broadcast_month_range(d):
    return align_to_broadcast_months(d, d);
