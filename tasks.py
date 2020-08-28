import invoke

from pathlib import Path
from datetime import date, datetime, timedelta
from date import months_in_range, month_inc

from collections import defaultdict, namedtuple

from groupby import sum_groupby

from pprint import pprint
from prettytable import PrettyTable

import functools

memoize = functools.lru_cache()

FILE = Path(__file__)
HERE = FILE.parent
DATA = HERE / 'data'

def identity(x):
    return x

def parse_date(d):
    import dateutil.parser as parser

    try:
        return parser.parse(d).date()
    except ValueError:
        return d


def parse_float(f):
    try:
        return float(f.replace(',', ''))
    except ValueError:
        return f


def fmt_month(m):
    return '{}/{}'.format(m.month, m.year)


def fmt_float(f):
    return '{: 2.02f}'.format(f)


def fmt_value(v):
    if isinstance(v, date):
        return fmt_month(v)
    elif isinstance(v, float):
        return fmt_float(v)

    return v


csv_converters = {
    'end_date'   : parse_date,
    'hourly'     : float,
    'hours'      : float,
    'name'       : str.strip,
    'revenue'    : float,
    'role'       : str.strip,
    'start_date' : parse_date,
    'total'      : float,
    'type'       : str.strip,
}


def read_csv(path):
    path = Path(path)

    with path.open() as fd:
        header = [ h.strip() for h in next(fd).split(',') ]

        for line in fd:
            yield { h : csv_converters[h](v) for h, v in zip(header, line.split(',')) }

RevenueRecord = namedtuple('RevenueRecord', [ 'type', 'date', 'revenue' ])

ONE_DAY = timedelta(1)

def revenue_records(path):
    for record in read_csv(path):
        current = record['start_date']
        end_date = record['end_date']

        delta = (end_date - current) + ONE_DAY
        delta = delta.total_seconds() / ONE_DAY.total_seconds()

        while current <= end_date:
            yield RevenueRecord(
                record['type'],
                current,
                record['revenue'] / delta,
            )

            current += ONE_DAY

@invoke.task
def print_total_revenue(context):
    total = 0.

    for r in revenue_records(DATA / 'revenue.csv'):
        total += r.revenue


    print('total', total)
    


def month_of(d):
    return date(d.year, d.month, 1)


def revenue_by_day(path, type='total'):
    return {
        r.date : r.revenue
        for r in revenue_records(path)
        if r.type == type
    }


def revenue_by_month(path):
    return sum_groupby(
        (month_of(record.date), record.revenue)
        for record in revenue_records(path)
    )


@invoke.task
def print_revenue_by_day(context):
    by_day = revenue_by_day(DATA / 'revenue.csv')

    for record in sorted(by_day.items()):
        print(record)
        break


@invoke.task
def print_revenue_by_month(context):
    pprint(revenue_by_month(DATA / 'revenue.csv'))


PayrollRecord = namedtuple('PayrollRecord',
    [
        'start_date', 'end_date',
        'name', 'role',
        'hours', 'hourly', 'total'
    ]
)

def payroll_records(path):
    for record in read_csv(path):
        yield PayrollRecord(**record)


def payroll_by_role(path):
    return sum_groupby(
        (r.role, r.total) for r in payroll_records(path)
    )


@invoke.task
def model_raise(context):
    by_person = sum_groupby(
        # (r.name, r.hours * 20) if r.name == 'calvin' else (r.name, r.hours * 15)
        (r.name, r.hours * 15)
        for r in payroll_records(DATA / 'payroll.csv')
    )

    table = PrettyTable(['name', 'total'])
    for r in sorted(by_person.items()):
        table.add_row(r)

    print(table)
    print()
    print()


    by_role = sum_groupby(
        (r.role, r.hours * 15)
        # (r.role, r.hours * 20) if r.name == 'calvin' else (r.role, r.hours * 15)
        for r in payroll_records(DATA / 'payroll.csv')
    )

    table = PrettyTable([ 'role', 'total' ])

    for r in sorted(by_role.items()):
        table.add_row(r)
    
    print(table)


@invoke.task
def print_payroll_records(context):
    records = payroll_records(DATA / 'payroll.csv')

    table = PrettyTable([ 'name', 'hours', 'hourly', 'expected', 'actual' ])

    for r in records:
        table.add_row([
            r.name,
            r.hours, r.hourly, round(r.hours * r.hourly, 2),
            r.total
        ])

    print(table)


@invoke.task
def print_payroll_by_role(context):
    table = PrettyTable([ 'role', 'total' ])

    records = sorted(payroll_by_role(DATA / 'payroll.csv').items())
    for r in records:
        table.add_row(r)

    print(table)


def profit_loss_records(path, filter=None):
    path = Path(path)

    records = { }

    with path.open() as fd:
        header = [ parse_date(h.strip())  for h in next(fd).split('|') ]

        for line in fd:
            line = line.strip()

            if not line:
                continue

            record = {
                h : parse_float(v.strip())
                for h, v in zip(header, line.split('|'))
            }

            category = record['category']

            if filter and category not in filter:
                continue


            for d, v in record.items():
                if isinstance(d, date):
                    key = (category, d)

                    records[key] = v

    return records

incomes = {
    'art sales',
    'food sales',
    'bar sales',
    'lottery commission',
    'refunds',
    'interest',
}

expenses = {
    'accounting',
    'advertising',
    'art commissions',
    'art payments',
    'ask my accountant',
    'automobile',
    'bank services',
    'bar purchases',
    'charitable',
    'cleaning',
    'consulting',
    'contract labor',
    'decorations',
    'depreciation',
    'discrepancies',
    'dishes and furniture',
    'electronic equipment',
    'entertainment',
    'equipment rental',
    'food purchases',
    'insurance other',
    'janitorial',
    'licenses',
    'lottery fees',
    'maintenance',
    'meals',
    'merchant account fees',
    'office supplies',
    'outside services',
    'parking',
    'party page',
    'payroll - cash draw',
    'payroll - officer salary',
    'payroll - officer wages',
    'payroll - other',
    'payroll - regular wages',
    'postage',
    'professional - other',
    'rent',
    'repairs',
    'restaurant operating supplies',
    'restaurant supplies',
    'security',
    'taxes - payroll',
    'telephone',
    'travel',
    'uber',
    'utilities',
    'voided check',
    'workmans comp',
    'ask my accountant',
    'vendor reporting paid',


    'over - cash till',
    'over - bar',
    'over - lottery',
}

@memoize
def fixed_profit_loss_records(path, filter=None):
    records = profit_loss_records(path, filter)

    # Negate expenses for mathematical convenience

    for (k, d), v in records.items():
        if k in expenses:
            records[(k,d)] = -v


    # The filter might remove certain keys so we ignore KeyErrors

    # Swapped bar and food sales

    try:
        month = date(2019, 9, 1)
        left = ('bar sales', month)
        right = ('food sales', month)

        records[left], records[right] = records[right], records[left]
    except KeyError:
        pass


    # Payroll rolled over one month

    try:
        category = 'payroll - regular wages'
        left = (category, date(2019, 9, 1))
        right = (category, date(2019, 10, 1))
        records[left] = records[right] / 2
        records[right] = records[left]
    except KeyError:
        pass


    # Zero out 'payroll - other'

    try:
        category = 'payroll - other'
        start = date(2019, 1, 1)
        end = date(2019, 12, 1)

        for month in months_in_range(start, end):
            records[(category, month)] = 0.
    except KeyError:
        pass


    # Lottery over/under is going wrong direction?
    try:
        category = 'over - lottery'
        months = [
            date(2019, 6, 1),
            date(2019, 7, 1),
        ]

        others = [
            records[(c, m)]
            for c, m in records if c == category and m not in months
        ]

        average = sum(others) / len(others)

        for month in months:
            key = (category, month)
            records[key] = average

            # records[key] = -records[key]
            # records[key] = 0
    except KeyError:
        pass


    return records


@memoize
def totals_2019():
    totals = defaultdict(float)

    records = fixed_profit_loss_records(DATA / 'profit_loss.psv')

    for (c, m), v in records.items():
        totals[c] += v

    return dict(totals)


@invoke.task
def print_totals(context):
    fmt = '{:02.2f}'

    totals = defaultdict(float)

    # records = fixed_profit_loss_records(DATA / 'profit_loss.psv', filter=PHASE_0)
    records = fixed_profit_loss_records(DATA / 'profit_loss.psv')

    for (c, m), v in records.items():
        totals[c] += v

    table = PrettyTable(('category', 'total'))
    table.align['total'] = 'r'

    rows = sorted(totals.items(), key=lambda _: abs(_[1]), reverse=True)
    
    for c, v in rows:
    # for c, v in rows[:7]:
    # for c, v in totals.items():
        table.add_row((c, fmt.format(v)))

    print(table)
    print()

    table = PrettyTable([ '', 'value'])
    table.align['value'] = 'r'

    total_income = sum(v for c,v in rows if c in incomes)
    table.add_row(['total_income', fmt.format(total_income)])

    total_cost = sum(v for c,v in rows if c in expenses)
    table.add_row([ 'total_cost', fmt.format(total_cost) ])

    total_net = sum(v for _, v in rows)
    table.add_row(['net', fmt.format(total_net)])

    avg_income_per_month = total_income / 12.
    table.add_row([ 'income per month', fmt.format(avg_income_per_month) ])

    avg_cost_per_month = total_cost / 12.
    table.add_row([ 'cost per month', fmt.format(avg_cost_per_month) ])

    avg_net_per_month = total_net / 12.
    table.add_row([ 'net per month', fmt.format(avg_net_per_month) ])

    print(table)


@invoke.task
def print_set_deltas(context):
    categories = { c for c, _ in profit_loss_records(DATA / 'profit_loss.psv') }

    all = incomes | expenses
    delta = categories - all

    pprint(delta)


PHASE_0 = {
    'bank services',
    'uber',
    'charitable',
    'depreciation',
    'dishes and furniture',
    'lottery fees',
    'workmans comp',
    'insurance other',
    'licenses',
    'office supplies',
    'outside services',
    'party page',
    'accounting',
    'consulting',
    'professional - other',
    'discrepancies',
    'rent',
    'cleaning',
    'repairs',
    'security',
    'maintenance',
    'telephone',
    'utilities',
    'voided check',
    'interest',
    # 'ask my accountant',
    # 'vendor reporting paid',
}

def model_phase_1():
    fmt = '{:02.2f}'

    model_week = {
        date(2019, 7, 20) : 181.50,
        date(2019, 7, 21) : 674.50,
        date(2019, 7, 22) : 252.50,
        date(2019, 7, 23) : 201.50,
        date(2019, 7, 24) : 439.00,
        date(2019, 7, 25) : 611.50,
        date(2019, 7, 26) : 166.50,
        date(2019, 7, 27) : 404.50,
    }

    show_days = {
        date(2019, 7, 21),
        date(2019, 7, 25),
    }

    slow_day = [ v for k, v in model_week.items() if k not in show_days ]
    slow_day = sum(slow_day) / len(slow_day)

    show_day = [ v for k, v in model_week.items() if k in show_days ]
    show_day = sum(show_day) / len(show_day)

    none_weekly = 7 * slow_day
    once_weekly = 1 * show_day + 6 * slow_day
    twice_weekly = 2 * show_day + 5 * slow_day

    table = PrettyTable([ '', 'value' ])
    table.align['value'] = 'r'

    table.add_row([ 'slow_day', fmt.format(slow_day) ])
    table.add_row([ 'show_day', fmt.format(show_day) ])
    table.add_row([ 'none_weekly', fmt.format(none_weekly) ])
    table.add_row([ 'once_weekly', fmt.format(once_weekly) ])
    table.add_row([ 'twice_weekly', fmt.format(twice_weekly) ])

    model_july = 2 * once_weekly + 1 * twice_weekly + none_weekly
    table.add_row([ 'model_july', fmt.format(model_july) ])

    # Normally:
    #   70% of grosses are in bar sales
    #   15% of grosses are in food sales
    #   15% of grosses are lottery
    #
    # In model July:
    #   Only grosses included are food and bar
    #     So that would normally only be 85% of total gross not counting
    #     lottery.  But lottery is only operating a 3/5 efficency right now
    #   So lottery would normally be be gross / .85 - gross = gross * ( 1/ .85 - 1)
    #
    #   And that is totally wrong.  Lottery basically always grosses the same.

    model_lottery = model_july * (1/.85 - 1)
    table.add_row([ 'pre-adjusted lottery', fmt.format(model_lottery) ])

    model_lottery *= (3/5)
    table.add_row([ 'adjusted lottery', fmt.format(model_lottery) ])

    actual_lottery = sum([
        983.65,
        1276.66,
        1522.41,
        1170.28,
        822.37
    ])

    table.add_row([ 'actual lottery', fmt.format(actual_lottery) ])

    full_capacity_lottery = (5 * actual_lottery / 3)
    table.add_row([ 'full capacity lottery', fmt.format(full_capacity_lottery) ])


    # adjusted_model_july = model_july + model_lottery
    adjusted_model_july = model_july + actual_lottery
    table.add_row([ 'adjusted model july', fmt.format(adjusted_model_july) ])

    gross = adjusted_model_july

    # Now for the expenses
    bartender_daily = -10 * 15

    bartender_weekly = 7 * bartender_daily
    table.add_row([ 'bartender_weekly', fmt.format(bartender_weekly) ])

    bartender_monthly = bartender_weekly * 4
    table.add_row([ 'bartender_monthlhy', fmt.format(bartender_monthly) ])

    rent_base = -3500 
    rent_ancillary = - 180.68 + - 169.46 + - 110.
    rent = rent_base + rent_ancillary

    table.add_row([ 'rent_base', fmt.format(rent_base) ])
    table.add_row([ 'rent_ancillary', fmt.format(rent_ancillary) ])
    table.add_row([ 'rent', fmt.format(rent) ])

    # Estimates from totals
    bar_gross = .7 * gross
    bar_expense = - bar_gross * .33

    food_gross = .15 * gross
    food_expense = - food_gross * .78

    # Just distributing last years over 12 months
    utilities     = -26483.16 / 12
    entertainment = -22763.88 / 12

    expenses = (
        bartender_monthly
        + rent
        + bar_expense
        + food_expense
        + utilities
        # + entertainment
    )


    table.add_row([ 'utilities', fmt.format(utilities) ])
    table.add_row([ 'entertainment', fmt.format(entertainment) ])
    table.add_row([ 'gross', fmt.format(gross) ])
    table.add_row([ 'expenses', fmt.format(expenses) ])

    net = gross + expenses
    table.add_row([ 'net', fmt.format(net) ])

    print(table)


@invoke.task
def print_phase_1_model(context):
    model_phase_1()


@invoke.task
def print_gross_ratios_by_month(context):
    fmt = '{:02.2f}'

    grosses = {
        'art sales',
        'bar sales',
        'food sales',
        'lottery commission',
        'refunds'
    }

    records = fixed_profit_loss_records(DATA / 'profit_loss.psv')

    header = [ 'month' ]
    header.extend(sorted(grosses))
    header.append('total')

    table = PrettyTable(header)

    for month in months_in_range(date(2019, 1, 1), date(2019, 12, 1)):
        total = sum(v for (c, m), v in records.items() if c in grosses and m == month)

        row = [ month ]

        for g in sorted(grosses):
            value = records[(g, month)] / total
            row.append(fmt.format(value))

        row.append(fmt.format(total))

        table.add_row(row)

    totals_by_category = {
        c : sum(v for (_, m), v in records.items() if _ == c)
        for c in grosses
    }

    total = sum(totals_by_category.values())

    row = [ 'year' ]

    for c in sorted(grosses):
        row.append(fmt.format(totals_by_category[c] / total))

    row.append(total)

    table.add_row(row)

    print(table)


# Guesses
# - If we go back to phase 0 it will last between three to six months
# - Phase 1 will last anywhere between three months to fifteen months
# - Phase 2 we will be open enough to break even
# - Phase 3 will require creation and deployment of a vaccine, which may not be
#   until 2024

models =  r'''
    month      | fucked | pessimistic | oops | likely | lucky | normal
    2020-10-01 | 0      | 0           | 0    | 1      | 1     | 3
    2020-11-01 | 0      | 0           | 0    | 1      | 1     | 3
    2020-12-01 | 0      | 0           | 0    | 1      | 1     | 3

    2021-01-01 | 0      | 0           | 1    | 1      | 2     | 3
    2021-02-01 | 0      | 0           | 1    | 1      | 2     | 3
    2021-03-01 | 0      | 0           | 1    | 1      | 2     | 3

    2021-04-01 | 0      | 1           | 1    | 1      | 2     | 3
    2021-05-01 | 0      | 1           | 1    | 1      | 2     | 3
    2021-06-01 | 0      | 1           | 1    | 1      | 2     | 3

    2021-07-01 | 0      | 1           | 1    | 1      | 2     | 3
    2021-08-01 | 0      | 1           | 1    | 1      | 2     | 3
    2021-09-01 | 0      | 1           | 1    | 1      | 2     | 3

    2021-10-01 | 0      | 1           | 1    | 2      | 3     | 3
    2021-11-01 | 0      | 1           | 1    | 2      | 3     | 3
    2021-12-01 | 0      | 1           | 1    | 2      | 3     | 3

    2022-01-01 | 0      | 1           | 2    | 2      | 3     | 3
    2022-02-01 | 0      | 1           | 2    | 2      | 3     | 3
    2022-03-01 | 0      | 1           | 2    | 2      | 3     | 3

    2022-04-01 | 0      | 1           | 2    | 2      | 3     | 3
    2022-05-01 | 0      | 1           | 2    | 2      | 3     | 3
    2022-06-01 | 0      | 1           | 2    | 2      | 3     | 3

    2022-07-01 | 0      | 2           | 2    | 2      | 3     | 3
    2022-08-01 | 0      | 2           | 2    | 2      | 3     | 3
    2022-09-01 | 0      | 2           | 2    | 2      | 3     | 3

    2022-10-01 | 0      | 2           | 2    | 3      | 3     | 3
    2022-11-01 | 0      | 2           | 2    | 3      | 3     | 3
    2022-12-01 | 0      | 2           | 2    | 3      | 3     | 3
'''

PHASES = { 0, 1, 2, 3 }

@memoize
def parse_models():
    lines = (line for line in map(str.strip, models.splitlines()) if line)

    header = [ h.strip() for h in next(lines).split('|') ]

    by_model = defaultdict(dict)

    for line in lines:
        line = [ r.strip() for r in line.split('|')]
        record = dict(zip(header, line))

        month = parse_date(record['month'])

        for model, phase in record.items():
            if model == 'month':
                continue

            by_model[model][month] = int(phase)

    return dict(by_model)


@invoke.task
def print_months(context):
    start = date(2020, 10, 1)
    end = date(2022, 12, 1)

    for m in months_in_range(start, end):
        print(m)


def sigmoid(alpha, x):
    from math import exp
    e = exp(alpha * x)
    return e / (1 + e)


def reference_month(month):
    reference = month.month
    return date(2019, reference, 1)
    

def bar_sales_by_month_model(month, model):
    months = parse_models()[model]
    phase = months[month]

    if phase == 0:
        percentage = 0.
    elif phase == 1:
        percentage = 1/3
    elif phase == 2:
        percentage = 2/3
    elif phase == 3:
        percentage = 1
    else:
        raise ValueError('phase not valid. phase = {}'.format(phase))

    records = fixed_profit_loss_records(DATA / 'profit_loss.psv')

    key = ('bar sales', reference_month(month))

    return percentage * records[key]


def bar_costs_by_month_model(month, model):
    sales = bar_sales_by_month_model(month, model)

    totals = totals_2019()
    ratio = totals['bar purchases'] / totals['bar sales']

    return ratio * sales


def food_sales_by_month_model(month, model):
    months = parse_models()[model]
    phase = months[month]

    if phase == 0:
        percentage = 0.
    elif phase == 1:
        percentage = 1/3
    elif phase == 2:
        percentage = 2/3
    elif phase == 3:
        percentage = 1
    else:
        raise ValueError('phase not valid. phase = {}'.format(phase))

    records = fixed_profit_loss_records(DATA / 'profit_loss.psv')

    key = ('food sales', reference_month(month))

    return percentage * records[key]


def food_costs_by_month_model(month, model):
    sales = food_sales_by_month_model(month, model)

    totals = totals_2019()
    ratio = totals['food purchases'] / totals['food sales']

    return ratio * sales
    

def rent_by_month(month):
    if month <= date(2020, 9, 1):
        return -3375.

    if month <= date(2022, 9, 1):
        return -3500.

    if month <= date(2024, 9, 1):
        return -3600.


def utilites_by_month(month):
    totals = totals_2019()
    amortized = totals['utilities'] / 12

    return amortized


def payroll_by_month_model(month, model):
    phase = parse_models()[model][month]

    out = 0.

    rick_start = date(2020, 10, 1)
    rick_end = month_of(rick_start + timedelta(180))

    if rick_start <= month <= rick_end:
        pass
        # out += -4500.
        # out += -5000.

    if phase == 0:
        return out

    manager_weekly   = 20 * 35.
    secondary_weekly = 15 * 35.

    next_month = month_inc(month)
    delta = next_month - month

    days = delta.total_seconds() / (24 * 60 * 60)
    weeks = days / 7.

    out -= weeks * (manager_weekly + secondary_weekly)

    return out


def payroll_taxes_by_month_model(month, model):
    totals = totals_2019()

    payroll = payroll_by_month_model(month, model)
    ratio = totals['taxes - payroll'] / totals['payroll - regular wages']

    return ratio * payroll


def lottery_by_month_model(month, model):
    phase = parse_models()[model][month]

    if phase == 0:
        return 0.

    average = totals_2019()['lottery commission'] / 12
    return average


def entertainment_by_month_model(month, model):
    phase = parse_models()[model][month]

    if phase in (0, 1):
        return 0.

    average = totals_2019()['entertainment'] / 12

    if phase == 2:
        return average / 2

    if phase == 3:
        return average


def over_lottery_by_month_model(month, model):
    return 0.


def over_bar_by_month_model(month, model):
    return 0.


class FakeTable(dict):
    def __init__(self, columns):
        pass


    def add_row(self, row):
        self[row[0]] = row[1]


@memoize
def values_by_month_model(month, model):
    phase = parse_models()[model][month]

    table = FakeTable([ '', 'value'])

    table.add_row([ 'model', model ])
    table.add_row([ 'month', month ])
    table.add_row([ 'phase', phase ])

    bar_sales = bar_sales_by_month_model(month, model)
    table.add_row([ 'bar sales', bar_sales ])

    bar_costs = bar_costs_by_month_model(month, model)
    table.add_row([ 'bar costs', bar_costs ])

    food_sales = food_sales_by_month_model(month, model)
    table.add_row([ 'food sales', food_sales ])

    food_costs = food_costs_by_month_model(month, model)
    table.add_row([ 'food costs', food_costs ])

    lottery_commission = lottery_by_month_model(month, model)
    table.add_row([ 'lottery commission', lottery_commission ])

    rent = rent_by_month(month)
    table.add_row([ 'rent', rent ])

    utilities = utilites_by_month(month)
    table.add_row([ 'utilities', utilities ])

    payroll = payroll_by_month_model(month, model)
    table.add_row([ 'payroll - regular wages', payroll ])

    payroll_taxes = payroll_taxes_by_month_model(month, model)
    table.add_row([ 'taxes - payroll', payroll_taxes ])

    entertainment = entertainment_by_month_model(month, model)
    table.add_row([ 'entertainment', entertainment ])

    over_lottery = over_lottery_by_month_model(month, model)
    table.add_row([ 'over - lottery', over_lottery ])

    over_bar = over_bar_by_month_model(month, model)
    table.add_row([ 'over - bar', over_bar ])

    gross = (
        bar_sales
        + food_sales
        + lottery_commission
    )

    costs = (
        bar_costs
        + food_costs
        + rent
        + utilities
        + payroll
        + payroll_taxes
        + entertainment
    )

    net = gross + costs

    table.add_row([ 'gross', gross ])
    table.add_row([ 'costs', costs ])
    table.add_row([ 'net',   net   ])


    # print(table)
    return table


@invoke.task
def print_test_month(context):
    fmt = '{: 2.02f}'

    month = date(2021, 6, 1)
    model = 'likely'

    table = PrettyTable([ '', 'value' ])

    values = values_by_month_model(month, model)

    for k, v in values.items():
        if isinstance(v, float):
            v = fmt.format(v)

        table.add_row([ k, v ])

    print(table)


@invoke.task
def print_models(context):
    models = parse_models()

    pprint(models)


excel_columns = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
excel_columns.extend([ 'A' + a for a in excel_columns ])

line_items = {
    'month'                         : 'Month',
    'phase'                         : 'Phase',

    'bar sales'                     : 'Bar Sales',
    'bar costs'                     : 'Bar Costs',
    'bar purchases'                 : 'Bar Costs',

    'food sales'                    : 'Food Sales',
    'food costs'                    : 'Food Costs',
    'food purchases'                : 'Food Costs',

    'lottery commission'            : 'Lottery Commission',
    'lottery fees'                  : 'Lottery Fees',

    'payroll - regular wages'       : 'Payroll Regular Wages',
    'payroll - officer wages'       : 'Payroll Officer Wages',
    'payroll - officer salary'      : 'Payroll Officer Salary',
    'payroll - cash draw'           : 'Payroll Cash Draw',
    'payroll - other'               : 'Payroll Other',
    'taxes - payroll'               : 'Payroll Taxes',

    'over - lottery'                : 'Over/Short Lottery',
    'over - bar'                    : 'Over/Short Bar',
    'over - cash till'              : 'Over/Short Cash Till',

    'art sales'                     : 'Art Sales',

    'rent'                          : 'Rent',

    'utilities'                     : 'Utilities',
    'entertainment'                 : 'Entertainment',
    'party page'                    : 'Party Page',

    'refunds'                       : 'Refunds',

    'contract labor'                : 'Contract Labor',
    'consulting'                    : 'Consulting',
    'outside services'              : 'Outside Services',
    'professional - other'          : 'Professional Other',

    'vendor reporting paid'         : 'Vendor Reporting',
    'ask my accountant'             : 'Ask My Accountant',

    'security'                      : 'Security',

    'maintenance'                   : 'Maintenance',
    'cleaning'                      : 'Cleaning',
    'janitorial'                    : 'Janitorial',
    'repairs'                       : 'Repairs',

    'advertising'                   : 'Advertising',
    'telephone'                     : 'Telephone',
    'lottery fees'                  : 'Lottery Fees',
    'postage'                       : 'Postage',

    'restaurant operating supplies' : 'Restaurant Operating Supplies',
    'equipment rental'              : 'Equipment Rental',
    'electronic equipment'          : 'Electronic Equipment',
    'dishes and furniture'          : 'Dishes and Furniture',
    'office supplies'               : 'Office Supplies',
    'restaurant supplies'           : 'Restaurant Supplies',

    'parking'                       : 'Parking',
    'automobile'                    : 'Automobile',
    'travel'                        : 'Travel',

    'merchant account fees'         : 'Merchant Account Fees',
    'accounting'                    : 'Accounting',
    'bank services'                 : 'Bank Services',
    'depreciation'                  : 'Depreciation',
    'voided check'                  : 'Voided Check',

    'art commissions'               : 'Art Commissions',
    'art payments'                  : 'Art Payments',
    'decorations'                   : 'Decorations',

    'uber'                          : 'Uber',
    'meals'                         : 'Meals',
    'charitable'                    : 'Charitable',
    'discrepancies'                 : 'Discrepancies',

    'workmans comp'                 : 'Workmans Comp',
    'insurance other'               : 'Insurance Other',
    'licenses'                      : 'Licenses',

    'interest'                      : 'Interest',

    'gross'                         : 'Gross',
    'costs'                         : 'Costs',
    'net'                           : 'Net',
}


def fill_model_sheet(model, sheet):
    start_date = date(2020, 10, 1)
    end_date   = date(2022, 12, 1)

    months = list(months_in_range(start_date, end_date))

    def set_cell(col, row, value):
        nonlocal sheet

        cell = '{}{}'.format(col, row)
        sheet[cell] = value


    row = 1

    for c, m in zip(excel_columns[1:], months):
        set_cell(c, row, fmt_month(m))

    for key, name in line_items.items():
        set_cell('A', row, name)

        wrote_row = False

        for col, m in zip(excel_columns[1:], months):
            values = values_by_month_model(m, model)

            if key not in values:
                break

            wrote_row = True

            v = values[key]
            set_cell(col, row, fmt_value(v))

        if wrote_row:
            row += 1 


def fill_profit_loss(sheet):
    records = fixed_profit_loss_records(DATA / 'profit_loss.psv')

    def set_cell(col, row, value):
        nonlocal sheet

        cell = '{}{}'.format(col, row)
        sheet[cell] = value

    start_date = date(2019, 1, 1)
    end_date   = date(2019, 12, 1)
    months = list(months_in_range(start_date, end_date))

    by_item = defaultdict(dict)
    for (k, m), v in records.items():
        by_item[k][m] = v


    row = 1
    set_cell('A', row, 'Month')

    for c, m in zip(excel_columns[1:], months):
        set_cell(c, row, fmt_value(m))

    row += 1

    for key, name in line_items.items():
        values_by_month = by_item.get(key)

        if not values_by_month:
            continue

        set_cell('A', row, name)

        for c, m in zip(excel_columns[1:], months):
            v = by_item[key][m]
            set_cell(c, row, fmt_value(v))

        row += 1


@invoke.task
def make_projection_spreadsheet(context):
    from openpyxl import Workbook

    book = Workbook()

    sheet = book.active
    fill_profit_loss(sheet)
    sheet.title = '2019 Profit and Loss'

    for model in parse_models():
        sheet = book.create_sheet(model)
        fill_model_sheet(model, sheet)

    book.save(filename='local_lounge_projections.xlsx')


@invoke.task
def print_delta(context):
    actual = { k for (k, _) in fixed_profit_loss_records(DATA / 'profit_loss.psv') }
    done = line_items.keys()

    pprint(actual - done)
