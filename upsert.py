from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyprind
import sqlalchemy as sa
import pandas as pd

from sql import OTC_DATA, FINRA, CATEGORY, WEEK, REPORT_TYPE, SYMBOL, \
    VENUE_DESCRIPTION


def year_string(v):
    try:
        int(v)
        return True
    except ValueError:
        return False


def parse_filename(path):
    return tuple(path.name.split('_'))


def read_frame(path):
    df = pd.read_csv(
        filepath_or_buffer=path,
        sep='|',
        engine='python',
        skipfooter=1,
        parse_dates=[
            'Shares_Last_Updated',
            'Trades_Last_Updated'
        ]
    )
    week_str, category, _, _ = parse_filename(path)
    df[WEEK] = pd.Timestamp(week_str)
    df[CATEGORY] = category

    df.rename(
        columns={
            'ATS_Description': VENUE_DESCRIPTION,
            'OTC_Non-ATS_Description': VENUE_DESCRIPTION,
        },
        inplace=True
    )

    index = [WEEK, CATEGORY, REPORT_TYPE, SYMBOL, VENUE_DESCRIPTION]
    df.set_index(index, inplace=True)

    return df


def count_files(path):
    return len(list(iterdata(path)))


def iterdata(path):
    for d in path.iterdir():
        if year_string(d.name):
            for f in d.iterdir():
                yield f


def insert_frame(path):
    engine = sa.create_engine('mysql+pymysql://rheineke:password@localhost')
    df = read_frame(path)
    df.to_sql(name=OTC_DATA, con=engine, schema=FINRA, if_exists='append')


if __name__ == '__main__':
    data_path = Path('data')

    iterations = count_files(data_path)
    pbar = pyprind.ProgBar(
        iterations=iterations, title='{} files'.format(iterations)
    )

    futures = []
    with ProcessPoolExecutor() as executor:
        for path in iterdata(data_path):
            future = executor.submit(insert_frame, path)
            futures.append(future)

        for future in as_completed(futures):
            pbar.update()

    num_exceptions = 0
    for future in futures:
        if future.exception() is not None:
            num_exceptions += 1