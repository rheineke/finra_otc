from collections import defaultdict
from pathlib import Path

import pyprind
import sqlalchemy as sa
import pandas as pd

from retrieve import CATEGORY, WEEK


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

    return df


def read_frames(path):
    iterator = iterdata(path)
    for p in iterator:
        yield read_frame(p)


def count_files(path):
    return len(list(iterdata(path)))


def iterdata(path):
    for d in path.iterdir():
        if year_string(d.name):
            for f in d.iterdir():
                yield f


if __name__ == '__main__':
    data_path = Path('data')

    iterations = count_files(data_path)
    pbar = pyprind.ProgBar(
        iterations=iterations, title='{} files'.format(iterations)
    )

    cols = [
        'Report_Type',
        'Symbol',
        'Issue_Description',
        'ATS_Description',
        'ATS_MPID',
        'OTC_Non-ATS_Description'
    ]
    max_len = defaultdict(int)
    iterator = read_frames(data_path)
    for df in iterator:
        for c in cols:
            if c in df.columns:
                max_len[c] = max(max_len[c], max(df[c].apply(len)))
        pbar.update()
    # sa.create_engine('postgresql://scott:tiger@localhost/test')
