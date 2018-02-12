import argparse
import datetime as dt
from pathlib import Path

import bs4 as bs
import pandas as pd
import pyprind
import requests

BASE_URL = 'https://otctransparency.finra.org'

CATEGORY = 'category'
DOWNLOAD = 'Download'
LAST_UPDATED = 'Last Updated'
REPORT_TYPE = 'Report Type'
WEEK = 'Week'
YEAR = 'Year'


def initialize_session():
    session = requests.Session()
    session.get(BASE_URL + '/Agreement/Submit')

    return session


def read_grid_frame(session):
    grid_dfs = []
    for cat in ['ATS', 'OTC']:
        grid_result = session.get(grid_url(cat))
        dfs = pd.read_html(io=grid_result.text)
        assert len(dfs) == 1
        df = dfs[0]
        df.columns = [c.strip() for c in df.columns]
        df[CATEGORY] = cat
        for c in [WEEK, LAST_UPDATED]:
            df[c] = pd.to_datetime(df[c])

        sp = bs.BeautifulSoup(grid_result.text, 'lxml')
        tb = sp.find_all('table')[0]
        df[DOWNLOAD] = [tag.get('href') for tag in tb.find_all('a')]

        grid_dfs.append(df)

    return pd.concat(grid_dfs)


def grid_url(category):
    prefix = 'OTC' if category == 'OTC' else ''
    tdf = 'TradingDetailFile'
    rel_path = '/{0}{1}Archive/{0}{1}sDownloadGrid'.format(prefix, tdf)
    return BASE_URL + rel_path


def get_parser():
    p = argparse.ArgumentParser()

    p.add_argument(
        'earliest_updated_date',
        type=lambda v: dt.datetime.strptime(v, '%Y-%m-%d'),
        help='Earliest last updated'
    )

    return p


def main(parser_args=None):
    parser = get_parser()
    args = parser.parse_args(args=parser_args)
    earliest_updated_dt = args.earliest_updated_date

    session = initialize_session()

    grid_df = read_grid_frame(session)

    last_updated_mask = grid_df[LAST_UPDATED] >= earliest_updated_dt

    iterations = last_updated_mask.sum()
    pbar = pyprind.ProgBar(
        iterations=iterations, title='OTC Data ({} items)'.format(iterations)
    )
    file_fmt = '{:%Y-%m-%d}_{}_{}_{:%Y-%m-%d %H:%M:%S}.csv'
    base_fmt = 'data/{}'
    for _, srs in grid_df.loc[last_updated_mask].iterrows():
        base_path = Path(base_fmt.format(srs[YEAR]))
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=True)
        file_path = base_path.joinpath(file_fmt.format(
            srs[WEEK],
            srs[CATEGORY],
            srs[REPORT_TYPE],
            srs[LAST_UPDATED])
        )
        if not file_path.exists():
            download_result = session.get(BASE_URL + srs[DOWNLOAD])
            with open(file_path, 'w') as fp:
                fp.write(download_result.text)
        pbar.update(item_id=file_path)


if __name__ == '__main__':
    main()

