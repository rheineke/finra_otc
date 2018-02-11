import bs4 as bs
import pandas as pd
import requests

BASE_URL = 'https://otctransparency.finra.org'

CATEGORY = 'category'
DOWNLOAD = 'Download'
LAST_UPDATED = 'Last Updated'
REPORT_TYPE = 'Report Type'
WEEK = 'Week'


def grid_url(category):
    prefix = 'OTC' if category == 'OTC' else ''
    tdf = 'TradingDetailFile'
    rel_path = '/{0}{1}Archive/{0}{1}sDownloadGrid'.format(prefix, tdf)
    return BASE_URL + rel_path


if __name__ == '__main__':
    s = requests.Session()
    submit_url = BASE_URL + '/Agreement/Submit'
    r = s.get(submit_url)

    grid_dfs = []
    for cat in ['ATS', 'OTC']:
        grid_result = s.get(grid_url(cat))
        dfs = pd.read_html(io=grid_result.text)
        grid_df = dfs[0]
        grid_df.columns = [c.strip() for c in grid_df.columns]
        grid_df[CATEGORY] = cat
        for c in [WEEK, LAST_UPDATED]:
            grid_df[c] = pd.to_datetime(grid_df[c])

        sp = bs.BeautifulSoup(grid_result.text, 'lxml')
        tb = sp.find_all('table')[0]
        grid_df[DOWNLOAD] = [tag.get('href') for tag in tb.find_all('a')]

        grid_df = grid_df.set_index([CATEGORY, WEEK, REPORT_TYPE])

        grid_dfs.append(grid_df)
