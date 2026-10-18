import sqlalchemy as sa

FINRA = "finra"
OTC_DATA = "otc_data"

CATEGORY = "Category"
REPORT_TYPE = "Report_Type"
SYMBOL = "Symbol"
WEEK = "Week"
VENUE_DESCRIPTION = "Venue_Description"


def finra_otc_table(metadata):
    return sa.Table(
        OTC_DATA,
        metadata,
        sa.Column(WEEK, sa.DATE, primary_key=True),
        sa.Column(CATEGORY, sa.CHAR(3), primary_key=True),
        sa.Column(REPORT_TYPE, sa.VARCHAR(10), primary_key=True),
        sa.Column(SYMBOL, sa.VARCHAR(10), primary_key=True),
        sa.Column(VENUE_DESCRIPTION, sa.VARCHAR(55), primary_key=True),
        sa.Column("Issue_Description", sa.VARCHAR(201)),
        sa.Column("ATS_MPID", sa.VARCHAR(4)),
        sa.Column("Shares", sa.BIGINT),
        sa.Column("Shares_Last_Updated", sa.DATETIME),
        sa.Column("Trades", sa.BIGINT),
        sa.Column("Trades_Last_Updated", sa.DATETIME),
    )


def main():
    engine = sa.create_engine("mysql+pymysql://rheineke:password@localhost")

    with engine.begin() as conn:
        conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS " + FINRA))

    md = sa.MetaData(schema=FINRA)
    finra_otc_table(md)
    md.drop_all(engine)
    md.create_all(engine)


if __name__ == "__main__":
    main()
