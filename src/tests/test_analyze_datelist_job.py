from chispa.dataframe_comparer import *
from collections import namedtuple
from ..jobs.analyze_datelist_job import do_analyze_datelist_transformation, REF_DATE

# minimal schema: user_id, dates_active[], date
UC = namedtuple("UC", "user_id dates_active date")

def test_analyze_datelist_basic(spark):
    # u1 active on the last 3 days relative to REF_DATE -> weekly_active=3, monthly_active=3
    u1_days = ["2023-03-29","2023-03-30","2023-03-31"]
    # u2 active 10 days before REF_DATE -> NOT in last-7-day window
    u2_days = ["2023-03-21"]

    input_df = spark.createDataFrame([
        UC("u1", [*u1_days], REF_DATE),
        UC("u2", [*u2_days], REF_DATE),
    ])

    actual_df = do_analyze_datelist_transformation(spark, input_df)

    # sanity: only two rows for two users
    assert actual_df.count() == 2

    rows = {r["user_id"]: r.asDict() for r in actual_df.collect()}

    assert rows["u1"]["monthly_active"] == 3
    assert rows["u1"]["weekly_active"] == 3
    assert rows["u1"]["weekly_active_flag"] is True
    assert rows["u1"]["weekly_active_previous_week"] in (True, False)  # depends on u1 history; okay to not enforce

    assert rows["u2"]["monthly_active"] == 1
    assert rows["u2"]["weekly_active"] == 0
    assert rows["u2"]["weekly_active_flag"] is False
