import pandas as pd
import numpy as np

if __name__ == "__main__":
    print "### Loading CSV..."
    df = pd.read_csv("../data/train.csv")

    print "### Analyse de TRIP_ID..."
    print df["TRIP_ID"].describe()
    print df["TRIP_ID"].value_counts()
    # count    1.710670e+06
    # mean     1.388622e+18
    # std      9.180944e+15
    # min      1.372637e+18
    # 25%      1.380731e+18
    # 50%      1.388493e+18
    # 75%      1.396750e+18
    # max      1.404173e+18
    # Name: TRIP_ID, dtype: float64

    print "### Analyse de CALL_TYPE..."
    print df["CALL_TYPE"].describe()
    print df["CALL_TYPE"].value_counts()
    # count     1710670
    # unique          3
    # top             B
    # freq       817881
    # Name: CALL_TYPE, dtype: object
    # B    817881
    # C    528019
    # A    364770
    # Name: CALL_TYPE, dtype: int64

    print "### Analyse de ORIGIN_CALL..."
    print df["ORIGIN_CALL"].describe()
    print df["ORIGIN_CALL"].value_counts()
    # count    364770.000000
    # mean      24490.363018
    # std       19624.290043
    # min        2001.000000
    # 25%        6593.000000
    # 50%       18755.000000
    # 75%       40808.000000
    # max       63884.000000
    # Name: ORIGIN_CALL, dtype: float64

    print "### Analyse de ORIGIN_STAND..."
    print df["ORIGIN_STAND"].describe()
    print df["ORIGIN_STAND"].value_counts()
    # count    806579.000000
    # mean         30.272381
    # std          17.747840
    # min           1.000000
    # 25%          15.000000
    # 50%          27.000000
    # 75%          49.000000
    # max          63.000000
    # Name: ORIGIN_STAND, dtype: float64

    print "### Analyse de TAXI_ID..."
    print df["TAXI_ID"].describe()
    print df["TAXI_ID"].value_counts()
    # count    1.710670e+06
    # mean     2.000035e+07
    # std      2.112405e+02
    # min      2.000000e+07
    # 25%      2.000017e+07
    # 50%      2.000034e+07
    # 75%      2.000052e+07
    # max      2.000098e+07
    # Name: TAXI_ID, dtype: float64

    print "### Analyse de DAY_TYPE..."
    print df["DAY_TYPE"].describe()
    print df["DAY_TYPE"].value_counts()
    # count     1710670
    # unique          1
    # top             A
    # freq      1710670
    # Name: DAY_TYPE, dtype: object
    # A    1710670
    # Name: DAY_TYPE, dtype: int64

    print "### Analyse de MISSING_DATA..."
    print df["MISSING_DATA"].describe()
    print df["MISSING_DATA"].value_counts()
    # count     1710670
    # unique          2
    # top         False
    # freq      1710660
    # Name: MISSING_DATA, dtype: object
    # False    1710660
    # True          10
    # Name: MISSING_DATA, dtype: int64

    # print "Analyse de POLYLINE..."
    # print df["POLYLINE"].describe()
    # print df["POLYLINE"].value_counts()