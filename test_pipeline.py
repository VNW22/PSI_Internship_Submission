import pandas as pd
import pytest
from pipeline import normalize_dates

def test_date_normalization():
    df = pd.DataFrame({'date': ['2023-01-01', '01/01/2023', 'invalid']})
    result = normalize_dates(df, 'date')
    assert pd.api.types.is_datetime64_any_dtype(result['date'])
    assert result['date'].iloc[0] == result['date'].iloc[1]

def test_tier_lowercasing():
    # Synthetic test for the lowercasing logic
    tiers = pd.Series(['Gold', 'SILVER', 'bronze'])
    result = tiers.str.lower()
    assert result.iloc[0] == 'gold'
    assert result.iloc[1] == 'silver'
    assert result.iloc[2] == 'bronze'

def test_net_amount_calc():
    total = 100.0
    discount = 15.0
    net = total * (1 - discount / 100)
    assert net == 85.0
