from faessentials.asset_formatter import AssetFormatter, BinanceFormatter, BybitFormatter 

# Test unwrap_symbol
def test_unwrap_symbol():
    formatter = AssetFormatter()
    assert formatter.unwrap_symbol('WETH') == 'ETH'
    assert formatter.unwrap_symbol('BTC') == 'BTC'  # Non-existent in symbol_pairs

# Test unwrap_market
def test_unwrap_market():
    formatter = AssetFormatter()
    assert formatter.unwrap_market('WETH/USDT') == 'ETH/USDT'
    assert formatter.unwrap_market('BTC-USDT') == 'BTC-USDT'

# Test clean_markets
def test_clean_markets():
    formatter = AssetFormatter()
    markets = {
        'BTC/USDT', 'ETH-USD', 'INVALIDMARKET', 'TOOLONGMARKETNAME1234'
    }
    expected = ['BTC/USDT', 'ETH-USD']  # Use a list instead of a set
    actual = formatter.clean_markets(markets)
    
    assert sorted(actual) == sorted(expected)  # Compare sorted lists


# Test get_base and get_quote
def test_get_base_and_quote():
    formatter = AssetFormatter()
    assert formatter.get_base('BTC/USDT') == 'BTC'
    assert formatter.get_quote('BTC/USDT') == 'USDT'
    # Add more tests for different formats and edge cases

# Test format_pair_default
def test_format_pair_default():
    formatter = AssetFormatter()
    assert formatter.format_pair_default('BTCUSDT') == 'BTC/USDT'
    # Add tests for different formats and edge cases

# Test format_to_dash and format_to_slash
def test_format_to_dash_and_slash():
    formatter = AssetFormatter()
    assert formatter.format_to_dash('BTC/USDT') == 'BTC-USDT'
    assert formatter.format_to_slash('BTC-USDT') == 'BTC/USDT'

# Test format_set_to_dash and format_set_to_slash
def test_format_set_to_dash_and_slash():
    formatter = AssetFormatter()
    markets = {'BTC/USDT', 'ETH/USDT'}
    assert formatter.format_set_to_dash(markets) == {'BTC-USDT', 'ETH-USDT'}
    assert formatter.format_set_to_slash(markets) == {'BTC/USDT', 'ETH/USDT'}

# Tests for BinanceFormatter and BybitFormatter
def test_binance_formatter():
    formatter = BinanceFormatter()
    assert formatter.format_pair('BTC/USDT') == 'BTC-USDT'

def test_bybit_formatter():
    formatter = BybitFormatter()
    assert formatter.format_pair('BTC/USDT') == 'BTC_USDT'

