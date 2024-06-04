import re
from typing import Set
from faessentials import global_logger


class AssetFormatter:
    def __init__(self, default_quote_asset: str = "USDT") -> None:
        self.default_quote_asset = default_quote_asset
        self.logger = global_logger.setup_custom_logger("app")
        self.symbol_pairs = {"WETH": "ETH"}

    def unwrap_symbol(self, wrapped_symbol):
        # Convert the input to uppercase to make the function case-insensitive
        wrapped_symbol_upper = wrapped_symbol.upper()
        # Return the unwrapped symbol, or the original symbol if not found
        return self.symbol_pairs.get(wrapped_symbol_upper, wrapped_symbol)

    def get_wrapped_symbol(self, original_symbol) -> str:
        """Returns the wrapped symbol for an asset. Or None"""
        for key, val in self.symbol_pairs.items():
            if val == original_symbol:
                return key
        return None

    def unwrap_market(self, market):
        # Extract the base and quote assets from the market string
        base_asset = self.get_base(market)
        quote_asset = self.get_quote(market)
        unwrapped_base = self.unwrap_symbol(base_asset)
        delimiter = "/"
        if "-" in market:
            delimiter = "-"

        # Combine the unwrapped base asset with the quote asset
        return f"{unwrapped_base}{delimiter}{quote_asset}"

    def clean_markets(self, markets: dict) -> dict:
        """
        Removes all non-compliant markets, those exceeding 12 characters in length,
        and those not containing a '-' or '/'.
        """
        allowed_chars_pattern = re.compile(r"^[a-zA-Z0-9/-]{1,12}$")
        must_have_chars = {"-", "/"}

        cleaned_markets = []

        for market in markets:
            if allowed_chars_pattern.match(market) and any(
                char in market for char in must_have_chars
            ):
                cleaned_markets.append(market)
            else:
                self.logger.info(
                    f"Invalid market format, length, or missing required characters found and removed: {market}"
                )
        self.logger.debug(f"Cleaned markets: {cleaned_markets}")
        return cleaned_markets

    def get_base(self, pair_string: str) -> str:
        """Returns the base asset. From SOL/USDT that is SOL."""
        match = re.match(r"([A-Za-z]+)[/-]", pair_string)
        if match:
            base_asset = match.group(1)
            return base_asset
        else:
            raise ValueError("Invalid pair string format")

    def get_quote(self, pair_string: str) -> str:
        """Returns the quote asset. From SOL/USDT that is USDT."""
        match = re.match(r"[A-Za-z]+[/-]([A-Za-z]+)", pair_string)
        if match:
            quote_asset = match.group(1)
            return quote_asset
        else:
            # Fall back to default quote asset if not found
            return self.default_quote_asset

    def format_pair(self, pair_string: str) -> str:
        raise NotImplementedError("This method should be implemented by subclasses.")

    def format_set_of_pairs(self, pairs: Set[str]) -> Set[str]:
        return {self.format_pair(pair) for pair in pairs}

    def format_pair_default(self, pair_string: str) -> str:
        """Will format the pair string to a slash-format. Mainly used for the ccxt LIB."""
        # Regex pattern to match 'BTCUSDT', 'BTC/USDT', and 'BTC-USDT'
        pattern = r"([A-Za-z]+)[/-]?(USDT)$"
        match = re.match(pattern, pair_string)

        if match:
            base_asset, quote_asset = match.groups()
            formatted_pair = f"{base_asset}/{quote_asset}"
            return formatted_pair
        else:
            self.logger.error(f"Invalid pair format: {pair_string}")
            raise ValueError(f"Invalid pair format: {pair_string}")

    def format_to_dash(self, pair_string: str) -> str:
        """Takes any combination of a market pair and replaces the divider by a dash (-)."""
        match = re.match(r"([A-Za-z0-9]+)[/-]?([A-Za-z0-9]+)", pair_string)        
        if match is None:
            raise ValueError(f"Invalid market pair format: '{pair_string}'")
        base_asset, _ = match.groups()
        return f"{base_asset}-{self.default_quote_asset}"

    def format_to_slash(self, pair_string: str) -> str:
        """Takes any combinatin of a market pair and replaces the divider by a front slash (/)."""
        match = re.match(r"([A-Za-z0-9]+)[/-]?([A-Za-z0-9]+)", pair_string)        
        if match is None:
            raise ValueError(f"Invalid market pair format: '{pair_string}'")
        base_asset, _ = match.groups()
        return f"{base_asset}/{self.default_quote_asset}"

    def format_set_to_dash(self, markets: Set[str]) -> Set[str]:
        """Takes a set of market pairs and formats each to dash notation."""
        return {self.format_to_dash(pair) for pair in markets}

    def format_set_to_slash(self, markets: Set[str]) -> Set[str]:
        """Takes a set of market pairs and formats each to front slash notation."""
        return {self.format_to_slash(pair) for pair in markets}


class BinanceFormatter(AssetFormatter):
    def format_pair(self, pair_string: str) -> str:
        match = re.match(r"([A-Za-z]+)[/-]?([A-Za-z]+)", pair_string)
        base_asset, _ = match.groups()
        return f"{base_asset}-{self.default_quote_asset}"


class BybitFormatter(AssetFormatter):
    def format_pair(self, pair_string: str) -> str:
        match = re.match(r"([A-Za-z]+)[/-]?([A-Za-z]+)", pair_string)
        base_asset, _ = match.groups()
        # Assuming Bybit format is slightly different, for example
        return f"{base_asset}_{self.default_quote_asset}"
