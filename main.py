import logging
import time

from collections import deque
from optibook import ORDER_TYPE_IOC, SIDE_ASK, SIDE_BID
from optibook.synchronous_client import Exchange
from helper import is_up,clear_all_stock

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Constants ---
ETF_EU_ID = 'SEMIS_ETF_EU'
ETF_US_ID = 'SEMIS_ETF_US'
ASML_ID = 'ASML'
AMD_ID = 'AMD'
NVDA_ID = 'NVDA'

THRESHOLD = 0.05
BASKET_THRESHOLD = 10

MAX_POSITION = 750
INSTRUMENT_IDS = ['SEMIS_ETF_US', 'SEMIS_ETF_EU', 'NVDA', 'AMD', 'ASML']

GRID_THRESHOLD_DEFAULT = 0.2
def get_dynamic_grid_threshold(e):
    """
    Return a dynamic grid threshold.
    If the combined basket position is near its limit (e.g. > 80% of 300),
    then the threshold is halved to clean up positions faster.
    """
    pos = e.get_positions()
    basket = (pos.get(ETF_US_ID, 0) + pos.get(ETF_EU_ID, 0) +
              pos.get(ASML_ID, 0) + pos.get(AMD_ID, 0) + pos.get(NVDA_ID, 0))
    if abs(basket) > 0.8 * 300:  # i.e. if basket exceeds 240 or is below -240
        return GRID_THRESHOLD / 2  # more aggressive grid: 0.1 instead of 0.2
    return GRID_THRESHOLD

GRID_THRESHOLD = GRID_THRESHOLD_DEFAULT  # This may be dynamically lowered.
BASKET_LOWER_LIMIT = -300
BASKET_UPPER_LIMIT = 300

position_queues = {''
    ETF_US_ID: {'long': deque(), 'short': deque()},
    ETF_EU_ID: {'long': deque(), 'short': deque()},
    ASML_ID: {'long': deque(), 'short': deque()},
    AMD_ID: {'long': deque(), 'short': deque()},
    NVDA_ID: {'long': deque(), 'short': deque()}
}

# Global counters (optional) for tracking trade events.
arbitrage_counter = 0
reverse_arbitrage_counter = 0
basket_violation_start_time = None

def check_and_correct_basket_limit(e):
    """
    Enforce the basket position limit.
    The basket (combined position of ETF_US, ETF_EU, ASML, AMD, NVDA)
    must lie between -300 and 300. If this is breached for more than 3 seconds,
    issue corrective orders (distributed evenly over the 5 instruments).
    """
    global basket_violation_start_time
    pos = e.get_positions()
    basket = (pos.get(ETF_US_ID, 0) + pos.get(ETF_EU_ID, 0) +
              pos.get(ASML_ID, 0) + pos.get(AMD_ID, 0) + pos.get(NVDA_ID, 0))
    
    # If within limits, reset the violation timer.
    if -250 <= basket <= 250:
        basket_violation_start_time = None
        return
    
    # Otherwise, mark the first breach time if not already marked.
    if basket_violation_start_time is None:
        basket_violation_start_time = time.time()
    elif time.time() - basket_violation_start_time > 2.5:
        # We have been out-of-bound for more than 3 seconds: force correction.
        if basket > 300:
            excess = basket - 250
            # Distribute corrective sell orders evenly over the 5 instruments.
            trade_volume = excess // 5 + 1  # round up
            for instrument in INSTRUMENT_IDS:
                current = pos.get(instrument, 0)
                if current > 0:
                    volume_to_sell = min(current, trade_volume)
                    pb = e.get_last_price_book(instrument)
                    if pb and pb.bids:
                        sell_price = pb.bids[0].price
                        e.insert_order(instrument, price=sell_price, volume=volume_to_sell,
                                       side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
                        logger.info(f"[Basket Correction] Forced sell of {volume_to_sell} {instrument} at {sell_price}")
        elif basket < -300:
            excess = -250 - basket
            trade_volume = excess // 5 + 1
            for instrument in INSTRUMENT_IDS:
                current = pos.get(instrument, 0)
                if current < 0:
                    pb = e.get_last_price_book(instrument)
                    if pb and pb.asks:
                        buy_price = pb.asks[0].price
                        volume_to_buy = min(abs(current), trade_volume)
                        e.insert_order(instrument, price=buy_price, volume=volume_to_buy,
                                       side=SIDE_BID, order_type=ORDER_TYPE_IOC)
                        logger.info(f"[Basket Correction] Forced buy of {volume_to_buy} {instrument} at {buy_price}")
        # After issuing corrective trades, reset the violation timer.
        basket_violation_start_time = None

def record_trade(instrument: str, side: str, price: float, volume: int):
    """
    Record an executed trade for auto-calibration.
    
    For a BUY order (SIDE_BID): treat as a long entry.
    For a SELL order (SIDE_ASK): treat as a short entry.
    
    If there are existing positions on the opposite side, offset them first (FIFO).
    """
    if side == SIDE_BID:
        # A buy order offsets any existing short entries first.
        while volume > 0 and position_queues[instrument]['short']:
            entry_price, entry_vol = position_queues[instrument]['short'][0]
            if entry_vol <= volume:
                volume -= entry_vol
                position_queues[instrument]['short'].popleft()
            else:
                position_queues[instrument]['short'][0] = (entry_price, entry_vol - volume)
                volume = 0
        if volume > 0:
            # Any remaining volume is recorded as a long position.
            position_queues[instrument]['long'].append((price, volume))
    elif side == SIDE_ASK:
        # A sell order offsets any existing long entries first.
        while volume > 0 and position_queues[instrument]['long']:
            entry_price, entry_vol = position_queues[instrument]['long'][0]
            if entry_vol <= volume:
                volume -= entry_vol
                position_queues[instrument]['long'].popleft()
            else:
                position_queues[instrument]['long'][0] = (entry_price, entry_vol - volume)
                volume = 0
        if volume > 0:
            # Any remaining volume is recorded as a short position.
            position_queues[instrument]['short'].append((price, volume))

def auto_calibrate_positions(e):
    for instrument, queues in position_queues.items():
        pb = e.get_last_price_book(instrument)
        if not pb:
            continue
        dyn_threshold = get_dynamic_grid_threshold(e)
        # For long entries:
        if pb.bids:
            current_bid = pb.bids[0].price
            while queues['long']:
                entry_price, volume = queues['long'][0]
                if current_bid > (entry_price + dyn_threshold):
                    e.insert_order(instrument, price=current_bid, volume=volume,
                                   side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
                    logger.info(f"[Auto Calibrate] Sold {volume} of {instrument} at {current_bid} (entry {entry_price})")
                    queues['long'].popleft()
                else:
                    break
        # For short entries:
        if pb.asks:
            current_ask = pb.asks[0].price
            while queues['short']:
                entry_price, volume = queues['short'][0]
                if current_ask < (entry_price - dyn_threshold):
                    e.insert_order(instrument, price=current_ask, volume=volume,
                                   side=SIDE_BID, order_type=ORDER_TYPE_IOC)
                    logger.info(f"[Auto Calibrate] Bought {volume} of {instrument} at {current_ask} (entry {entry_price})")
                    queues['short'].popleft()
                else:
                    break


def risk_allowed(e: Exchange, instrument: str, side: str) -> int:
    """
    Returns the maximum volume allowed by risk management for a given instrument and order side.
    
    For a BUY (SIDE_BID) order:
        Allowed additional volume = MAX_POSITION - current_position
    For a SELL (SIDE_ASK) order:
        Allowed volume = MAX_POSITION + current_position
        (This works even if current_position is negative; for example,
         if current_position is -200, then you may sell up to 750 + (-200) = 550 more
         so that your resulting position is -750.)
    
    If the allowed volume is <= 0, then no trade should be executed.
    """
    pos = e.get_positions().get(instrument, 0)
    if side == SIDE_BID:  # Buying increases position.
        allowed = MAX_POSITION - pos
    elif side == SIDE_ASK:  # Selling decreases position (or increases short).
        allowed = MAX_POSITION + pos
    else:
        allowed = 0
    return max(allowed, 0)


def arbitrage_strategy(e: Exchange, threshold=THRESHOLD):
    """
    Normal arbitrage between ETFs:
    If US ETF bid > (EU ETF ask + threshold), then execute:
      - Sell US ETF at its bid price (IOC)
      - Buy EU ETF at its ask price (IOC)
    """
    global arbitrage_counter
    etf_us = e.get_last_price_book(ETF_US_ID)
    etf_eu = e.get_last_price_book(ETF_EU_ID)

    # Check that both price books are valid and contain orders.
    if not (is_up(etf_us) and is_up(etf_eu)):
        return
    if not (etf_us.bids and etf_eu.asks):
        return

    bid_price_us = etf_us.bids[0].price
    ask_price_eu = etf_eu.asks[0].price
    if bid_price_us > (ask_price_eu + threshold):
        # Determine the maximum volume available for arbitrage.
        order_volume = min(etf_us.bids[0].volume, etf_eu.asks[0].volume)
        allowed_sell_us = risk_allowed(e, ETF_US_ID, SIDE_ASK)  # For selling US ETF.
        allowed_buy_eu = risk_allowed(e, ETF_EU_ID, SIDE_BID)     # For buying EU ETF.
        volume = min(order_volume, allowed_sell_us, allowed_buy_eu)

        # Execute orders:
        if volume > 0:
            e.insert_order(ETF_US_ID, price=bid_price_us, volume=volume,
                        side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
            e.insert_order(ETF_EU_ID, price=ask_price_eu, volume=volume,
                        side=SIDE_BID, order_type=ORDER_TYPE_IOC)
            
            arbitrage_counter += 1
            record_trade(ETF_US_ID, SIDE_ASK, bid_price_us, volume)
            record_trade(ETF_EU_ID, SIDE_BID, ask_price_eu, volume)
            logger.info(f"[Arbitrage #{arbitrage_counter}] Sold {volume} of {ETF_US_ID} at {bid_price_us} and "
                        f"bought {volume} of {ETF_EU_ID} at {ask_price_eu}")
        else:
            logger.debug("Arbitrage strategy: risk limits prevent trade execution.")

def reverse_arbitrage_strategy(e: Exchange, threshold=THRESHOLD):
    """
    Reverse arbitrage between ETFs:
    If EU ETF bid > (US ETF ask + threshold), then execute:
      - Sell EU ETF at its bid price (IOC)
      - Buy US ETF at its ask price (IOC)
    """
    global reverse_arbitrage_counter
    etf_us = e.get_last_price_book(ETF_US_ID)
    etf_eu = e.get_last_price_book(ETF_EU_ID)

    if not (is_up(etf_us) and is_up(etf_eu)):
        return
    if not (etf_eu.bids and etf_us.asks):
        return

    bid_price_eu = etf_eu.bids[0].price
    ask_price_us = etf_us.asks[0].price

    if bid_price_eu > (ask_price_us + threshold):
        order_volume  = min(etf_eu.bids[0].volume, etf_us.asks[0].volume)
        allowed_sell_eu = risk_allowed(e, ETF_EU_ID, SIDE_ASK)  # For selling EU ETF.
        allowed_buy_us = risk_allowed(e, ETF_US_ID, SIDE_BID)     # For buying US ETF.
        volume = min(order_volume, allowed_sell_eu, allowed_buy_us)
        
        if volume > 0:
            e.insert_order(ETF_EU_ID, price=bid_price_eu, volume=volume,
                        side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
            e.insert_order(ETF_US_ID, price=ask_price_us, volume=volume,
                        side=SIDE_BID, order_type=ORDER_TYPE_IOC)
            reverse_arbitrage_counter += 1
            record_trade(ETF_EU_ID, SIDE_ASK, bid_price_eu, volume)
            record_trade(ETF_US_ID, SIDE_BID, ask_price_us, volume)
            logger.info(f"[Reverse Arbitrage #{reverse_arbitrage_counter}] Sold {volume} of {ETF_EU_ID} at {bid_price_eu} and "
                        f"bought {volume} of {ETF_US_ID} at {ask_price_us}")
        else:
            logger.debug("Reverse arbitrage strategy: risk limits prevent trade execution.")

# --- Global Variables for Last Known Underlying Prices ---
last_asml_bid = 0.0
last_amd_bid  = 0.0
last_nvda_bid = 0.0

last_asml_ask = 0.0
last_amd_ask = 0.0
last_nvda_ask = 0.0

# --- Update Underlying Prices ---
def update_underlying_prices(e):
    """
    Update global variables with the latest available prices for ASML, AMD, and NVDA.
    If the current price book is missing bid data, the previous value is retained.
    """
    global last_asml_bid, last_amd_bid, last_nvda_bid, last_asml_ask, last_amd_ask, last_nvda_ask
    asml_pb = e.get_last_price_book(ASML_ID)
    amd_pb  = e.get_last_price_book(AMD_ID)
    nvda_pb = e.get_last_price_book(NVDA_ID)
    if asml_pb and asml_pb.bids:
        last_asml_bid = asml_pb.bids[0].price
    if amd_pb and amd_pb.bids:
        last_amd_bid = amd_pb.bids[0].price
    if nvda_pb and nvda_pb.bids:
        last_nvda_bid = nvda_pb.bids[0].price
    if asml_pb and asml_pb.asks:
        last_asml_ask = asml_pb.asks[0].price
    if amd_pb and amd_pb.asks:
        last_amd_ask = amd_pb.asks[0].price
    if nvda_pb and nvda_pb.asks:
        last_nvda_ask = nvda_pb.asks[0].price

# --- Hedge Basket Strategy ---
def hedge_basket_strategy(e):
    """
    Hedge the basket so that:
      SEMIS_ETF_US = (ASML + NVDA + AMD)/3.
      
    When the ETF's mid price deviates from the underlying average by more than
    the dynamic grid threshold, trigger a hedge:
      - If ETF is undervalued relative to the underlying (i.e. mid ETF < avg_underlying - threshold):
            Buy ETF and sell the underlying stocks.
      - If ETF is overvalued (i.e. mid ETF > avg_underlying + threshold):
            Sell ETF and buy the underlying stocks.
            
    The allowed volumes are constrained by our risk management.
    """
    # Update the underlying prices so we have a fallback in case of market close.
    update_underlying_prices(e)

    # Retrieve live price books.
    etf_us = e.get_last_price_book(ETF_US_ID)
    asml   = e.get_last_price_book(ASML_ID)
    amd    = e.get_last_price_book(AMD_ID)
    nvda   = e.get_last_price_book(NVDA_ID)
    
    # Ensure that all necessary price books are available.
    if not (etf_us and asml and amd and nvda):
        return
    if not (etf_us.bids and etf_us.asks and asml.bids and asml.asks and
            amd.bids and amd.asks and nvda.bids and nvda.asks):
        return

    # Compute the average underlying price using the last known prices.
    avg_underlying_bid = (last_asml_bid + last_amd_bid + last_nvda_bid) / 3.0
    avg_underlying_ask = (last_asml_ask + last_amd_ask + last_nvda_ask) / 3.0

    # Use ETF_US's mid price as the reference.
    etf_bid = etf_us.bids[0].price
    etf_ask = etf_us.asks[0].price

    # --- Case 1: ETF appears undervalued ---
    if etf_ask < (avg_underlying_bid - BASKET_THRESHOLD):
        # We want to buy ETF (at its ask) and sell underlying stocks (at their bids).
        # Determine available volume from the order books:
        volume = min(
            etf_us.asks[0].volume // 3,
            asml.bids[0].volume,
            amd.bids[0].volume,
            nvda.bids[0].volume,
            10
        )
        # Apply risk management:
        allowed_buy_etf  = risk_allowed(e, ETF_US_ID, SIDE_BID)
        allowed_sell_asml = risk_allowed(e, ASML_ID, SIDE_ASK)
        allowed_sell_amd  = risk_allowed(e, AMD_ID, SIDE_ASK)
        allowed_sell_nvda = risk_allowed(e, NVDA_ID, SIDE_ASK)
        allowed_volume = min(volume, allowed_buy_etf, allowed_sell_asml, allowed_sell_amd, allowed_sell_nvda)
        if allowed_volume > 0:
            # Execute hedge orders.
            e.insert_order(ETF_US_ID, price=etf_us.asks[0].price, volume=allowed_volume * 3,
                           side=SIDE_BID, order_type=ORDER_TYPE_IOC)
            e.insert_order(ASML_ID, price=asml.bids[0].price, volume=allowed_volume,
                           side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
            e.insert_order(AMD_ID, price=amd.bids[0].price, volume=allowed_volume,
                           side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
            e.insert_order(NVDA_ID, price=nvda.bids[0].price, volume=allowed_volume,
                           side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
            record_trade(ETF_US_ID, SIDE_BID, etf_us.asks[0].price, allowed_volume * 3)
            record_trade(ASML_ID, SIDE_ASK, asml.bids[0].price, allowed_volume)
            record_trade(AMD_ID, SIDE_ASK, amd.bids[0].price, allowed_volume)
            record_trade(NVDA_ID, SIDE_ASK, nvda.bids[0].price, allowed_volume)
            logger.info(f"[Hedge] ETF undervalued (Underlying-value: {avg_underlying_bid}): Bought {allowed_volume * 3} of {ETF_US_ID} at {etf_us.asks[0].price} "
                        f"and sold underlying stocks at {asml.bids[0].price}, {amd.bids[0].price}, {nvda.bids[0].price}")
    
    # --- Case 2: ETF appears overvalued ---
    elif etf_bid > (avg_underlying_ask + BASKET_THRESHOLD):
        # We want to sell ETF (at its bid) and buy underlying stocks (at their asks).
        volume = min(
            etf_us.bids[0].volume // 3,
            asml.asks[0].volume,
            amd.asks[0].volume,
            nvda.asks[0].volume,
            10
        )
        allowed_sell_etf = risk_allowed(e, ETF_US_ID, SIDE_ASK)
        allowed_buy_asml  = risk_allowed(e, ASML_ID, SIDE_BID)
        allowed_buy_amd   = risk_allowed(e, AMD_ID, SIDE_BID)
        allowed_buy_nvda  = risk_allowed(e, NVDA_ID, SIDE_BID)
        allowed_volume = min(volume, allowed_sell_etf, allowed_buy_asml, allowed_buy_amd, allowed_buy_nvda)
        if allowed_volume > 0:
            e.insert_order(ETF_US_ID, price=etf_us.bids[0].price, volume=allowed_volume * 3,
                           side=SIDE_ASK, order_type=ORDER_TYPE_IOC)
            e.insert_order(ASML_ID, price=asml.asks[0].price, volume=allowed_volume,
                           side=SIDE_BID, order_type=ORDER_TYPE_IOC)
            e.insert_order(AMD_ID, price=amd.asks[0].price, volume=allowed_volume,
                           side=SIDE_BID, order_type=ORDER_TYPE_IOC)
            e.insert_order(NVDA_ID, price=nvda.asks[0].price, volume=allowed_volume,
                           side=SIDE_BID, order_type=ORDER_TYPE_IOC)
            record_trade(ETF_US_ID, SIDE_ASK, etf_us.bids[0].price, allowed_volume * 3)
            record_trade(ASML_ID, SIDE_BID, asml.asks[0].price, allowed_volume)
            record_trade(AMD_ID, SIDE_BID, amd.asks[0].price, allowed_volume)
            record_trade(NVDA_ID, SIDE_BID, nvda.asks[0].price, allowed_volume)
            logger.info(f"[Hedge] ETF overvalued (Underlying value: {avg_underlying_ask}): Sold {allowed_volume * 3} of {ETF_US_ID} at {etf_us.bids[0].price} "
                        f"and bought underlying stocks at {asml.asks[0].price}, {amd.asks[0].price}, {nvda.asks[0].price}")


def trade_cycle(e: Exchange):
    """
    Run the arbitrage strategies.
    """
    hedge_basket_strategy(e)
    arbitrage_strategy(e)
    reverse_arbitrage_strategy(e)
    auto_calibrate_positions(e)
    check_and_correct_basket_limit(e)
    

def start():
    """
    Connect to the exchange, clear any stale orders, and repeatedly run the trading cycle.
    """
    exchange = Exchange()
    exchange.connect()

    # Optional: clear stale orders for the ETFs.
    exchange.delete_orders(ETF_US_ID)
    exchange.delete_orders(ETF_EU_ID)

    sleep_duration_sec = 0.05  # Adjust as necessary.

    for id in INSTRUMENT_IDS:
        exchange.delete_orders(id)
    clear_all_stock(exchange)
    print(exchange.get_positions())
    while True:
        trade_cycle(exchange)
        time.sleep(sleep_duration_sec)

if __name__ == '__main__':
    start()
