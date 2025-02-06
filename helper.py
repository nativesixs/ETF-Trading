import logging
import time
from typing import List
from optibook import common_types as t
from optibook import ORDER_TYPE_IOC, ORDER_TYPE_LIMIT, SIDE_ASK, SIDE_BID
from optibook.exchange_responses import InsertOrderResponse
from optibook.synchronous_client import Exchange
import random
import json

INSTRUMENT_IDS = ['SEMIS_ETF_US', 'SEMIS_ETF_EU', 'NVDA', 'AMD', 'ASML']

logging.getLogger('client').setLevel('ERROR')
logger = logging.getLogger(__name__)
# Checks if the market 
def is_up(priceBook) -> bool:
    return priceBook and priceBook.asks and priceBook.bids

def print_order_response(order_response: InsertOrderResponse):
    if order_response.success:
        logger.info(f"Inserted order successfully, order_id='{order_response.order_id}'")
    else:
        logger.info(f"Unable to insert order with reason: '{order_response.success}'")

def print_all_responses(reponses):
    for response in reponses:
        print_order_response(response)

def clear_excess_stock(e: Exchange): 
    positions = e.get_positions()
    for id in INSTRUMENT_IDS:
        pb = e.get_last_price_book(id)
        if (not is_up(pb)):
            continue
        # print(pb)
        # print(positions[id])
        if positions[id] == 0 or abs(positions[id]) < 500:
            continue
        # We've sold too much, need to buy back 
        if positions[id] < 0:
            e.insert_order(id, price=pb.asks[0].price, volume=abs(positions[id]), side=SIDE_BID, order_type=ORDER_TYPE_LIMIT)
        else:
            e.insert_order(id, price=pb.bids[0].price, volume=positions[id], side=SIDE_ASK, order_type=ORDER_TYPE_LIMIT)

def clear_all_stock(e: Exchange): 
    positions = e.get_positions()
    for id in INSTRUMENT_IDS:
        pb = e.get_last_price_book(id)
        if (not is_up(pb)):
            continue
        # print(pb)
        # print(positions[id])
        if positions[id] == 0:
            continue
        # We've sold too much, need to buy back 
        if positions[id] < 0:
            e.insert_order(id, price=pb.asks[0].price, volume=abs(positions[id]), side=SIDE_BID, order_type=ORDER_TYPE_LIMIT)
        else:
            e.insert_order(id, price=pb.bids[0].price, volume=positions[id], side=SIDE_ASK, order_type=ORDER_TYPE_LIMIT)

# Returns 10% low and 10% of average of last 10s of data
def calc_range(e: Exchange): 
    cache_asks = {'SEMIS_ETF_US': [],
             'SEMIS_ETF_EU': [],
             'NVDA': [],
             'ASML': [],
             'AMD': [],
    }
    cache_bids = {'SEMIS_ETF_US': [],
             'SEMIS_ETF_EU': [],
             'NVDA': [],
             'ASML': [],
             'AMD': [],
    }
    sleep_time = 0.1
    print("started collecting data")
    for i in range(100):
        for id in INSTRUMENT_IDS: 
            pd = e.get_last_price_book(id)
            if (not is_up(pd)):
                continue
            cache_asks[id].append(pd.asks[0].price)
            cache_bids[id].append(pd.bids[0].price)
        time.sleep(sleep_time)
    print("finished collecting data")

    for key in cache_asks.keys():
        data = cache_asks[key]
        sorted_data = sorted(data)
        low_index = int(len(sorted_data) * 0.1)
        high_index = int(len(sorted_data) * 0.9)
        cache_asks[key] = (cache_asks[key][low_index], cache_asks[key][high_index])
    for key in cache_bids.keys():
        data = cache_bids[key]
        sorted_data = sorted(data)
        low_index = int(len(sorted_data) * 0.1)
        high_index = int(len(sorted_data) * 0.9)
        cache_bids[key] = (cache_bids[key][low_index], cache_bids[key][high_index])
    return (cache_asks, cache_bids)
    
