import matplotlib.pyplot as plt # type: ignore
import numpy as np # type: ignore
import os
import pandas as pd # type: ignore
import requests
from bs4 import BeautifulSoup
import pandas as pd


def get_the_latest_titan_pubkey_from_website():
    # prep Titan Builder public key from their page
    url = 'https://docs.titanbuilder.xyz/builder-public-keys'
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')

    assorted_list = [tag.text for tag in soup.find_all('li')]
    titan_pub_key_list = [key for key in assorted_list if key.startswith('0x')]
    return titan_pub_key_list

def get_titan_won_921_blocks_bidding_data():
    # Read 921 blocks' bidding data. Titan won these 921 blocks.
    pd.options.display.float_format = '{:.0f}'.format
    # Read the titan 921 block bidding history parquet file
    dft = pd.read_parquet('titan921.parquet', engine='pyarrow')
    return dft
    
    
def get_block_data_with_winning_bids():
    # Load winning bid block history data
#  !!!the data here is NOT THE LATEST FROM THE OG MEV DATA ALWAYS PROJECT. Go to: https://github.com/dataalways/mevboost-data to sync.
    base_path = '../data/'
    file_paths = os.listdir(base_path)

    dfs = []
    for file in file_paths:
        if len(file) < 10: #.DS_store
            continue
        
        df_tmp = pd.read_parquet(os.path.join(base_path, file))
        dfs.append(df_tmp)

    df = pd.concat(dfs)
    df = df[df['payload_delivered'] == True]
    df.sort_values(by=['block_number', 'bid_timestamp_ms'], ascending=True, inplace=True)
    df.reset_index(inplace=True, drop=True)

    df.dropna(subset='relay', inplace=True)
    # drop non-boost blocks

    df.drop_duplicates(subset='block_hash', keep='first', inplace=True)
    # drop relays that got the data late, only keep the earliest.

    df.reset_index(inplace=True, drop=True)
    return df

def get_builder_info():
    # prepare builder label data frame from the winning block data, these are the builders succeed in submitting bidding and building block or blocks.
    # We only need the latest builder label matching the pubkey
    # 首先对 'builder_pubkey' 和 'block_timestamp' 进行排序
    df = get_block_data_with_winning_bids()
    df = df.sort_values(by=['builder_pubkey', 'block_timestamp'])

    # 然后，选择每一个 'builder_pubkey' 的最后一个 'builder_label'，并把这两列放入一个新的 DataFrame
    df_builder_info = df.groupby('builder_pubkey')[['builder_label']].last().reset_index()

    # extract builder pubkeys from the 921 titan bidding data. Some of these builders NEVER built a block. 
    # We will use FAILED_UNKNOWN_BUILDERS as their label
    dft_builder = dft[['builder_pubkey']].drop_duplicates()

    # Find out the FAILED_UNKNOWN_BUILDERS, create a data frame for them and add the data frame to the overall builder data frame.
    not_in_df_builder_info = dft_builder[~dft_builder['builder_pubkey'].isin(df_builder_info['builder_pubkey'])]
    missing_labels = pd.DataFrame({'builder_pubkey': not_in_df_builder_info['builder_pubkey'].unique(), 
                                'builder_label': 'FAILED_UNKNOWN_BUILDERS'})
    df_builder_info = pd.concat([df_builder_info, missing_labels], ignore_index=True)
    return df_builder_info
