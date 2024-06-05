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
        
    
    dft.loc[:, 'block_timestamp'] = pd.to_datetime(dft['block_timestamp'].str.replace(' UTC', ''), format='%Y-%m-%d %H:%M:%S', errors='coerce')
    dft.loc[:, 'timestamp'] = pd.to_datetime(dft['timestamp'].str.replace(' UTC', ''), format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

    
    # 计算时间差并且存储在新的 Dataframe 中 Calculate the time difference and store it in a new Dataframe.
    ts_diff_dft = (dft['block_timestamp'] - dft['timestamp']).apply(lambda x: x.total_seconds()) * 1000
    ts_diff_dft = ts_diff_dft.apply(lambda x: 0 if abs(x) < 0.001 else x)

    # 添加新的列到原始的 Dataframe 中.  Add a new column, ts_diff, as ms difference, to the original Dataframe.
    # if ts_diff > 0, bid before 12s, if ts_diff<0, bid after 12s
    dft = pd.concat([dft, ts_diff_dft.rename('ts_diff')], axis=1)
    
    dft['ts_diff_secs'] = dft['ts_diff'] / 1000

    return dft
    
def get_titan_march_blocks_with_to_and_from():
    pd.options.display.float_format = '{:.0f}'.format
    dft = pd.read_parquet('titan_march_blocks_builder_rewards.parquet', engine='pyarrow')
    return dft

def get_eigenphi_march_blocks_with_to_and_from():
    df = pd.read_csv("eigenphi_march_block_builder_rewards.csv")
    return df


def get_raw_block_data_with_winning_bids():
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


    
def get_block_data_with_winning_bids_having_bid_ts():
    # Load winning bid block history data
    df = get_raw_block_data_with_winning_bids()
    
    ##############################
    # N.B.: WE ARE DROPPING ROWS WITH NULL VALUES IN 'bid_timestamp_ms' COLUMN.
    #       THIS IS BECAUSE WE ARE INTERESTED IN BIDS THAT WERE SUCCESSFUL.(NO BIDS WAS MARKED AS BLUE ON PAYLOAD.DE)
    #       EXAMPLE: https://payload.de/data/18326108/
    #       BIDS THAT WERE NOT SUCCESSFUL WILL NOT HAVE A TIMESTAMP.
    ##############################
    df = df.dropna(subset=['bid_timestamp_ms'])
    
    df.loc[:,'org_bid_timestamp_ms'] = df['bid_timestamp_ms']
        
    df.loc[:,'bid_timestamp_ms'] = df['bid_timestamp_ms'].astype('int64') # 确保时间戳为整数
    df.loc[:,'bid_timestamp_ms'] = pd.to_datetime(df['bid_timestamp_ms'], unit='ms')
    
    # 计算时间差并且存储在新的 Dataframe 中 Calculate the time difference and store it in a new Dataframe.
    ts_diff_df = (df['block_datetime'] - df['bid_timestamp_ms']).apply(lambda x: x.total_seconds()) * 1000
    ts_diff_df = ts_diff_df.apply(lambda x: 0 if abs(x) < 0.001 else x)

    # 添加新的列到原始的 Dataframe 中.  Add a new column, ts_diff, as ms difference, to the original Dataframe.
    # if ts_diff > 0, bid before 12s, if ts_diff<0, bid after 12s
    df = pd.concat([df, ts_diff_df.rename('ts_diff')], axis=1)
    
    df['ts_diff_secs'] = df['ts_diff'] / 1000

    return df

def get_builder_info_from_dataalways_block(df_with_ts_diff):
    # prepare builder label data frame from the winning block data, these are the builders succeed in submitting bidding and building block or blocks.
    # We only need the latest builder label matching the pubkey
    # 首先对 'builder_pubkey' 和 'block_timestamp' 进行排序
    if (df_with_ts_diff is None):
        df = get_block_data_with_winning_bids_having_bid_ts()
    else:
        df = df_with_ts_diff        
    df = df.sort_values(by=['builder_pubkey', 'block_timestamp'])

    # 然后，选择每一个 'builder_pubkey' 的最后一个 'builder_label'，并把这两列放入一个新的 DataFrame
    df_builder_info = df.groupby('builder_pubkey')[['builder_label']].last().reset_index()

    # extract builder pubkeys from the 921 titan bidding data. Some of these builders NEVER built a block. 
    # We will use FAILED_UNKNOWN_BUILDERS as their label
    dft_builder = df[['builder_pubkey']].drop_duplicates()

    # Find out the FAILED_UNKNOWN_BUILDERS, create a data frame for them and add the data frame to the overall builder data frame.
    not_in_df_builder_info = dft_builder[~dft_builder['builder_pubkey'].isin(df_builder_info['builder_pubkey'])]
    missing_labels = pd.DataFrame({'builder_pubkey': not_in_df_builder_info['builder_pubkey'].unique(), 
                                'builder_label': 'FAILED_UNKNOWN_BUILDERS'})
    df_builder_info = pd.concat([df_builder_info, missing_labels], ignore_index=True)
    
    # Add imposter builder labels. Based on: https://collective.flashbots.net/t/block-builder-profitability-research/2803
    imposter = ['0xa95b3a3cfc35a77663d6a5a9ac133bf1b68b4118f7f7a6f4ec43b298211441d1ebd1a1063446fea18138e7ef6c1379b6',
        '0xb61a17407826a0c7a20ce8a0e9c848350bb94bf258be9c40da0dafd5be83be240c3d24c901e1d4423cc2eb90703ff0bc',
        '0xa003117a3befd6d4f4f5a6db633caf7a2038d3f195c97a6b83ce6760cbbb1c0d09c11c33286fb14eb64c33ffb47f93cf']
    
    df_builder_info.loc[df_builder_info['builder_pubkey'].isin(imposter), 'builder_label'] = 'IMPOSTER ' + df_builder_info['builder_label']

    
    return df_builder_info

def get_builder_info_from_latest_TLDR_talk():
    df_builder_info = pd.read_csv('Builder_Public_Keys.csv', usecols=['name', 'pubkey'])
    df_builder_info = df_builder_info.rename(columns={'name': 'builder_label', 'pubkey': 'builder_pubkey'})
    df_builder_info.sort_values(by='builder_label', inplace=True)
    return df_builder_info


def replace_small_values(value):
    return 0 if abs(value) < 0.001 else value