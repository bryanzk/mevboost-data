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
    df_bidding = pd.read_parquet('titan921.parquet', engine='pyarrow')
    print(df_bidding.columns)
        
    
    df_bidding.loc[:, 'block_timestamp'] = pd.to_datetime(df_bidding['block_timestamp'].str.replace(' UTC', ''), format='%Y-%m-%d %H:%M:%S', errors='coerce')
    df_bidding.loc[:, 'timestamp'] = pd.to_datetime(df_bidding['timestamp'].str.replace(' UTC', ''), format='%Y-%m-%d %H:%M:%S.%f', errors='coerce')

    
    # 计算时间差并且存储在新的 Dataframe 中 Calculate the time difference and store it in a new Dataframe.
    ts_diff_dft = (df_bidding['block_timestamp'] - df_bidding['timestamp']).apply(lambda x: x.total_seconds()) * 1000
    ts_diff_dft = ts_diff_dft.apply(lambda x: 0 if abs(x) < 0.001 else x)

    # 添加新的列到原始的 Dataframe 中.  Add a new column, ts_diff, as ms difference, to the original Dataframe.
    # if ts_diff > 0, bid before 12s, if ts_diff<0, bid after 12s
    df_bidding = pd.concat([df_bidding, ts_diff_dft.rename('ts_diff')], axis=1)
    
    df_bidding['ts_diff_secs'] = df_bidding['ts_diff'] / 1000
    
    ## START processing builder labels
    ## Bidding data has a lot of builder pubkeys presenting. We need to find out the builder labels for these pubkeys. 
    ## Step 1: Get the builder labels from the matching blocks: inner join block number, left join builder_pubkey
    ## Step 2: If builder labels are still None or NaN, then get the builder labels from the latest TLDR talk: left join builder_pubkey
    ## Step 3: If builder labels are still None or NaN, then we use the first 8 hex chars from the pubkey and add "FAILED_" as the start of the label.
   
    df_blocks_with_builder = get_raw_block_data_with_winning_bids_and_latest_builder_label_from_CSV()
    
    inner_merged_df = pd.merge(df_bidding, df_blocks_with_builder, on='block_number', how='inner')

    # Step 2: Left join on builder_pubkey from the result of the inner join
    result_df = pd.merge(inner_merged_df, df_blocks_with_builder[['builder_pubkey', 'builder_label']], on='builder_pubkey', how='left')

    # Add org_builder_label column
    result_df['org_builder_label'] = result_df['builder_label']

    # Drop unnecessary columns if any
    result_df = result_df.drop(['builder_label'], axis=1)
    
    
    # ## Step 1: Get the builder labels from the matching blocks: inner join block number, left join builder_pubkey
    # #  1: Inner join on block_number
    # inner_merged_bidding_df = pd.merge(df_bidding, df_blocks_with_builder_for_innerjoin, on='block_number', how='inner', suffixes=('_bidding', '_block'))
    
    # #######  分步join 仍然有问题  #######  分步join 仍然有问题  #######  分步join 仍然有问题  #######  分步join 仍然有问题  
    # ### maybe we can use 2 df to do the 2 join

    # #  2: Left join on builder_pubkey from the result of the inner join
    # result_df = pd.merge(inner_merged_bidding_df, df_blocks_with_builder_for_leftjoin[['builder_pubkey', 'builder_label']], on='builder_pubkey', how='left')

    # # Add org_builder_label column
    # result_df['org_builder_label'] = result_df['builder_label']

    # # Drop unnecessary columns if any
    # result_df = result_df.drop(['builder_label'], axis=1)


    
    
    ## Step 2: If builder labels are still None or NaN, then get the builder labels from the latest TLDR talk: left join builder_pubkey
    # Load latest TLDR builder info
    df_tldr_builder_info = get_builder_info_from_latest_TLDR_talk()
    df_bidding_with_new_builder_label = pd.merge(result_df, df_tldr_builder_info, how='left', on='builder_pubkey')

    #######  后面的builder label 取值仍然需要处理  #######  后面的builder label 取值仍然需要处理  #######  后面的builder label 取值仍然需要处理  #######  后面的builder label 取值仍然需要处理
    # After the join, the builder_label column now is from the TLDR data, we rename it as tldr_builder_label.
    df_bidding_with_new_builder_label['tldr_builder_label'] = df_bidding_with_new_builder_label['builder_label']
    df_bidding_with_new_builder_label = df_bidding_with_new_builder_label.drop('builder_label', axis=1)
    
    
    # df_bidding_with_new_builder_label['builder_label'] = df_bidding_with_new_builder_label['org_builder_label'].fillna(df_bidding_with_new_builder_label['tldr_builder_label'])


    # ## Step 3: If builder labels are still None or NaN, then we use the first 8 hex chars from the pubkey and add "FAILED_" as the start of the label.
    # df_bidding_with_new_builder_label.loc[df_bidding_with_new_builder_label['builder_label'] == '', 'builder_label'] = 'FAILED_' + df_bidding_with_new_builder_label['builder_pubkey'].str[:10]
    
    
    # # Merge the block data with the newest builder label data on builder's pubkeys. 
    # # We are using "LEFT" join because that some of pubkeys are not in the latest builder data
    # df_with_new_builder_label = pd.merge(df_bidding, df_blocks_with_builder, how='left', on='builder_pubkey')
    
    # # After the join, the builder_label column now is from the TLDR data, we rename it as tldr_builder_label.
    # df_with_new_builder_label.rename(columns={'builder_label': 'tldr_builder_label'}, inplace=True)
    
    # # We create a new builder_label column to store the final result of builder label. 
    # # This would prevent the coupling of reference places, which can always use "builder_label" to access the latest data. 
    # # The value of "builder_label" follows logic as:
    # # If a pubkey of a builder can be found in the TLDR data, then use the TLDR builder label
    # # If the pubkey can't matach any from TLDR, then use the existing one: org_builder_label.
    # df_with_new_builder_label['builder_label'] = df_with_new_builder_label['tldr_builder_label'].fillna(df_with_new_builder_label['org_builder_label'])
    
    # # If the org_builder_label, the label in the dataalways project,  is "", then we use the first 8 hex chars from the pubkey.
    # df_with_new_builder_label.loc[df_with_new_builder_label['builder_label'] == '', 'builder_label'] = df_with_new_builder_label['builder_pubkey'].str[:10]
    
    

    return df_bidding_with_new_builder_label
    
def get_titan_march_blocks_with_to_and_from():
    pd.options.display.float_format = '{:.0f}'.format
    dft = pd.read_parquet('titan_march_blocks_builder_rewards.parquet', engine='pyarrow')
    return dft

def get_eigenphi_march_blocks_with_to_and_from():
    df = pd.read_csv("eigenphi_march_block_builder_rewards.csv")
    return df



####### NO NEED TO USE THIS ONE BECAUSE WE HAVE THE CSV FILE CONTAINING ONLY THE BLOCKS BUILT BY TITAN
# # Get all the block data from the latest CSV from dataalways project, then process the builder label
# # ABOUT builder label:
# # If a pubkey of a builder can be found in the CSV imported from the latest set: https://bit.ly/3Vs16HU, then use the label from the CSV.
# # If the pubkey can't matach any from the csv, then use the existing one from dataalways project. 
# # If the label in the dataalways project is "", then we use the first 8 hex chars from the pubkey.
# def get_raw_block_data_with_winning_bids_and_latest_builder_label():
# ##### STOP USING LOCAL DATA, USE THE CSV FILE INSTEAD
# #     # Load winning bid block history data
# # #  !!!the data here is NOT THE LATEST FROM THE OG MEV DATA ALWAYS PROJECT. Go to: https://github.com/dataalways/mevboost-data to sync.
# #     base_path = '../data/'
# #     file_paths = os.listdir(base_path)

# #     dfs = []
# #     for file in file_paths:
# #         if len(file) < 10: #.DS_store
# #             continue
        
# #         df_tmp = pd.read_parquet(os.path.join(base_path, file))
# #         dfs.append(df_tmp)

# #     df = pd.concat(dfs)

#     df = get_raw_block_data_with_winning_bids_and_latest_builder_label_from_CSV()
#     print(df.shape[0])
#     df = df[df['payload_delivered'] == True]
#     df.sort_values(by=['block_number', 'bid_timestamp_ms'], ascending=True, inplace=True)
#     df.reset_index(inplace=True, drop=True)

#     df.dropna(subset='relay', inplace=True)
#     # drop non-boost blocks

#     df.drop_duplicates(subset='block_hash', keep='first', inplace=True)
#     # drop relays that got the data late, only keep the earliest.

#     df.reset_index(inplace=True, drop=True)
    
    
#     ## START PROCESSING BUILDER LABELS
#     # Rename the builder_label column as org_builder_label to prep for data merg
#     df.rename(columns={'builder_label': 'org_builder_label'}, inplace=True)
    
#     # Load latest TLDR builder info
#     df_tldr_builder_info = get_builder_info_from_latest_TLDR_talk()
    
#     # Merge the block data with the newest builder label data on builder's pubkeys. 
#     # We are using "LEFT" join because that some of pubkeys are not in the latest builder data
#     df_with_new_builder_label = pd.merge(df, df_tldr_builder_info, how='left', on='builder_pubkey')
    
#     # After the join, the builder_label column now is from the TLDR data, we rename it as tldr_builder_label.
#     df_with_new_builder_label.rename(columns={'builder_label': 'tldr_builder_label'}, inplace=True)
    
#     # We create a new builder_label column to store the final result of builder label. 
#     # This would prevent the coupling of reference places, which can always use "builder_label" to access the latest data. 
#     # The value of "builder_label" follows logic as:
#     # If a block's builder info already exists in the dataalways project, then use the existing one: org_builder_label.
#     # Otherwise, if a pubkey of a builder can be found in the TLDR data, then use the TLDR builder label.
#     df_with_new_builder_label['builder_label'] = df_with_new_builder_label['org_builder_label'].fillna(df_with_new_builder_label['tldr_builder_label'])
    
#     # Otherwise, if a block's builder pubkey has no label, then use the first 8 hex chars from the pubkey.
#     df_with_new_builder_label.loc[df_with_new_builder_label['builder_label'] == '', 'builder_label'] = df_with_new_builder_label['builder_pubkey'].str[:10]
#     print(df_with_new_builder_label.shape[0])
#     return df_with_new_builder_label


    
def get_block_data_with_winning_bids_having_bid_ts():
    # Load winning bid block history data
    df = get_raw_block_data_with_winning_bids_and_latest_builder_label()
    
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


def get_raw_block_data_with_winning_bids_and_latest_builder_label_from_CSV():
    return pd.read_csv("blocks_by_titan_19433573_to_19440930_with_builders.csv")

# def get_builder_info_by_df_block_numbers(df_block_number):
#     # Load winning bid block history data with builder info
#     df_blocks_with_builder = get_raw_block_data_with_winning_bids_and_latest_builder_label()
    
#     # merege_builder 
    
#     df = df_blocks_with_builder[df_blocks_with_builder['block_number'].isin(df_block_number)]
    
#     # Merge the block data with the newest builder label data on builder's pubkeys. 
#     # We are using "LEFT" join because that some of pubkeys are not in the latest builder data
#     df_with_new_builder_label = pd.merge(df, df_builder_info, how='left', on='builder_pubkey')
    
#     # After the join, the builder_label column now is from the TLDR data, we rename it as tldr_builder_label.
#     df_with_new_builder_label.rename(columns={'builder_label': 'tldr_builder_label'}, inplace=True)
    
#     # We create a new builder_label column to store the final result of builder label. 
#     # This would prevent the coupling of reference places, which can always use "builder_label" to access the latest data. 
#     # The value of "builder_label" follows logic as:
#     # If a pubkey of a builder can be found in the TLDR data, then use the TLDR builder label
#     # If the pubkey can't matach any from TLDR, then use the existing one: org_builder_label.
#     df_with_new_builder_label['builder_label'] = df_with_new_builder_label['tldr_builder_label'].fillna(df_with_new_builder_label['org_builder_label'])
    
#     # If the org_builder_label, the label in the dataalways project,  is "", then we use the first 8 hex chars from the pubkey.
#     df_with_new_builder_label.loc[df_with_new_builder_label['builder_label'] == '', 'builder_label'] = df_with_new_builder_label['builder_pubkey'].str[:10]
    
#     return df_with_new_builder_label
    



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
    return df_builder_info

def get_builder_info_from_latest_TLDR_talk():
    df_builder_info = pd.read_csv('TLDR_Builder_Public_Keys.csv', usecols=['name', 'pubkey'])
    df_builder_info = df_builder_info.rename(columns={'name': 'builder_label', 'pubkey': 'builder_pubkey'})
    df_builder_info.sort_values(by='builder_label', inplace=True)
    return df_builder_info


def replace_small_values(value):
    return 0 if abs(value) < 0.001 else value