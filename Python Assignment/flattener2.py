import json
import pandas as pd
import re
import socket
import aiodns 
import asyncio
import time

resolver = aiodns.DNSResolver()
# ip_cache = {}
import swifter
import os
import time

def extract_domain(url):
    """
    Extracts the domain from a given URL.

    Args:
        url (str): The URL from which to extract the domain.

    Returns:
        str: The domain extracted from the URL, or 'NA' if no domain can be found.
    """
    pattern = r"https?://(?:www\.)?([^/]+)"
    match = re.search(pattern, url)
    if match:
        return match.group(1)
    else:
        return 'NA'

async def get_ip_async(domain):
    # if domain in ip_cache:
    #     return ip_cache[domain]
    try:
        result = await resolver.gethostbyname(domain, socket.AF_INET)
        # ip_cache[domain] = result.addresses[0]
        return result.addresses[0]
    except Exception:
        return 'NA'
    
async def resolve_ips_async(unique_domains):
    print("Starting IP fetching...")
    # Track the start time
    start_time = time.time()
    tasks = [get_ip_async(domain) for domain in unique_domains['domain']]
    unique_domains['ipaddress'] = await asyncio.gather(*tasks)
    end_time = time.time()
    duration = end_time - start_time

    print(f"IP fetching completed in {duration:.2f} seconds.")

    return unique_domains

def parallelize_domain_ip(df):
    df = df.reset_index(drop=True)
    df['domain'] = df['infringing_urls'].swifter.apply(extract_domain)
    df = df[df['domain'] != 'NA']

    unique_domains = pd.DataFrame(df['domain'].unique(), columns=['domain'])

    # Replace this section with async handling
    loop = asyncio.get_event_loop()
    unique_domains = loop.run_until_complete(resolve_ips_async(unique_domains))

    df = df.merge(unique_domains, how='left', on='domain')
    df['ipaddress'] = df['ipaddress'].fillna('NA')

    return df

def summarize_data(df):
    """
    Summarizes the data with different perspectives.

    Args:
        df (pd.DataFrame): DataFrame containing the necessary columns for summarization.

    Returns:
        tuple: 
            - pd.DataFrame: Top 10 domains with the most DMCA notices.
            - pd.DataFrame: Distribution of DMCA notices over time.
            - pd.DataFrame: Top copyright holders and their most frequently reported infringing domains.
    """
    # 1. Top 10 domains with the most DMCA notices
    top_domains = df.groupby('domain').agg(
        notice_count=('domain', 'size'),
        unique_copyrighted_urls=('copyrighted_urls', lambda x: len(set(x)))
    ).reset_index()
    top_domains = top_domains.sort_values(by='notice_count', ascending=False).head(10)
    
    # 2. Distribution of DMCA notices over time
    df['date_sent'] = pd.to_datetime(df['date_sent'])
    time_distribution = df.groupby('date_sent').size().reset_index(name='notice_count')
    
    # 3. Top copyright holders and their most frequently reported infringing domains
    top_copyright_holders = df.groupby('principal_name').agg(
        notice_count=('principal_name', 'size'),
        top_infringing_domain=('domain', lambda x: x.value_counts().idxmax()),
        unique_infringing_domains=('domain', 'nunique')
    ).reset_index().sort_values(by='notice_count', ascending=False).head(20)
    
    return top_domains, time_distribution, top_copyright_holders

if __name__ == "__main__":
    # File paths
    input_file = 'flattened_response_domain_ip.csv'

    print(f"Processing data to regenerate {input_file}...")

    # Load JSON data from a file
    with open('response.json') as json_file:
        jsondata = json.load(json_file)

    # Flatten JSON data into a DataFrame
    df = pd.DataFrame.from_dict(jsondata['notices'])
    df = df.explode(column='works')
    df = pd.concat([df.drop('works', axis=1), pd.DataFrame(df['works'].apply(pd.Series))], axis=1)
    df = df.explode(column='copyrighted_urls')
    df = df.explode(column='infringing_urls')
    df['infringing_urls'] = df['infringing_urls'].apply(lambda x: x['url'])
    df['copyrighted_urls'] = df['copyrighted_urls'].apply(lambda x: x['url'])

    # Remove columns that are entirely null
    df = df.dropna(axis=1, how='all')

    # Extract domains and resolve IP addresses
    df = parallelize_domain_ip(df)

    # Save the updated DataFrame with domain and IP address columns
    df.to_csv(input_file, index=False)
    
    # Generate and save summaries
    top_domains, time_distribution, top_copyright_holders = summarize_data(df)

    top_domains.to_csv('top_10_infringing_domains.csv', index=False)
    time_distribution.to_csv('dmca_notices_time_distribution.csv', index=False)
    top_copyright_holders.to_csv('copyright_holders_rank_wise.csv', index=False)

    # Print shapes of summary files for verification
    print(f"Top 10 domains summary shape: {top_domains.shape}")
    print(f"DMCA notices time distribution shape: {time_distribution.shape}")
    print(f"Top copyright holders summary shape: {top_copyright_holders.shape}")