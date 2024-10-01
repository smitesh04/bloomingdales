import csv
import gzip
import re
import io

import pandas as pd
import requests
import hashlib
from fake_useragent import UserAgent
ua = UserAgent()
# from db_config import DbConfig
# obj = DbConfig()
def create_md5_hash(input_string):
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()

def download_csv(url, local_filename):
    # Send a GET request to the URL
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Check for request errors
        # Open a local file with write mode
        with open(local_filename, 'wb') as f:
            # Write the response content in chunks
            for chunk in response.iter_content():
                f.write(chunk)
    print(f"CSV file downloaded: {local_filename}")

# url = 'https://www.bloomingdales.com/dyn_img/sitemap/bcom_sitemapindex.xml'
file = open("sitemap.txt", "r", encoding='utf-8')
data = file.read()
file.close()
print()

all_urls = re.findall('<loc>.*?</loc>', data)
for url in all_urls:
    url = url.replace('<loc>', '').replace('</loc>', '')

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'user-agent': ua.random,
        # 'Cookie': '_abck=531344FCB8AB681050D4C05427B06619~-1~YAAQpiTDFxudMzGSAQAAP8bTMgzJ2tcNAsImrHFNTAG8KDpJ63QyBiPO8J3XzTk5IpdjH/sKFM8D0KfsETzn6QnKyQkN7p2bpVpibgfQxY6E8FQyhiWShMrM+165CeIG32X4nIdt1phLuy8QI9gkvIDH4EgERUY3DTHPBPjIkT6jyG4hDmPgCPePmcC11vxlv65FL3qB2tF55lZwGwK3OO7Gwgr10QpWz89Gp5bdz7/YaAGr01Eez6oGemTF9Nmav6XoBQE3fINqnaUcwXtvGphAc4ZoNtHbqwwkaPEV3Ckq2luG1vcPetno9KwDZqiE1RJDxoqsDEGSlbd3UlrPT35/BaUHmoG0yyur9r3dzP8gPMBLjxx4LuCtSzbeYV8pgVn/bU2MlE+Z/YhCJw3T4xXMdcP4mnE4ix7Ff5YoCjjqQEj1jzWYD8hwcX1wnVg=~-1~-1~-1; bm_sz=397A27EB6C8A8F485D481A78D3702C6A~YAAQpiTDFxydMzGSAQAAP8bTMhljifz64Uz6qlfYRw/4ffcXnTnj8NDukg5WbvwFigID1mRLhVw5GbZ5JGJX5vzx8M3ZRi8VQ0lLKtWbkCqAqm23rVpU7BzPBYZxqPmHzlqbElyh0deRLgfc7hrwaums38bNnbCQWfcavfmMICiqiHBirB0ic72uIppTDY6eVDIX8hHe75kNWcrzWx86xRu9wJmLYi5SGT3JI9M5iHvYXVpzJ70GsGj0psBUxxMrYsLMjRUm3VQsd1joVC+wQCOcRcQJLhDKzIGBkQlxo/jtsuEtLTmH5HxF1vYuS8iL4ZlvPAHBD4W76ZBak2O+Xr78+L6xia24AdYlFFAbBzxjm9X3AfV5DpPDueOfFkfUzuFXR76EkaBY8drnjl67/LwWg/PRLDnv8Vf+~4600388~4338484; currency=USD; shippingCountry=US'
    }
    # r = requests.get(url, headers=headers)

    hashid = create_md5_hash(url)
    # download_csv(url, hashid)
    # file = open(fr"C:/Users/Actowiz/Desktop/Smitesh_Docs/Project/apparel_store_locator/csv/{hashid}.csv", "w", encoding='utf8')
    print()
    with requests.Session() as s:
        download = s.get(url, headers=headers)

        if download.status_code == 200:
            # Write the content to a .rar file
            with open(fr"C:/Users/Actowiz/Desktop/Smitesh_Docs/Project/bloomingdales/sitemap/{hashid}.rar", 'wb') as f:
                f.write(download.content)
            print("RAR file downloaded successfully.")
        else:
            print(f"Failed to download file. Status code: {download.status_code}")
