import time
import os
import requests
import pandas as pd
import asyncio
import aiohttp
import os
from datetime import datetime
from bs4 import BeautifulSoup


user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

best_seller_main = 'https://www.yes24.com/Product/Category/BestSeller?categoryNumber=001&pageSize=24'
month_seller_main = 'https://www.yes24.com/Product/Category/MonthWeekBestSeller?categoryNumber=001&pageSize=24'
steady_seller_main = 'https://www.yes24.com/Product/Category/SteadySeller?categoryNumber=001&pageSize=24'

book_selector = '#yesBestList > li > div > div.item_info > div.info_row.info_name > a.gd_name'
rank_selector = '#yesBestList > li > div > div.item_img > div.img_canvas > div > em'
book_name_selector= "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > div > h2"
auth_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > span.gd_pubArea > span.gd_auth"
publish_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > span.gd_pubArea > span.gd_pub"
date_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > span.gd_pubArea > span.gd_date"
price_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoBot > div.gd_infoTbArea > div > table > tbody > tr > td > span > em"
category_selector = '#infoset_goodsCate > div.infoSetCont_wrap > dl:nth-child(1) > dd > ul'
introduce_selector = '#infoset_introduce > div.infoSetCont_wrap'

test_get_url_list = [best_seller_main, month_seller_main, steady_seller_main]
test_list = ['https://www.yes24.com/Product/Goods/124999476', 'https://www.yes24.com/Product/Goods/125558276', 'https://www.yes24.com/Product/Goods/122120495']

def get_book_url(links):
    for l in links:     
        res = requests.get(l, headers={"user-agent":user_agent})
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, "lxml")
            book_list = soup.select(book_selector)
            rank_list = soup.select(rank_selector)
            result_list = []
            for book, ranks in zip(book_list, rank_list):
                link = book.get("href")  # href 속성 값
                rank = ranks.get_text()
                result_list.append(('https://www.yes24.com/'+link,int(rank)))
        
        else:
            raise Exception(f"요청 실패. 응답코드: {res.status_code}")
    return result_list

async def get_book_info(url, session):
    async with session.get(url[0]) as res:
        if res.status == 200:
            pk = url[0].split('/')[-1]
            html = await res.text()
            soup = BeautifulSoup(html, "lxml")
            rank = url[1]

            book_name = soup.select(book_name_selector)[0].get_text()

            auth_datas = soup.select(auth_selector)
            auth_list = []
            for a in auth_datas:
                auth_list.append(a.get_text().strip().replace("/n",''))

            publish = soup.select(publish_selector)[0].get_text()

            date = soup.select(date_selector)[0].get_text().replace(' ','-').replace('월','').replace('일','').replace('년','')

            price_list = soup.select(price_selector)
            pricese = []
            for p in price_list:
                pricese.append(p.get_text().replace(',','').replace('원',''))

            category_list = soup.select(category_selector)
            category_datas = ['']*4
            for cl in category_list:
                remove_list = ['\xa0\n','\n','\r']
                category_data = cl.get_text().strip()
                for r in remove_list:
                    category_data = category_data.replace(r,'')
                cd_i = 0
                for cd in category_data.split('국내도서>'):
                    if cd_i == 4:
                        continue
                    if len(cd) > 1:
                        category_datas[cd_i]=cd.strip().split('>')[0]
                        category_datas[cd_i+1]=cd.strip().split('>')[1]
                        cd_i += 2

            introduce_list = soup.select(introduce_selector)
            introduce_datas = []
            for il in introduce_list:
                remove_list = ['\xa0','\n','\r','책의 일부 내용을 미리 읽어보실 수 있습니다. 미리보기','MD 한마디']
                introduce_data = il.get_text()
                for r in remove_list:
                    introduce_data = introduce_data.replace(r,'')
                introduce_datas.append(introduce_data.strip())
            if introduce_datas == []:
                introduce_datas = ['']

            result_list = [pk, rank, book_name, auth_list[0], publish, date, int(pricese[0]), int(pricese[1]), *category_datas, introduce_datas[0]]
            
            return result_list
        else:
            raise Exception(f"요청 실패. 응답코드: {res.status_code}")

async def main(links):
    async with aiohttp.ClientSession(headers={"user-agent":user_agent}) as session:
        result = await asyncio.gather(*[get_book_info(url, session) for url in links])
    return result


if __name__ == '__main__':

    os.chdir(r'C:\Projects\DA35 2nd project\DA35-2nd-SMJ-OBC\OBC')
    os.makedirs('Datas/best_seller_datas', exist_ok=True)
    os.makedirs('Datas/month_seller_datas', exist_ok=True)
    os.makedirs('Datas/steady_seller_datas', exist_ok=True)

    best_pages = ['https://www.yes24.com/Product/Category/BestSeller?categoryNumber='+str(x)+'&pageSize=24' for x in range(1,2)]
    best_seller_links = get_book_url(best_pages)
    best_seller_datas = asyncio.run(main(best_seller_links))
    

    best_df = pd.DataFrame(best_seller_datas)
    d = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    best_file_path = f"Datas/best_seller_datas/{d}.csv"
    best_df.to_csv(best_file_path, index=False)

'''
    month_pages = ['https://www.yes24.com/Product/Category/MonthWeekBestSeller?categoryNumber='+str(x)+'&pageSize=24' for x in range(1,3)]
    month_seller_links = get_book_url(month_pages)
    month_seller_datas = asyncio.run(main(month_seller_links))

    month_df = pd.DataFrame(month_seller_datas)

    steady_pages = ['https://www.yes24.com/Product/Category/SteadySeller?categoryNumber='+str(x)+'&pageSize=24' for x in range(1,3)]
    steady_seller_links = get_book_url(steady_pages)
    steady_seller_datas = asyncio.run(main(steady_seller_links))

    steady_df = pd.DataFrame(steady_seller_datas)
'''
    

'''
    ##### 파일로 저장.
    

    month_file_path = f"Datas/month_seller_datas'{d}.csv"
    month_df.to_csv(month_file_path, index=False)

    steady_file_path = f"Datas/steady_seller_datas'{d}.csv"
    steady_df.to_csv(steady_file_path, index=False)
'''

    