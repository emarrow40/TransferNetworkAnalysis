from aiohttp import ClientSession
import asyncio
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from neo4jHelpers import clear_db, remove_loans, remove_orphan_clubs, set_constraints, write_club, write_stream
from tableParser import TableParser, ClubIncomeParser, ClubStreamParser
from typing import List, Union
from itertools import chain
from math import ceil

def get_income_urls(top_x_teams: int=50) -> List[str]:
    base_url = "https://www.transfermarkt.us/transfers/einnahmenausgaben/statistik/a/ajax/yw1/ids/a/sa//saison_id/2016/saison_id_bis/2021/land_id/0/nat/0/kontinent_id/0/pos//w_s//intern/0/plus/1/sort/saldo.desc"
    pages = ceil(top_x_teams / 25)
    all_urls = [base_url + f'/page/{i}' for i in range(2, (pages+1))]
    all_urls.append(base_url)
    return all_urls

async def get_html(s: ClientSession, url: str) -> str:
    async with s.get(url) as r:
        if r.status != 200:
            r.raise_for_status()
        html = await r.text()
        return html

async def extract_clubs(s: ClientSession, url: str) -> List[ClubIncomeParser]:
    html = await get_html(s, url)
    clubs = [ClubIncomeParser(tr) for tr in TableParser(html).trs]
    return clubs

def get_stream_urls(html: str, url: str) -> Union[List[str], None]:
    soup = BeautifulSoup(html, 'lxml')
    page_list = soup.find('ul', class_="tm-pagination")
    if page_list is None:
        return None
    else:
        last_page = int(page_list.find_all('li')[-1].find('a').get('href')[-1])
        stream_urls = [url + f"page/{i}" for i in range(2, (last_page+1))]
        return stream_urls

async def extract_stream(s: ClientSession, club: ClubIncomeParser) -> List[ClubStreamParser]:
    html = await get_html(s, club.stream_url)
    rem_stream_urls = get_stream_urls(html, club.stream_url)
    if rem_stream_urls:
        stream_htmls = [await get_html(s, url) for url in rem_stream_urls]
        stream_htmls.append(html)
        stream_trs = [TableParser(stream_html).trs for stream_html in stream_htmls]
        club_stream = [ClubStreamParser(tr, club.name) for tr in chain(*stream_trs)]
        return club_stream
    else:
        stream_parser = TableParser(html)
        club_stream = [ClubStreamParser(tr, club.name) for tr in stream_parser.trs]
        return list(club_stream)

def load_all(clubs: List[ClubIncomeParser], streams: List[ClubStreamParser]) -> None:
    with GraphDatabase.driver() as driver:
        with driver.session() as session:
            session.write_transaction(clear_db)
            session.write_transaction(set_constraints)
            for club in clubs:
                session.write_transaction(write_club, club)
            for stream in streams:
                session.write_transaction(write_stream, stream)
            session.write_transaction(remove_loans)
            session.write_transaction(remove_orphan_clubs)

async def main():
    async with ClientSession() as s:
        clubs = await asyncio.gather(*[extract_clubs(s, url) for url in get_income_urls()])
        club_chain = list(chain(*clubs))
        stream_tasks= [extract_stream(s, club) for club in club_chain]
        streams = await asyncio.gather(*stream_tasks)
        stream_chain = list(chain(*streams))
        load_all(club_chain, stream_chain)
        
if __name__ == "__main__":
    asyncio.run(main())

