from bs4 import BeautifulSoup
import re
import numpy as np
from dataclasses import dataclass

class TableParser:
    def __init__(self, html):
        self.trs = self.get_tablerows(html)

    def get_tablerows(self, html):
        soup = BeautifulSoup(html, 'lxml')
        table_rows = soup.find('table', class_='items').find('tbody').find_all('tr')
        return table_rows

class CurrencyFormatter:
    def format_dollars(self, dollars):
        if 'm' in dollars:
            million = float(re.sub(r'[^\d.]', '', dollars))
            return million
        elif 'Th' in dollars:
            hundred_th = float(re.sub(r'[^\d.]', '', dollars))
            return np.around(hundred_th / 1000, 2)
        else:
            return 0.0

@dataclass
class ClubIncomeParser(CurrencyFormatter):
    name: str
    country: str
    expenditure: float
    arrivals: int
    income: float
    departures: int
    balance: float
    stream_url: str
    top50: bool = True

    def __init__(self, tr):
        self.tds = tr.find_all('td')
        self.url_pattern = re.compile(r'(\/.+\/)transfers(\/verein\/\d{1,})\/')
        self.format_stream_url()
        self.format_club_info()

    def format_stream_url(self):
        href = self.tds[2].find('a').get('href')
        key_info = re.search(self.url_pattern, href)
        url_name = key_info.group(1)
        club_id = key_info.group(2)
        stream_url = 'https://www.transfermarkt.us' + url_name + 'transferstroeme' \
                        + club_id + '/saisonIdVon/2016/saisonIdBis/2021/zuab/zu/verein_id//plus/0/'
        self.stream_url = stream_url

    def format_club_info(self):
        self.name = self.tds[2].text.strip()
        self.country = self.tds[3].find('img').get('alt').strip()
        self.expenditure = self.format_dollars(self.tds[4].text.strip())
        self.arrivals = int(self.tds[5].text.strip())
        self.income = self.format_dollars(self.tds[6].text.strip())
        self.departures = int(self.tds[7].text.strip())
        self.balance = self.format_dollars(self.tds[8].text.strip())

@dataclass
class ClubStreamParser(CurrencyFormatter):
    arrived_at: str
    arrived_from: str
    transfers: int
    volume: float = 0
    academy: bool = False

    def __init__(self, tr, arrived_at):
        self.tds = tr.find_all('td')
        self.arrived_at = arrived_at
        self.format_club_stream()
        self.academy_check()

    def academy_check(self):
        arrat_split = self.arrived_at.split()
        arrat_filtered = ' '.join(list(filter(lambda x: len(x) > 3, arrat_split)))
        arrfrom_split = self.arrived_from.split()
        arrfrom_filtered = ' '.join(list(filter(lambda x: len(x) > 3, arrfrom_split)))
        if arrat_filtered == arrfrom_filtered:
            self.academy = True
        elif re.sub('Primavera', '', arrfrom_filtered) == arrat_filtered:
            self.academy = True
        
    def format_club_stream(self):
        self.arrived_from = self.tds[2].text.strip()
        self.transfers = int(self.tds[3].text.strip()) - int(self.tds[4].text.strip())
        if self.transfers:
            self.volume = self.format_dollars(self.tds[5].text.strip())
        


    
