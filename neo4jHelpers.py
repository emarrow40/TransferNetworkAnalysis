from tableParser import ClubIncomeParser, ClubStreamParser
from dataclasses import asdict

def clear_db(tx) -> None:
    query = """
        MATCH (n) DETACH DELETE n
    """
    tx.run(query)

def set_constraints(tx) -> None:
    query = """
        DROP CONSTRAINT constraint_691ce0ef IF EXISTS
    """
    tx.run(query)

def write_club(tx, club: ClubIncomeParser) -> None:
    query = """
        CREATE (:Club $clubparser)
    """
    clubparser_dict = asdict(club)
    clubparser_dict.pop('stream_url')
    tx.run(query, clubparser=clubparser_dict)

"""
name: str
country: str
expenditure: float
arrivals: int
income: float
departures: int
balance: float
top50: bool
stream_url: str
"""

def write_stream(tx, stream: ClubStreamParser) -> None:
    query = """
        MATCH (c2:Club)
        WHERE c2.name = $arrived_at
        WITH c2
        MERGE (c1:Club {name: $arrived_from})
        ON CREATE
            SET c1.top50_academy = $academy
        MERGE (c1)-[:SOLD_TO {transfers: $transfers, volume: $volume}]->(c2)    
    """
    stream_dict = asdict(stream)
    tx.run(query, **stream_dict)

"""
arrived_at: str
arrived_from: str
transfers: int
volume: float
academy: bool = False
"""

def remove_loans(tx) -> None:
    query = """
        MATCH (c:Club)-[r:SOLD_TO]->()
        WHERE r.transfers = 0
        DELETE r
    """
    tx.run(query)

def remove_orphan_clubs(tx) -> None:
    query = """
        MATCH (c:Club)
        WHERE NOT (c)--() AND c.top50 IS NULL
        DELETE c
    """
    tx.run(query)