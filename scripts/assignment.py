# coding: utf-8
from scripts.helpers import *


def query_01(connection, column_names):
    # Bouw je query
    query = """
    select t.name, t.yearID, t.HR
    from Teams as t
    order by t.HR DESC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_02(connection, column_names, datum_x='1980-01-16'):
    # Bouw je query
    query = """
    select m.nameFirst, m.nameLast, m.birthYear, m.birthMonth, m.birthDay
    from Master as m
    where m.debut >= {}
    order by m.nameLast;
    """.format(datum_x)

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_03(connection, column_names):
    # Bouw je query
    query = """
    select distinct ma.nameFirst, ma.nameLast, t.name
    from Managers as m
    join Teams as t on t.teamID = m.teamID and t.yearID = m.yearID
    join Master as ma on m.playerID = ma.playerID
    where m.plyrMgr = 'N'
    order by t.name ASC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_04(connection, column_names, league_l='AL', jaar_x=1980, jaar_y=1990):
    # Bouw je query
    query = """
    select t.teamID, t.name, t.yearID, t.W, t.L
    from Teams as t
    where t.lgID = '{}' and t.yearID > {} and t.teamID not in 
                                                    (select t_.teamID 
                                                     from Teams as t_ 
                                                     where t_.yearID < {})
    order by t.teamID, t.yearID ASC;
    """.format(league_l, jaar_y, jaar_x)

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_05(connection, column_names):
    # Bouw je query
    query = """
    select ma.playerID, ma.nameGiven, count(ap.awardID)
    from Master as ma
    join AwardsManagers as am on ma.playerID = am.playerID
    join AwardsPlayers  as ap on ma.playerID = ap.playerID
    group by ma.playerID
    order by count(ap.awardID) DESC, ma.playerID DESC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_06(connection, column_names):
    # Bouw je query
    query = """
    select distinct ma.playerID
    from Master as ma
    join AwardsPlayers as ap on ap.playerID = ma.playerID
    where exists (select aa.playerID
                  from AwardsPlayers as aa
                  where aa.playerID = ma.playerID and aa.yearID = ap.yearID + 1)
      and exists (select ab.playerID
                  from AwardsPlayers as ab
                  where ab.playerID = ma.playerID and ab.yearID = ap.yearID + 2)
    order by ma.playerID;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_07(connection, column_names, jaar_y=1980, manager_x='joske'):
    # Bouw je query
    query = """
    select distinct ma.playerID, ma.nameFirst, ma.nameLast
    from Master as ma
    join AwardsPlayers as ap on ap.playerID = ma.playerID
    join Appearances as aa on aa.playerID = ma.playerID
    join Managers as ms on ms.teamID = aa.teamID and ms.yearID = aa.yearID
    where ap.yearID = {} and ms.playerID = '{}'
    order by ma.playerID asc;
    """.format(jaar_y, manager_x)

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_08(connection, column_names):
    # Bouw je query
    query = """
    select ma.playerID, ma.nameFirst, ma.nameLast
    from Master as ma
         join Salaries as sa on sa.playerID = ma.playerID
    group by ma.playerID
    having AVG(sa.salary) > (select AVG(sa2.salary) from Salaries as sa2)
    order by ma.playerID asc;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


# Test
query_08(c, col_names['query_08'])


def query_09(connection, column_names):
    # Bouw je query
    query = """
    select t.teamID, t.name, t.yearID, t.W
    from Teams as t
    where t.yearID = 
                    (select MIN(t2.yearID) 
                     from Teams as t2 
                     where t.teamID = t2.teamID and t2.W = 
                                                            (select MAX(t3.W) 
                                                             from Teams as t3
                                                             where t2.teamID = t3.teamID))
    order by t.W desc, t.teamID desc;
    """

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


def query_10(connection, column_names, jaar_y=1990):
    # Bouw je query
    query = """
    select distinct t.teamID, t.name
    from Teams as t
        join Salaries as s on t.teamID = s.teamID and t.yearID = s.yearID
    where s.salary > 0 and t.yearID = {} and (select count(*) 
                                              from AwardsPlayers as a
                                              where a.playerID = s.playerID and t.yearID = a.yearID) = 0
    order by t.teamID asc;
    """.format(jaar_y)

    # Stap 2 & 3
    res = run_query(connection, query)  # Query uitvoeren
    df = res_to_df(res, column_names)  # Query in DataFrame brengen

    return df


if __name__ == '__main__':
    query_01(c, col_names['query_01'])
    query_02(c, col_names['query_02'])
    query_03(c, col_names['query_03'])
    query_04(c, col_names['query_04'])
    query_05(c, col_names['query_05'])
    query_06(c, col_names['query_06'])
    query_07(c, col_names['query_07'])
    query_08(c, col_names['query_08'])
    query_09(c, col_names['query_09'])
    query_10(c, col_names['query_10'])


# 4. Als je oplossing definitief is, submit je je `dd_X_groep_YY.py` via Toledo.
#
#
# 5. Nogmaals, als finale submissie verwachten we dus een python bestand (e.g., `wo_1_groep_03.py`) dat jullie ingevulde functies bevat en niks anders. De ingevulde notebook is niet acceptabel.
