# coding: utf-8

def query_01(connection, column_names):

    # Bouw je query
    query="""
    select t.name, t.yearID, t.HR
    from Teams as t
    order by t.HR DESC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_02(connection, column_names, datum_x='1980-01-16'):

    # Bouw je query
    query="""
    select m.nameFirst, m.nameLast, m.birthYear, m.birthMonth, m.birthDay
    from Master as m
    where m.debut >= '""" + datum_x + """'
    order by m.nameLast;
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_03(connection, column_names):

    # Bouw je query
    query="""
    select distinct ma.nameFirst, ma.nameLast, t.name
    from Managers as m
    join Teams as t on t.teamID = m.teamID and t.yearID = m.yearID
    join Master as ma on m.playerID = ma.playerID
    where m.plyrMgr = 'N'
    order by t.name ASC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_04(connection, column_names, league_l='AL', jaar_x=1980, jaar_y=1990):

    # Bouw je query
    query="""
    select t.teamID, t.name, t.yearID, t.W, t.L
    from Teams as t
    where t.lgID = '""" + league_l + """' and t.yearID > """ +
        str(jaar_y) + """ and t.teamID not in (select t_.teamID from Teams as t_ where t_.yearID < """ + str(jaar_x) + """)
    order by t.teamID, t.yearID ASC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_05(connection, column_names):

    # Bouw je query
    query="""
    select ma.playerID, ma.nameGiven, count(ap.awardID)
    from Master as ma
    join AwardsManagers as am on ma.playerID = am.playerID
    join AwardsPlayers  as ap on ma.playerID = ap.playerID
    group by ma.playerID
    order by count(ap.awardID) DESC, ma.playerID DESC;
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_06(connection, column_names):

    # Bouw je query
    query="""
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
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_07(connection, column_names, jaar_y=1980, manager_x='joske'):

    # Bouw je query
    query="""
    select distinct ma.playerID, ma.nameFirst, ma.nameLast
    from Master as ma
    join AwardsPlayers as ap on ap.playerID = ma.playerID
    join Appearances as aa on aa.playerID = ma.playerID
    join Managers as ms on ms.teamID = aa.teamID and ms.yearID = aa.yearID
    where ap.yearID = """ + str(jaar_y) + """ and ms.playerID = '""" + manager_x + """'
    order by ma.playerID asc;
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df

def query_08(connection, column_names):

    # Bouw je query
    query="""
    MAAK QUERY HIER
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_09(connection, column_names):

    # Bouw je query
    query="""
    MAAK QUERY HIER
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


def query_10(connection, column_names, jaar_y=1990):

    # Bouw je query
    query="""
    MAAK QUERY HIER
    """

    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen

    return df


# 4. Als je oplossing definitief is, submit je je `dd_X_groep_YY.py` via Toledo.
#
#
# 5. Nogmaals, als finale submissie verwachten we dus een python bestand (e.g., `wo_1_groep_03.py`) dat jullie ingevulde functies bevat en niks anders. De ingevulde notebook is niet acceptabel.
