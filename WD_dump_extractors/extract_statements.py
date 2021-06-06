# -*- coding: utf-8 -*-
import os
import sys
import re
import pandas as pd
# reload(sys)
# sys.setdefaultencoding("utf8")
import ujson
import psycopg2
import bz2


import itertools
import html
import unicodedata
from html.parser import HTMLParser
from shutil import copyfileobj
from numba import jit, autojit
import logging

DB_USERNAME = os.getenv('POSTGRES_NAME')
DB_PASSWD = os.getenv('POSTGRES_PWD')

# connection parameters
def get_db_params():
    params = {
        'database': 'wikidb',
        'user': DB_USERNAME,
        'password': DB_PASSWD,
        'host': 'localhost',
        'port': '5432'
    }
    conn = psycopg2.connect(**params)
    return conn


# create table
def create_table():
    ###statement table query
    query_table_1 = """
    		CREATE TABLE IF NOT EXISTS revisionData (
    			itemId VARCHAR(255),
    			revId VARCHAR(255) PRIMARY KEY,
    			parId VARCHAR(255),
                timeStamp VARCHAR(255),
                userName VARCHAR(255)
                )
    		"""

    ###statement table query
    query_table_2 = """
		CREATE TABLE IF NOT EXISTS statementsData (
		    itemId VARCHAR(255),
		    revId VARCHAR(255),
			statementId VARCHAR(255),
			statProperty VARCHAR(255),
            statValue VARCHAR(500),
            statRank VARCHAR(255),
            statType VARCHAR(255),
            PRIMARY KEY(revId, statementId)
		)
		"""
    ###reference table query
    query_table_3 = """
		CREATE TABLE IF NOT EXISTS referenceData (
		    serialId SERIAL,
		    revId VARCHAR(255),
			statementId VARCHAR(255),
			referenceId VARCHAR(255),
            refProperty VARCHAR(255),
            refType VARCHAR(255),
            refValue VARCHAR(500),
            PRIMARY KEY(serialId, referenceId)
		)
		"""
    ###qualifier table query
    query_table_4 = """
		CREATE TABLE IF NOT EXISTS qualifierData (
		    serialId SERIAL,
		    revId VARCHAR(255),
			statementId VARCHAR(255),
			qualifierId VARCHAR(255),
            qualProperty VARCHAR(255),
            qualType VARCHAR(255),
            qualValue VARCHAR(500),
            PRIMARY KEY(serialId, qualifierId)
		)
		"""

    query_list = [query_table_1, query_table_2, query_table_3, query_table_4]

    conn = None

    try:
        conn = get_db_params()
        cur = conn.cursor()
        for query in query_list:
            cur.execute(query)
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

#Delete duplicates

# def cleanDuplicates():
#
#     statementQuery = """
#     DELETE FROM statementsData WHERE revId IN (SELECT revId FROM (SELECT revId, ROW_NUMBER() OVER( PARTITION BY statementId, statProperty, statValue ORDER BY revId ) AS row_num FROM statementsData ) t WHERE t.row_num > 1 )
#     """
#
#     referenceQuery = """
#     DELETE FROM referenceData WHERE revId IN ( SELECT revId FROM (SELECT revId, ROW_NUMBER() OVER( PARTITION BY referenceId, statementId, refProperty, refValue ORDER BY revId ) AS row_num FROM referenceData ) t WHERE t.row_num > 1 )
#     """
#
#     qualifierQuery = """
#     DELETE FROM qualifierData WHERE revId IN (SELECT revId FROM (SELECT revId, ROW_NUMBER() OVER( PARTITION BY statementId, qualifierId, qualProperty, qualValue ORDER BY revId ) AS row_num FROM qualifierData ) t WHERE t.row_num > 1 )
#     """
#     query_list = [statementQuery, referenceQuery, qualifierQuery]
#     conn = None
#
#
#     conn = get_db_params()
#     cur = conn.cursor()
#
#     for query in query_list:
#         try:
#             cur.execute(query)
#         except (Exception, psycopg2.DatabaseError) as error:
#             print(error)
#
#     cur.close()
#     conn.commit()
#
#     if conn is not None:
#         conn.close()

# def get_max_rows(df):
#     B_maxes = df.groupby(['statementId', 'statValue']).revId.transform(min)
#     return df[df.revId == B_maxes]

def get_max_rows(df):
    B_maxes = df.groupby(['statementId', 'statValue']).revId.transform(min) == df['revId']
    return df[B_maxes]

def get_max_rowsQual(df):
    B_maxes = df.groupby(['qualId', 'qualProperty', 'qualValue']).revId.transform(min) == df.revId
    return df[B_maxes]

def get_max_rowsRef(df):
    B_maxes = df.groupby(['referenceId', 'refProperty', 'refValue']).revId.transform(min) == df.revId
    return df[B_maxes]

def getDeleted(dfStat, dfRev):

    # dfStat.revid = dfStat['revid'].astype('int')
    lastRev = dfStat.revId.max()
    dfRev.revId = dfRev['revId'].astype('int')
    itemId = dfStat['itemId'].unique()
    itemId = itemId[0]
    revList = dfRev[dfRev['itemId'] == itemId].revId.unique()
    revList = sorted(revList)

    try:
        position = revList.index(lastRev)

        if position != (len(revList) - 1):
            # deleted = False
            # print('not deleted')
        # else:
            # deleted = True
            # timeStamp = dfRev['timestamp'][dfRev['revid'] == revList[position+1]]
            statementId = dfStat['statementId'].unique()
            statementId = statementId[0]
            statproperty = dfStat['statProperty'].unique()
            statproperty = statproperty[0]
            stattype = dfStat['statType'].unique()
            stattype = stattype[0]
            # dictDel = {'itemId': itemId, 'revId': revList[position+1], 'statementId': statementId, 'statProperty': statproperty, 'statRank': 'normal', 'statType': stattype, 'statValue': 'deleted'}
            dictDel = {'revId': revList[position + 1], 'statementId': statementId, 'itemId': itemId,
                       'statType': stattype, 'statValue': 'deleted', 'statProperty': statproperty, 'statRank': 'normal'}
            # print('deleted')

            return dictDel

    except ValueError:
        print(lastRev, 'not in list; item ', itemId)

def getDeletedQual(dfStat, dfRev):

    # dfStat.revid = dfStat['revid'].astype('int')
    lastRev = dfStat.revId.max()
    dfRev.revId = dfRev['revId'].astype('int')
    itemId = dfStat['qualId'].unique()[0]
    itemId = re.search('[pP|qQ][0-9]{1,}', itemId).group(0)
    itemId = itemId.upper()
    revList = dfRev[dfRev['itemId'] == itemId].revId.unique()
    revList = sorted(revList)

    try:
        position = revList.index(lastRev)

        if position != (len(revList) - 1):
            # deleted = False
            # print('not deleted')
            # else:
            # deleted = True
            # timeStamp = dfRev['timestamp'][dfRev['revid'] == revList[position+1]]
            statementId = dfStat['statementId'].unique()
            statementId = statementId[0]
            qualId = dfStat['qualId'].unique()
            qualId = qualId[0]
            qualproperty = dfStat['qualProperty'].unique()
            qualproperty = qualproperty[0]
            qualtype = dfStat['qualType'].unique()
            qualtype = qualtype[0]
            dictDel = {'revId': revList[position + 1], 'qualId': qualId,
                       'qualProperty': qualproperty, 'qualType': qualtype, 'qualValue': 'deleted', 'statementId': statementId}
            # print('deleted')

            return dictDel

    except ValueError:
        print(lastRev, 'not in list; item ', itemId)

def getDeletedRef(dfStat, dfRev):

    # dfStat.revid = dfStat['revid'].astype('int')
    lastRev = dfStat.revId.max()
    dfRev.revId = dfRev['revId'].astype('int')
    itemId = dfStat['referenceId'].unique()[0]
    itemId = re.search('[pP|qQ][0-9]{1,}', itemId).group(0)
    itemId = itemId.upper()
    revList = dfRev[dfRev['itemId'] == itemId].revId.unique()
    revList = sorted(revList)

    try:
        position = revList.index(lastRev)

        if position != (len(revList) - 1):
            # deleted = False
            # print('not deleted')
            # else:
            # deleted = True
            # timeStamp = dfRev['timestamp'][dfRev['revid'] == revList[position+1]]
            statementId = dfStat['statementId'].unique()
            statementId = statementId[0]
            referenceid = dfStat['referenceId'].unique()
            referenceid = referenceid[0]
            refproperty = dfStat['refProperty'].unique()
            refproperty = refproperty[0]
            reftype = dfStat['refType'].unique()
            reftype = reftype[0]
            dictDel = {'revId': revList[position + 1], 'referenceId': referenceid,
                       'refProperty': refproperty, 'refType': reftype, 'refValue': 'deleted', 'statementId': statementId}
            # print('deleted')

            return dictDel

    except ValueError:
        print(lastRev, 'not in list; item ', itemId)

# This function cleans the item data

def h_parser(line):
    parsed_line = html.unescape(line)
    parsed_line = parsed_line.replace('<text xml:space="preserve">', '').replace('</text>', '').replace('\n',
                                                                                                        '').lstrip(' ')
    parsed_line = unicodedata.normalize('NFKD', parsed_line).encode('utf-8', 'ignore')

    return parsed_line


###Reference extraction
def ref_extract(j):
    ref_to_extract = {}

    ref_to_extract['refProperty'] = j['property']
    ref_to_extract['refType'] = j['datavalue']['type']

    if ref_to_extract['refType'] == 'string':
        ref_to_extract['refValue'] = j['datavalue']['value']
    elif ref_to_extract['refType'] == 'time':
        ref_to_extract['refValue'] = j['datavalue']['value']['time']
    elif ref_to_extract['refType'] == 'wikibase-entityid':
        try:
            ref_to_extract['refValue'] = j['datavalue']['value']['id']
        except KeyError:
            ref_to_extract['refValue'] = j['datavalue']['value']['numeric-id']
            ref_to_extract['refValue'] = 'Q' + str(ref_to_extract['refValue'])
    elif ref_to_extract['refType'] == 'monolingualtext':
        ref_to_extract['refValue'] = j['datavalue']['value']['text']
    elif ref_to_extract['refType'] == 'quantity':
        ref_to_extract['refValue'] = j['datavalue']['value']['amount']
    elif ref_to_extract['refType'] == 'globecoordinate':
        latitude = j['datavalue']['value']['latitude']
        longitude = j['datavalue']['value']['longitude']
        globe = j['datavalue']['value']['globe']
        altitude = j['datavalue']['value']['altitude']
        ref_to_extract['refValue'] = (str(latitude), str(longitude), str(globe), str(altitude))

    else:
        ref_to_extract['refValue'] = j['datavalue']['value']
    # if type(ref_to_extract['ref_value']) == 'dict':
    # 	ref_to_extract['ref_value'] = j['datavalue']['value']['text']
    # 	print ref_to_extract['ref_value']

    return ref_to_extract



def ref_loop(ref_node, refId, stat_id, revId):
    refe_results = [map(ref_extract, ref_node['snaks'][snak]) for snak in ref_node['snaks']]
    refe_results = list(itertools.chain.from_iterable(refe_results))

    refe_results = [dict(item, referenceId=refId) for item in refe_results]
    refe_results = [dict(item, statementId=stat_id) for item in refe_results]
    refe_results = [dict(item, revId=revId) for item in refe_results]

    return refe_results


# Qualifier extractor
def qual_extractor(qualifier, stat_id, idx, revId):
    dict_qual = {}
    qualId = str(stat_id) + '-' + str(idx)
    dict_qual['qualId'] = qualId
    dict_qual['statementId'] = stat_id
    dict_qual['revId'] = revId
    stat_value = None

    try:
        stat_property = qualifier['property']

        if qualifier['snaktype'] == 'value':

            try:
                stat_type = qualifier['datavalue']['type']
            except KeyError as k:
                #print(k)  # , text_wd['mainsnak']
                stat_type = None

            try:
                if stat_type == 'wikibase-entityid':
                    try:
                        stat_value = qualifier['datavalue']['value']['id']
                    except KeyError:
                        stat_value = qualifier['datavalue']['value']['numeric-id']
                        stat_value = 'Q%s' % stat_value

                elif stat_type == 'globecoordinate':
                    latitude = qualifier['datavalue']['value']['latitude']
                    longitude = qualifier['datavalue']['value']['longitude']
                    stat_value = (str(latitude), str(longitude))

                elif stat_type == 'time':
                    stat_value = qualifier['datavalue']['value']['time']

                elif stat_type == 'quantity':
                    stat_value = qualifier['datavalue']['value']['amount']

                elif stat_type == 'monolingualtext':
                    stat_value = qualifier['datavalue']['value']['text']

                else:
                    stat_value = qualifier['datavalue']['value']
                    if type(stat_value) == 'dict':
                        stat_value = qualifier['datavalue']['value']['text']
                        # print(dictStat['statValue'])

                dict_qual['qualProperty'] = stat_property
                dict_qual['qualType'] = stat_type
                dict_qual['qualValue'] = str(stat_value)

            except TypeError as e:
                print(e)
                # pass
            # , text_wd['mainsnak']['datavalue']

            except ValueError as e:
                print(e)
                # print(stat_value)
                # pass

            except KeyError as e:
                print(e)  # , text_wd['mainsnak']
                # pass

        else:
            dict_qual['qualProperty'] = stat_property
            dict_qual['qualType'] = qualifier['snaktype']
            dict_qual['qualValue'] = qualifier['snaktype']

    except IndexError:
        print('wrong', qualifier)

    except TypeError as e:
        print(e, qualifier)

    return dict_qual


###Statement extractor
def extr_statement(text_wd, itemId, revId):
    dictStat = {}

    dictStat['revId'] = revId
    dictStat['itemId'] = itemId
    dictStat['statementId'] = text_wd['id']
    dictStat['statProperty'] = text_wd['mainsnak']['property']
    dictStat['statRank'] = text_wd['rank']

    if text_wd['mainsnak']['snaktype'] == 'value':

        try:
            dictStat['statType'] = text_wd['mainsnak']['datavalue']['type']
        except KeyError as k:
            # print(k)  # , text_wd['mainsnak']
            dictStat['statType'] = None

        try:
            if dictStat['statType'] == 'wikibase-entityid':
                try:
                    dictStat['statValue'] = text_wd['mainsnak']['datavalue']['value']['id']
                except KeyError:
                    stat_value = text_wd['mainsnak']['datavalue']['value']['numeric-id']
                    dictStat['statValue'] = 'Q' + str(stat_value)

            elif dictStat['statType'] == 'globecoordinate':
                latitude = text_wd['mainsnak']['datavalue']['value']['latitude']
                longitude = text_wd['mainsnak']['datavalue']['value']['longitude']
                dictStat['statValue'] = (str(latitude), str(longitude))

            elif dictStat['statType'] == 'time':
                dictStat['statValue'] = text_wd['mainsnak']['datavalue']['value']['time']

            elif dictStat['statType'] == 'quantity':
                dictStat['statValue'] = text_wd['mainsnak']['datavalue']['value']['amount']

            elif dictStat['statType'] == 'monolingualtext':
                dictStat['statValue'] = text_wd['mainsnak']['datavalue']['value']['text']

            else:
                dictStat['statValue'] = text_wd['mainsnak']['datavalue']['value']
                if type(dictStat['statValue']) == 'dict':
                    dictStat['statValue'] = text_wd['mainsnak']['datavalue']['value']['text']
                    # print(dictStat['statValue'])

                dictStat['statValue'] = str(dictStat['statValue'])


        except TypeError as e:
            print(text_wd['mainsnak'])
            # pass
        # , text_wd['mainsnak']['datavalue']

        except ValueError as e:
            print(text_wd['mainsnak'])
            # print(dictStat['statValue'])
            # pass

        except KeyError as e:
            # print(text_wd['mainsnak'])  # , text_wd['mainsnak']
            pass

    else:
        dictStat['statType'] = text_wd['mainsnak']['snaktype']
        dictStat['statValue'] = text_wd['mainsnak']['snaktype']
        # print(text_wd['mainsnak']['snaktype'])

    if 'references' in text_wd:
        for idx, ref in enumerate(text_wd['references']):
            refId = dictStat['statementId'] + '-' + str(idx)
            ref_data = ref_loop(ref, refId, dictStat['statementId'], revId)

    else:
        ref_data = None

    if 'qualifiers' in text_wd:
        qual_data = []
        for key in text_wd['qualifiers']:
            # print(text_wd['qualifiers'][key])
            qual_single = [qual_extractor(el, text_wd['id'], idx, revId) for idx, el in enumerate(text_wd['qualifiers'][key])]
            qual_data += qual_single

            # qual_data = list(itertools.chain.from_iterable(qualifier_list))

    else:
        qual_data = None

    # print(dictStat)
    return dictStat, ref_data, qual_data


#     return dictStat


def extr_rev_data(revision, revId):
    try:
        itemId = revision['id']
        statement_data = []
        for key in revision['claims']:
            statementId = [extr_statement(el, itemId, revId) for el in revision['claims'][key]]
            statement_data += statementId

        return statement_data


    except KeyError as k:
        print(revision, k)
        # pass

    except TypeError as t:
        print(revision, revId, t)
        # pass


###extract file
def file_extractor(file_name):
    logging.basicConfig(filename='./errors.log', level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')
    logger = logging.getLogger(__name__)
    create_table()
    revision_processed = []
    new_counter = 0
    counter = 0
    revisionStore = []
    statementStore = []
    referenceStore = []
    qualifierStore = []
    procCounter = 0
    # counterImport = 0
    record = False
    revId = None
    parId = None
    timeStamp = None
    userName = None
    revi = False
    revDict = {}
    revMetadata = []

    with bz2.open(file_name, 'rt') as inputfile:
        for line in inputfile:

            if counter >= 350:
                print('350 items')
                # counterImport += 1
                dfRev = pd.DataFrame(revMetadata)

                revision_processed = list(filter(None, revision_processed))
                revision_processed_clean = list(itertools.chain.from_iterable(revision_processed))
                qualifier_all = [x[2] for x in revision_processed_clean]
                # revision_processed_clean = list(zip(*revision_processed_clean))
                revisionStore = revisionStore + revMetadata

                try:
                    # print(statement_all)

                    # statement_all = list(itertools.chain.from_iterable(revision_processed_clean[0]))
                    # statement_all = list(itertools.chain.from_iterable(statement_all))
                    # statement_all = list(filter(None, statement_all))
                    #`


                    # statement_all = list(filter(None, revision_processed_clean[0]))
                    statement_all = [x[0] for x in revision_processed_clean]
                    statement_all = list(filter(None, statement_all))
                    revisionDf = pd.DataFrame(statement_all)
                    revisionDf.statementId = revisionDf['statementId'].astype('category')
                    revisionDf.revId = revisionDf['revId'].astype('int')
                    revisionDf.itemId = revisionDf['itemId'].astype('category')
                    uniStats = get_max_rows(revisionDf)
                    dicto = uniStats.to_dict('records')

                    delStats = revisionDf.groupby('statementId').apply(getDeleted, dfRev)
                    delStats = list(filter(None, list(delStats)))
                    for x in delStats:
                        x['revId'] = int(x['revId'])
                    statement_all = dicto + delStats
                    statementStore = statementStore + statement_all
                    print('new statement df')
                        # break

                    references_all = [x[1] for x in revision_processed_clean]
                    # references_all = list(filter(None, revision_processed_clean[1]))
                    references_all = list(filter(None, references_all))
                    references_all = list(itertools.chain.from_iterable(references_all))
                    # print(references_all)
                    revisionDf = pd.DataFrame(references_all)
                    revisionDf.referenceId = revisionDf['referenceId'].astype('category')
                    revisionDf.revId = revisionDf['revId'].astype('int')
                    uniStats = get_max_rowsRef(revisionDf)
                    dicto = uniStats.to_dict('records')

                    delStats = revisionDf.groupby('referenceId').apply(getDeletedRef, dfRev)
                    delStats = list(filter(None, list(delStats)))
                    for x in delStats:
                        x['revId'] = int(x['revId'])
                    references_all = dicto + delStats
                    print('new statement df refs')
                    referenceStore = referenceStore + references_all


                        # break
                    if any(v is not None for v in qualifier_all):  # revision_processed_clean[2].count(None) == len(revision_processed_clean[2]):
                    #     pass
                    # else:

                        qualifier_all = list(filter(None, qualifier_all))
                        qualifier_all = list(itertools.chain.from_iterable(qualifier_all))
                        revisionDf = pd.DataFrame(qualifier_all)
                        revisionDf.qualId = revisionDf['qualId'].astype('category')
                        revisionDf.revId = revisionDf['revId'].astype('int')
                        uniStats = get_max_rowsQual(revisionDf)
                        dicto = uniStats.to_dict('records')

                        delStats = revisionDf.groupby('qualId').apply(getDeletedQual, dfRev)
                        delStats = list(filter(None, list(delStats)))
                        for x in delStats:
                            x['revId'] = int(x['revId'])
                        qualifier_all = dicto + delStats
                        print('new statement df quals')
                        qualifierStore = qualifierStore + qualifier_all



                            # print(qualifier_all)

                except IndexError as ie:
                    print(ie, revision_processed_clean)
                    # break

                revision_processed = []
                revision_processed_clean =[]
                revMetadata = []
                statement_all = []
                dicto =[]
                uniStats = []
                references_all = []
                qualifier_all = []
                new_counter += counter
                procCounter += 1
                counter = 0
                dfRev = pd.DataFrame()
                # break

            if procCounter == 40:

                conn = get_db_params()
                cur = conn.cursor()
                try:
                    cur.executemany(
                        """INSERT INTO revisionData (itemId, parId, revId, timeStamp, userName) VALUES (%(itemId)s, %(parId)s, %(revId)s, %(timeStamp)s, %(userName)s);""",
                        revisionStore)
                    conn.commit()
                    # print('imported')
                except:
                    conn.rollback()
                    for stat in revisionStore:
                        try:
                            cur.execute(
                                """INSERT INTO revisionData (itemId, parId, revId, timeStamp, userName) VALUES (%(itemId)s, %(parId)s, %(revId)s, %(timeStamp)s, %(userName)s);""",
                                stat)
                            conn.commit()
                        except:
                            conn.rollback()
                            e = sys.exc_info()[0]
                            print("<p>Error: %s</p>" % e)
                            print('not imported, revision id error')
                            print(stat)

                try:
                    # conn = get_db_params()
                    # cur = conn.cursor()
                    cur.executemany(
                        """INSERT INTO statementsData (itemId, revId, statementId, statProperty, statRank, statType, statValue) VALUES (%(itemId)s, %(revId)s, %(statementId)s, %(statProperty)s, %(statRank)s, %(statType)s, %(statValue)s);""",
                        statementStore)
                    conn.commit()
                    # print('imported')
                except:
                    conn.rollback()
                    for stat in statementStore:
                        try:
                            cur.execute(
                                """INSERT INTO statementsData (itemId, revId, statementId, statProperty, statRank, statType, statValue) VALUES (%(itemId)s, %(revId)s, %(statementId)s, %(statProperty)s, %(statRank)s, %(statType)s, %(statValue)s);""",
                                stat)
                            # print(stat)
                            conn.commit()
                        except:
                            conn.rollback()
                            # e = sys.exc_info()[0]
                            # print("<p>Error: %s</p>" % e)
                            # print('not imported')
                            # print(stat)
                            logger.exception(stat)

                try:

                    cur.executemany(
                        """INSERT INTO referenceData (referenceId, refProperty, refType, refValue, revId, statementId) VALUES (%(referenceId)s, %(refProperty)s, %(refType)s, %(refValue)s, %(revId)s, %(statementId)s);""",
                        referenceStore)
                    conn.commit()
                    # print('references imported')
                except:
                    conn.rollback()
                    for ref in referenceStore:
                        try:
                            cur.execute(
                                """INSERT INTO referenceData (referenceId, refProperty, refType, refValue, revId, statementId) VALUES (%(referenceId)s, %(refProperty)s, %(refType)s, %(refValue)s, %(revId)s, %(statementId)s);""",
                                ref)
                            conn.commit()
                        except:
                            conn.rollback()
                            e = sys.exc_info()[0]
                            print("<p>Error: %s</p>" % e)
                            print('not imported')
                            print(ref)
                            # break

                try:
                    cur.executemany(
                        """INSERT INTO qualifierData (qualifierId, qualProperty, qualType, qualValue, revId, statementId) VALUES (%(qualId)s, %(qualProperty)s, %(qualType)s, %(qualValue)s, %(revId)s, %(statementId)s);""",
                        qualifierStore)
                    conn.commit()
                    # print('qualifiers imported')
                except:
                    conn.rollback()
                    for qual in qualifierStore:
                        try:
                            cur.execute(
                                """INSERT INTO qualifierData (qualifierId, qualProperty, qualType, qualValue, revId, statementId) VALUES (%(qualId)s, %(qualProperty)s, %(qualType)s, %(qualValue)s, %(revId)s, %(statementId)s);""",
                                qual)
                            conn.commit()
                        except:
                            conn.rollback()
                            e = sys.exc_info()[0]
                            print("<p>Error: %s</p>" % e)
                            print('not imported')
                            print(qual)
                            # break

                revisionStore = []
                statementStore = []
                referenceStore =[]
                qualifierStore = []
                procCounter = 0
                print(new_counter, ' statements imported!')

            if '<title>' in line:
                itemId = line
                itemId = itemId.lstrip()
                itemId = itemId.replace('<title>', '')
                itemId = itemId.replace('</title>', '')
                itemId = itemId.rstrip()
                itemId = itemId.replace('Property:', '')
                if re.match('[PQ][0-9]{1,}', itemId):
                    counter += 1
                    record = True
                    itemSaved = itemId

            if '<revision>' in line and record:
                revi = True

            if ('<id>' in line and revi is True) and record:
                revId = line
                revId = revId.lstrip()
                revId = revId.replace('<id>', '')
                revId = revId.replace('</id>', '')
                revId = revId.rstrip()
                revDict['revId'] = revId
                revId = None
                revi = False

            if '<parentid>' in line and record:
                parId = line
                parId = parId.lstrip()
                parId = parId.replace('<parentid>', '')
                parId = parId.replace('</parentid>', '')
                parId = parId.rstrip()
                revDict['parId'] = parId
                parId = None

            if '<timestamp>' in line and record:
                timeStamp = line.replace('\t', '')
                timeStamp = timeStamp.replace('\n', '')
                timeStamp = timeStamp.replace('T', ' ')
                timeStamp = timeStamp.replace('Z', '')
                timeStamp = re.sub(r'<timestamp>|</timestamp>', '', timeStamp)
                timeStamp = timeStamp.lstrip()
                timeStamp = timeStamp.rstrip()
                revDict['timeStamp'] = timeStamp
                timeStamp = None

            if '<username>' in line and record:
                userName = line
                userName = userName.lstrip()
                userName = re.sub(r'<username>|</username>', '', userName)
                userName = userName.rstrip()
                revDict['userName'] = userName
                userName = None

                revDict['itemId'] = itemSaved
                if 'parId' not in revDict.keys():
                    revDict['parId'] = 'None'
                revMetadata.append(revDict)

            elif '<ip>' in line and record:
                userName = line
                userName = userName.lstrip()
                userName = re.sub(r'<ip>|</ip>', '', userName)
                revDict['userName'] = userName
                userName = None

                revDict['itemId'] = itemSaved
                if 'parId' not in revDict.keys():
                    revDict['parId'] = 'None'
                revMetadata.append(revDict)


            if '<text xml:space="preserve">' in line and record:

                parsed_line = h_parser(line)
                try:
                    parsed_json = ujson.loads(parsed_line)

                    rev_process = extr_rev_data(parsed_json, revDict['revId'])
                    revision_processed.append(rev_process)

                except ValueError as e:
                    # print(e)
                    # print(parsed_line)
                    # revDict = {}
                    pass
                except KeyError:
                    print(revDict)
                finally:
                    revDict = {}

            if '</page>' in line:
                record = False


            # counter += 1
            # if counterImport == 3:
            #     cleanDuplicates()
            #     print('Duplicated cleaned')
            #     counterImport = 0

            # continue


        revision_processed = list(filter(None, revision_processed))
        revision_processed_clean = list(itertools.chain.from_iterable(revision_processed))
        # revision_processed_clean = list(zip(*revision_processed_clean))
        qualifier_all = [x[2] for x in revision_processed_clean]

        dfRev = pd.DataFrame(revMetadata)

        try:
            # print(statement_all)

            # statement_all = list(itertools.chain.from_iterable(revision_processed_clean[0]))
            # statement_all = list(itertools.chain.from_iterable(statement_all))
            # statement_all = list(filter(None, statement_all))
            #
            conn = get_db_params()
            cur = conn.cursor()
            try:
                cur.executemany(
                    """INSERT INTO revisionData (itemId, parId, revId, timeStamp, userName) VALUES (%(itemId)s, %(parId)s, %(revId)s, %(timeStamp)s, %(userName)s);""",
                    revMetadata)
                conn.commit()
                # print('imported')
            except:
                conn.rollback()
                for stat in revMetadata:
                    try:
                        cur.execute(
                            """INSERT INTO revisionData (itemId, parId, revId, timeStamp, userName) VALUES (%(itemId)s, %(parId)s, %(revId)s, %(timeStamp)s, %(userName)s);""",
                            stat)
                        conn.commit()
                    except:
                        conn.rollback()
                        e = sys.exc_info()[0]
                        print("<p>Error: %s</p>" % e)
                        print('not imported, revision id error')
                        print(stat)


            # statement_all = list(filter(None, revision_processed_clean[0]))
            statement_all = [x[0] for x in revision_processed_clean]
            statement_all = list(filter(None, statement_all))
            revisionDf = pd.DataFrame(statement_all)
            revisionDf.statementId = revisionDf['statementId'].astype('category')
            revisionDf.revId = revisionDf['revId'].astype('int')
            revisionDf.itemId = revisionDf['itemId'].astype('category')
            uniStats = get_max_rows(revisionDf)
            dicto = uniStats.to_dict('records')
            print('duplicates removed')

            delStats = revisionDf.groupby('statementId').apply(getDeleted, dfRev)
            delStats = list(filter(None, list(delStats)))
            for x in delStats:
                x['revId'] = int(x['revId'])
            print('deleted statements added')
            statement_all = dicto + delStats
            print('new statement df')
            conn = get_db_params()
            cur = conn.cursor()


            try:

                cur.executemany(
                    """INSERT INTO statementsData (itemId, revId, statementId, statProperty, statRank, statType, statValue) VALUES (%(itemId)s, %(revId)s, %(statementId)s, %(statProperty)s, %(statRank)s, %(statType)s, %(statValue)s);""",
                    statement_all)
                conn.commit()
                print('imported')
            except:
                conn.rollback()
                for stat in statement_all:
                    try:
                        cur.execute(
                            """INSERT INTO statementsData (itemId, revId, statementId, statProperty, statRank, statType, statValue) VALUES (%(itemId)s, %(revId)s, %(statementId)s, %(statProperty)s, %(statRank)s, %(statType)s, %(statValue)s);""",
                            stat)
                        conn.commit()
                    except:
                        conn.rollback()
                        e = sys.exc_info()[0]
                        print("<p>Error: %s</p>" % e)
                        print('not imported')
                        logging.exception(stat)
                        #print(stat)
                        # break

            references_all = [x[1] for x in revision_processed_clean]
            # references_all = list(filter(None, revision_processed_clean[1]))
            references_all = list(filter(None, references_all))
            references_all = list(itertools.chain.from_iterable(references_all))
            # print(references_all)
            revisionDf = pd.DataFrame(references_all)
            revisionDf.referenceId = revisionDf['referenceId'].astype('category')
            revisionDf.revId = revisionDf['revId'].astype('int')
            uniStats = get_max_rowsRef(revisionDf)
            dicto = uniStats.to_dict('records')
            print('duplicates removed ref')

            delStats = revisionDf.groupby('referenceId').apply(getDeletedRef, dfRev)
            delStats = list(filter(None, list(delStats)))
            for x in delStats:
                x['revId'] = int(x['revId'])
            print('deleted refs added')
            references_all = dicto + delStats
            print('new statement df refs')

            try:
                cur.executemany(
                    """INSERT INTO referenceData (referenceId, refProperty, refType, refValue, revId, statementId) VALUES (%(referenceId)s, %(refProperty)s, %(refType)s, %(refValue)s, %(revId)s, %(statementId)s);""",
                    references_all)
                conn.commit()
                print('imported')
            except:
                conn.rollback()
                for ref in references_all:
                    try:
                        cur.execute(
                            """INSERT INTO referenceData (referenceId, refProperty, refType, refValue, revId, statementId) VALUES (%(referenceId)s, %(refProperty)s, %(refType)s, %(refValue)s, %(revId)s, %(statementId)s);""",
                            ref)
                        conn.commit()
                    except:
                        conn.rollback()
                        e = sys.exc_info()[0]
                        print("<p>Error: %s</p>" % e)
                        print('not imported')
                        print(ref)
                        # break

            if any(v is None for v in qualifier_all):  # revision_processed_clean[2].count(None) == len(revision_processed_clean[2]):
                pass
            else:
                # qualifier_all = list(filter(None, revision_processed_clean[2]))
                # qualifier_all = list(itertools.chain.from_iterable(qualifier_all))
                # qualifier_all = list(filter(None, revision_processed_clean[2]))
                # qualifier_all = list(itertools.chain.from_iterable(qualifier_all))
                qualifier_all = list(filter(None, qualifier_all))
                qualifier_all = list(itertools.chain.from_iterable(qualifier_all))
                revisionDf = pd.DataFrame(qualifier_all)
                revisionDf.qualId = revisionDf['qualId'].astype('category')
                revisionDf.revId = revisionDf['revId'].astype('int')
                uniStats = get_max_rowsQual(revisionDf)
                dicto = uniStats.to_dict('records')
                print('duplicates removed qual')

                delStats = revisionDf.groupby('qualId').apply(getDeletedQual, dfRev)
                delStats = list(filter(None, list(delStats)))
                for x in delStats:
                    x['revId'] = int(x['revId'])
                print('deleted quals added')
                qualifier_all = dicto + delStats
                print('new statement df quals')

                try:
                    cur.executemany(
                        """INSERT INTO qualifierData (qualifierId, qualProperty, qualType, qualValue, revId, statementId) VALUES (%(qualId)s, %(qualProperty)s, %(qualType)s, %(qualValue)s, %(revId)s, %(statementId)s);""",
                        qualifier_all)
                    conn.commit()
                    print('imported')
                except:
                    conn.rollback()
                    for qual in qualifier_all:
                        try:
                            cur.execute(
                                """INSERT INTO qualifierData (qualifierId, qualProperty, qualType, qualValue, revId, statementId) VALUES (%(qualId)s, %(qualProperty)s, %(qualType)s, %(qualValue)s, %(revId)s, %(statementId)s);""",
                                qual)
                            conn.commit()
                        except:
                            conn.rollback()
                            e = sys.exc_info()[0]
                            print("<p>Error: %s</p>" % e)
                            print('not imported')
                            print(qual)
                            # break
                    #     return revision_processed


        except IndexError as ie:
            print(ie, revision_processed_clean)
            # break

        revision_processed = []
        counter = 0
        print('done!')
        # break


def main():
    file_extractor(sys.argv[1])


if __name__ == "__main__":
    main()
