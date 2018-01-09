import logging, os, sys, getopt

from ConfigParser import SafeConfigParser
from bs4 import BeautifulSoup
import time
import datetime
from dateutil.parser import parse
import requests
import mysql.connector
from mysql.connector import errorcode

log = None
logLocation = ""

cfg = None
printXml = False

db = None

class LogDBHandler(logging.Handler):
    def __init__(self, sql_conn, sql_cursor, db_tbl_log):
        logging.Handler.__init__(self)
        self.sql_cursor = sql_cursor
        self.sql_conn = sql_conn
        self.db_tbl_log = db_tbl_log

    def emit(self, record):
        # Set current time
        tm = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))
        # Clear the log message so it can be put to db via sql (escape quotes)
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')
        # Make the SQL insert
        sql = ("INSERT INTO " + self.db_tbl_log + " (log_level, log_levelname, log, created_at, created_by) VALUES (%s, %s, %s, %s, %s)")
        data = (str(record.levelno), str(record.levelname), str(self.log_msg), tm, record.name)
        try:
            self.sql_cursor.execute(sql, data)
            self.sql_conn.commit()
        # If error - print it out on screen. Since DB is not working - there's
        # no point making a log about it to the database :)
        except mysql.connector.Error as e:
            print self.sql_cursor.statement
            print 'CRITICAL DB ERROR! Logging to database not possible!'

def initLogger(name, debug):
    global log
    global logLocation
    if logLocation == "":
        logLocation = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
    logging.basicConfig(filename=logLocation + "/" + name + ".log", level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger(__name__)
    soh = logging.StreamHandler(sys.stdout)
    soh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    if db != None:
        dbh = LogDBHandler(db, db.cursor(buffered=True),cfg.get("db", "logtable"))
        log.addHandler(dbh)
    log.addHandler(soh)
    if debug:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

def connectDb():
    global db
    dbuser = cfg.get("db", "username")
    dbpass = cfg.get("db", "password")
    dbhost = cfg.get("db", "hostname")
    dbdb = cfg.get("db", "database")

    try:
        db = mysql.connector.connect(user=dbuser, password=dbpass, host=dbhost, database=dbdb, buffered=True)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        elif err.errno == errorcode.CR_CONN_HOST_ERROR:
            print("Error connecting to database host")
        else:
            print("Oops!", sys.exc_info()[0], "occured.")
            print("Errorcode: " + str(err.errno) + " error: " + str(err.message))
        print("Error: " + str(err.msg))

class AccaEvent():
    @property
    def eventDate(self):
        return self._eventDate

    @property
    def eventName(self):
        return self._eventName

    @property
    def eventOutcome(self):
        return self._eventOutcome

    @property
    def odds(self):
        return self._odds

    @property
    def layOdds(self):
        return self._layOdds

    @property
    def commission(self):
        return self._commission

    @property
    def exchange(self):
        return self._exchange

    @property
    def layStake(self):
        return self._layStake

    @property
    def win(self):
        return self._win

    @property
    def legNum(self):
        return self._legNum

    @property
    def win(self):
        return self._win

    def __init__(self, legNum, date, name, outcome, exchange, odds, layodds, commission, actuallaystake, win):
        self.eventName = name
        self.eventDate = parse(date)
        self.legNum = legNum
        self.commission = commission
        self.odds = float(odds)
        self.layOdds = float(layodds)
        self.exchange = exchange
        self.eventOutcome = outcome
        self.layStake = float(actuallaystake)
        self.win = win


class AccaClass():
    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def url(self):
        return self._url

    @property
    def events(self):
        return self._events

    @property
    def isComplete(self):
        return self._isComplete

    @property
    def bookmaker(self):
        return self._bookmaker

    @property
    def saved(self):
        return self._saved

    @property
    def stake(self):
        return self._stake

    @property
    def lockIn(self):
        return self._lockIn

    @property
    def betType(self):
        return self._betType

    @property
    def bonus(self):
        return self._bonus

    @property
    def legs(self):
        return self._legs

    @property
    def bonusRetention(self):
        return self._bonusRetention

    def __init__(self, name, url = "", isComplete = False, stake = 0, bettype = "", bookmaker = "", refundamount = 0, bonus = False, bonusretention = 80):

        self.url = url
        self.name = name
        self.stake = stake
        self.bookmaker = bookmaker
        self.betType = bettype
        self.bonus = bonus
        self.bonusRetention = bonusretention
        self.refundAmount = refundamount
        self.events = []
        self.isComplete = isComplete

    def deleteAcca(self, session):
        request = None
        payload = {"acca_id": self.id, "delete":"Delete Acca"}
        retry = 0
        while request == None and retry <= 3:
            try:
                request = session.post(cfg.get("site", "deleteurl"), data=payload)
                log.info("Acca - " + self.name + "(Acca Id:" + self.id + ") successfully deleted...")
            except requests.ConnectionError as e:
                log.error("Unable to connect to - " + self.url)
                log.error("Error: " + str(e.message))
                request = None
                retry+=1
                log.info("Retrying attempt #" + str(retry) + "...")
            except Exception as e:
                log.debug("Oops!", sys.exc_info()[0], "occured.")
                log.error("Error: " + str(e.message))
                retry += 1
                log.info("Retrying attempt #" + str(retry) + "...")
        return request

    def getAccaFromDatabase(self):
        cursor = db.cursor()

        query = ("SELECT AccaName, "
                 "stake, "
                 "bookmaker, "
                 "bettype, "
                 "bonus, "
                 "bonusretention, "
                 "refundamount, "
                 "lockin, "
                 "complete FROM acca "
                 "WHERE accaname = %s")

        returnedAccas=[]

        try:
            cursor.execute(query, (str(self.name), ))
            log.info("Successfully retrieved Acca " + self.name + " details from database")
            log.debug("SQL: " + cursor.statement)
        except Exception as e:
            log.debug("Oops!", sys.exc_info()[0], "occured.")
            log.error("Error: " + str(e.msg))
        else:
            for (AccaName, stake, bookmaker, bettype, bonus, bonusretention, refundamount, lockin, complete) in cursor:
                returnedAccas.append(AccaClass(AccaName,"",complete,stake,bettype,bookmaker,refundamount,bonus,bonusretention))

        return returnedAccas

    def getAccaEventsFromDatabase(self):
        cursor = db.cursor()

        query = ("SELECT leg, "
                 "eventdate, "
                 "odds, "
                 "layodds, "
                 "commission, "
                 "actuallaystake, "
                 "win "
                 "from events "
                 "WHERE accaname = %s")

        returnedEvents = []

        try:
            cursor.execute(query, (str(self.name),))
            log.info("Successfully retrieved Events " + self.name + " from database for Acca " + self.name)
            log.debug("SQL: " + cursor.statement)
        except Exception as e:
            log.debug("Oops!", sys.exc_info()[0], "occured.")
            log.error("Error: " + str(e.msg))
        else:
            for (leg, date, odds, layodds, commission, actuallaystake, win) in cursor:
                returnedEvents.append(
                    AccaEvent(leg, str(date), "", "", "", odds, layodds, commission, actuallaystake, (True if win == 1 else False)))

        return returnedEvents

    def updateAcca(self, acca):
        log.info("Comparing Acca " + self.name)
        acca.events = self.getAccaEventsFromDatabase()
        accaEventQueries = []
        for eventNum in range(0, len(self.events)):
            eventQuery = "Update events set "
            addComma = False
            update = False
            layodds=0
            laystake=0
            odds=0
            win=0
            commission=0
            log.debug("Comparing Event Leg " + str(self.events[eventNum].legNum))
            if self.events[eventNum].layOdds == acca.events[eventNum].layOdds:
                log.debug("Lay Odds are the same")
            else:
                log.debug("Lay Odds - Site: " + str(self.events[eventNum].layOdds) + " db: " + str(acca.events[eventNum].layOdds))
                eventQuery = eventQuery + " layodds = '" + str(self.events[eventNum].layOdds) + "'"
                update = True
                addComma = True

            if self.events[eventNum].layStake == acca.events[eventNum].layStake:
                log.debug("Lay Stake is the same")
            else:
                log.debug("Lay Stake - Site: " + str(self.events[eventNum].layStake) + " db: " + str(acca.events[eventNum].layStake))
                if addComma:
                    eventQuery = eventQuery + " , "
                eventQuery = eventQuery + " actuallaystake = '" + str(self.events[eventNum].layStake) + "'"
                update = True
                addComma = True

            if self.events[eventNum].odds == acca.events[eventNum].odds:
                log.debug("Odds is the same")
            else:
                log.debug("Odds - Site: " + str(self.events[eventNum].odds) + " db: " + str(acca.events[eventNum].odds))
                if addComma:
                    eventQuery = eventQuery + " , "
                eventQuery = eventQuery + " odds = '" + str(self.events[eventNum].odds) + "'"
                update = True

            if self.events[eventNum].win == acca.events[eventNum].win:
                log.debug("Win is the same")
            else:
                log.debug("Win - Site: " + ("Yes" if self.events[eventNum].win else "No") + " db: " + ("Yes" if acca.events[eventNum].win else "No"))
                if addComma:
                    eventQuery = eventQuery + " , "
                eventQuery = eventQuery + " win = '" + ("1" if self.events[eventNum].win else "0") + "'"
                update = True

            if update:
                eventQuery = eventQuery + " where leg = " + str(self.events[eventNum].legNum) + " and accaname = '" + str(self.name) + "'"
                accaEventQueries.append(eventQuery)
                log.debug(eventQuery)

        if self.isComplete == acca.isComplete:
            log.debug("isComplete is the same")
        else:
            log.debug("Acca is complete? - Site: " + ("Yes" if self.isComplete else "No") + " db: " + ("Yes" if acca.isComplete else "No"))
            accaQuery = "update acca set complete = '" + ("1" if self.isComplete else "0") + "' where accaname = '" + self.name + "'"
            accaEventQueries.append(accaQuery)

        if len(accaEventQueries) > 0:
            cursor = db.cursor()
            try:
                for query in accaEventQueries:
                    cursor.execute(query)
                    log.debug("Running sql: " + cursor.statement)
                log.info(str(len(accaEventQueries)) + " row" + ("s" if len(accaEventQueries) > 1 else "") + " for Acca " + self.name + " updated in database")
            except Exception as e:
                db.rollback()
                log.debug("Oops!", sys.exc_info()[0], "occured.")
                log.error("Error: " + str(e.message))
            else:
                db.commit()
        else:
            log.info("No updates required")
        return


    def updateAccaDetails(self, session):
        request = None
        retry = 0
        while request == None and retry <= 3:
            try:
                request = session.get(self.url)
            except requests.ConnectionError as e:
                log.error("Unable to connect to - " + self.url)
                log.error("Error: " + str(e.message))
                request = None
                retry += 1
                log.info("Retrying attempt #" + str(retry) + "...")
            except Exception as e:
                log.debug("Oops!", sys.exc_info()[0], "occured.")
                log.error("Error: " + str(e.message))
                request = None
                retry += 1
                log.info("Retrying attempt #" + str(retry) + "...")

        if request != None:
            soup = BeautifulSoup(request.text, "lxml")
            if printXml:
                log.debug(soup.prettify())
            self.updateAccaId(soup)
            self.updateStake(soup)
            self.updateBetType(soup)
            self.updateBonus(soup)
            self.updateBonusRetention(soup)
            self.updateLockIn(soup)
            self.updateRefundAmount(soup)
            self.updateIsComplete(soup)
            self.updateLegs(soup)
            self.updateBookmaker(soup)
            try:
                self.updateEvents(soup)
            except Exception as e:
                log.warn("Unable to update events, skipping events for " + self.name)
        return

    def saveAccaToDatabase(self, user):
        cursor = db.cursor()

        addAcca = ("Insert into Acca (AccaName, userid, stake, bookmaker, bettype, bonus, bonusretention, refundamount, lockin, complete)"
                   " Values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

        accaData = (str(self.name), user, self.stake, self.bookmaker, self.betType, (1 if self.bonus else 0), self.bonusRetention, self.refundAmount, (1 if self.lockIn else 0), (1 if self.isComplete else 0))

        addEvents = ("insert into events (leg, accaname, eventname, outcome, odds, eventdate, layodds, commission, actuallaystake, win)" \
                     " Value ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        eventsData=[]
        for event in self.events:
            eventsData.append((event.legNum, str(self.name), event.eventName, event.eventOutcome, event.odds, event.eventDate, event.layOdds, event.commission, event.layStake, (1 if event.win else 0)))

        try:
            cursor.execute(addAcca, accaData)
            log.debug("Successfully Saved Acca " + self.name + " to database")
            log.debug("SQL: " + cursor.statement)
        except Exception as e:
            db.rollback()
            log.error("Oops!", sys.exc_info()[0], "occured.")
            log.debug("Error: " + str(e.msg))
        else:
            try:
                cursor.executemany(addEvents, eventsData)
                log.info("Successfully saved Acca details and events to database")
                log.debug("SQL: " + cursor.statement)
            except Exception as e:
                db.rollback()
                log.error("Oops!", sys.exc_info()[0], "occured.")
                log.debug("Error: " + str(e.msg))
                log.debug("Error: " + str(e.message))
            else:
                db.commit()
                return True
        return False

    def updateEvents(self, soup):

        eventTable = soup.find("table", id="tablePop")
        tableRows = eventTable.find_all("tr", {"id": True})
        cell = int(cfg.get("events", "startcell"))
        legNum = 1
        for row in tableRows:
            legNum = legNum
            log.debug("Event Leg: " + str(legNum))

            eventDate = row.find("input", {"id": cfg.get("events", "date")+str(cell)})['value']
            log.debug("Event Date: " + str(eventDate))

            event = row.find("input", {"id": cfg.get("events", "event")+str(cell)})['value']
            log.debug("Event: " + event)

            outcome = row.find("input", {"id": cfg.get("events", "outcome") + str(cell)})['value']
            log.debug("Outcome: " + outcome)

            exchange = row.find("input", {"id": cfg.get("events", "exchange")+str(cell)})['value']
            log.debug("Exchange: " + exchange)

            odds =row.find("input", {"id": cfg.get("events", "odds")+str(cell)})['value']
            log.debug("Odds: " + str(odds))

            layodds =row.find("input", {"id": cfg.get("events", "layodds")+str(cell)})['value']
            log.debug("Lay Odds: " + str(layodds))

            commission = row.find("input", {"id": cfg.get("events", "commission")+str(cell)})['value']
            log.debug("Commission: " + str(commission))

            win = row.find("input", {"id": cfg.get("events", "win") + str(cell)})['value']
            log.debug("Win: " + ("Yes" if win == "W" else "No"))

            laystake = row.find("input", {"id": cfg.get("events", "actuallaystake")+str(cell)})['value']
            log.debug("Lay Stake: " + str(laystake))

            event = AccaEvent(legNum,
                                eventDate,
                                event,
                                outcome,
                                exchange,
                                odds,
                                layodds,
                                commission,
                                laystake,
                                win = (True if win == "W" else False))
            cell += 1
            legNum += 1
            self.events.append(event)
            if legNum > self.legs:
                break;
        return



    def getCellResults(self, cell, soup):
        tag = soup.find_all(id=cell)
        log.debug("Number of tags for cell: " + cell + " - " + str(len(tag)))
        if (tag[0].has_attr('value')):
            result = tag[0]['value']
        else:
            log.debug("Cell: " + cell + " has no value attribute...")
            selectedOption = tag[0].find_all(attrs={'selected':True})
            if (len(selectedOption) == 0):
                log.debug("Bonus not selected, assuming No")
                result = 0
            else:
                log.debug("Cell: " + cell + " value = " + str(selectedOption[0].string))
                result = selectedOption[0]['value']
        return result

    def updateBookmaker(self, soup):
        bookmakerResults = soup.find("input", {"name": "bookie_name"})
        if bookmakerResults == None:
            log.warn("Acca - " + self.name + " has no bookmaker...")
        else:
            self.bookmaker = bookmakerResults['value']
            log.debug("Bookie Name: " + self.bookmaker)
        return

    def updateAccaId(self, soup):
        self.id = soup.find("input", {"name":"acca_id"})['value']
        log.debug("Acca Id: " + self.id)

    def updateIsComplete(self, soup):
        isCompleteTag = soup.find_all("input", {"name" : "is_completed"})
        if (isCompleteTag[0].has_attr('checked')):
            self.isComplete = True
        else:
            self.isComplete = False
        log.debug("Is Complete: " + ("Yes" if self.isComplete else "No"))

    def updateLegs(self, soup):
        legsCell = cfg.get("class", "legs")
        legs = self.getCellResults(legsCell, soup)
        if legs == "Single":
            self.legs = 1
        elif legs == "Double":
            self.legs = 2
        elif legs == "Triple":
            self.legs = 3
        elif legs == "Fourfold":
            self.legs = 4
        elif legs == "Fivefold":
            self.legs = 5
        elif legs == "Sixfold":
            self.legs = 6
        elif legs == "Sevenfold":
            self.legs = 7
        else:
            self.legs = 8

        log.debug("Legs: " + str(self.legs))

    def updateStake(self, soup):
        stakeCell = cfg.get("class", "stake")
        self.stake = self.getCellResults(stakeCell, soup)
        log.debug("Stake: " + str(self.stake))

    def updateBetType(self,soup):
        betTypeCell = cfg.get("class", "bettype")
        self.betType = self.getCellResults(betTypeCell, soup)
        log.debug("Bet Type: " + str(self.betType))

    def updateBonus(self, soup):
        bonusCell = cfg.get("class", "bonus")
        self.bonus = (True if self.getCellResults(bonusCell, soup) == "Yes" else False)
        log.debug("Bonus: " + ("Yes" if self.bonus else "No"))

    def updateBonusRetention(self, soup):
        bonusRetentionCell = cfg.get("class", "bonusretention")
        self.bonusRetention = self.getCellResults(bonusRetentionCell, soup)
        log.debug("Bonus Retention: " + str(self.bonusRetention))

    def updateRefundAmount(self, soup):
        refundAmountCell = cfg.get("class", "refundamount")
        self.refundAmount = self.getCellResults(refundAmountCell, soup)
        log.debug("Refund Amount: " + str(self.refundAmount))

    def updateLockIn(self,soup):
        lockInCell = cfg.get("class", "lockin")
        self.lockIn = (True if self.getCellResults(lockInCell, soup) == "1" else False)
        log.debug("Lock In: " + ("Yes" if self.lockIn == 1 else "No"))

class AccaGen():
    @property
    def loginUrl(self):
        return self._loginUrl

    @property
    def accaListUrl(self):
        return self._accaListUrl

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def session(self):
        return self._session

    @property
    def accaList(self):
        return self._accaList

    def __init__(self, loginUrl, accalistUrl, username, password):
        self.loginUrl = loginUrl
        self.accaListUrl = accalistUrl
        self.username = username
        self.password = password
        self.accaList = []
        self.createSession()
        self.loginToAccaGen()

    def createSession(self):
        self.session = requests.session()
        result = None
        try:
            result = self.session.get(self.loginUrl)
        except requests.RequestException as e:
            log.error("Error creating session, exiting... : ")
            log.error(str(e.message))
            sys.exit(1)
        return self.session

    def loginToAccaGen(self):
        result = None
        payload = {"email": self.username, "password": self.password}
        try:
            result = self.session.post(self.loginUrl, data=payload)
        except Exception as e:
            log.error("Exception has occurred during login attempt")
            log.debug("Error: " + str(e.message))
        if result == None:
            self.session = None
        return self.session

    # Check if Acca is in Database, if not save acca to database. If Acca is complete delete acca from AccaGen.
    # If Acca is saved in database, check if acca is marked as Complete, and delete if both Acca is complete and in database
    # If acca is not complete in database, compare data and update database if necessary. If acca is complete, delete acca.
    def checkAccaInDatabase(self, acca):
        accasInDB = acca.getAccaFromDatabase()
        if len(accasInDB) == 0:
            accaSaved = acca.saveAccaToDatabase(self.username)
            if acca.isComplete and accaSaved:
                log.debug("Delete acca " + acca.name)
                acca.deleteAcca(self.session)
        else:
            if len(accasInDB) > 1:
                log.info("More than 1 Acca found for " + acca.name + "...")
            if acca.isComplete and accasInDB[0].isComplete:
                log.debug("Delete Acca - " + acca.name)
                acca.deleteAcca(self.session)
            else:
                log.debug("Check acca (" + acca.name + ") for updates...")
                acca.updateAcca(accasInDB[0])


    def parseAcca(self, accas):
        accaTags = accas[0].find_all("a")
        for acca in accaTags:
            log.debug(acca.string + " - " + acca['href'])
            acca = AccaClass(acca.string, acca['href'])
            acca.updateAccaDetails(self.session)
            self.accaList.append(acca)
            if db != None:
                self.checkAccaInDatabase(acca)
        return

    def updateAccaList(self):
        result = None
        try:
            result = self.session.get(self.accaListUrl)
        except Exception as e:
            log.error("Oops!", sys.exc_info()[0], "occured.")
            log.debug("Error: " + str(e.message))
        else:
            accaSoup = BeautifulSoup(result.text, "lxml")
            log.debug("Active")
            self.parseAcca(accaSoup.find_all("div", {"class": "upcoming_events"}))
            log.debug("Finished")
            self.parseAcca(accaSoup.find_all("div", {"class": "completed_events colum2"}))

        return

    def loadAccaListFromDb(self):
        return 0

def main(argv):
    runAsService = False
    debug = False
    dbConnection = False
    global printXml

    try:
        opts, args = getopt.getopt(argv, "psbd")
    except getopt.GetoptError, e:
        print(os.path.basename(__file__ ) + " -p -s -b -d \n" + e.msg )
        sys.exit(1)

    for opt, arg in opts:
        if opt in '-h':
            print(__file__ & "-o")
            sys.exit(2)
        elif opt == "-p":
            printXml = True
        elif opt == "-s":
            runAsService = True
        elif opt==  "-d":
            debug = True
        elif opt == "-b":
            dbConnection = True


    global cfg
    cfg = SafeConfigParser()
    cfg.optionxform = str
    cfg.read(os.path.normpath(os.path.dirname(os.path.realpath(__file__)) + "/config.conf"))

    login_url = cfg.get("site", "loginurl")
    accaUrl = cfg.get("site", "accalisturl")
    username = cfg.get("site", "username")
    password = cfg.get("site", "password")

    connectDb()

    initLogger(os.path.splitext(os.path.basename(__file__))[0], debug)

    firstRun = True
    acca = AccaGen(login_url, accaUrl, username, password)

    if dbConnection:
        if db == None:
            log.error("DB connection specified however no DB Connection could be made. Exiting...")
            sys.exit(1)

    while acca.session != None and (firstRun or runAsService):
        acca.updateAccaList()

        firstRun = False

    if db != None:
        db.close()

main(sys.argv[1:])
