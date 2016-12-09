import re
import MySQLdb

def insertFileToDB(database, file):
    queryArr = []
    tweetId = ""
    tweet = ""
    hashTag = ""
    category = ""
    entity = ""
    entityRelevent = ""
    polarity = ""
    score = ""
    priority = ""
    regId = r'id:(\w*-\w*-\w*-\w*-\w*\b)?'
    compileIdReg = re.compile(regId)
    regTweet = r'Tweet:(.*?)Entity:'
    compileTweetReg = re.compile(regTweet)
    regEntity = r'Entity:(.*?)Category:'
    compileEntityReg = re.compile(regEntity)
    regCategory = r'Category:(.*?)Hashtags:'
    compileCategoryReg = re.compile(regCategory)
    regHashtags = r'Hashtags:(.*?)Polarity:'
    compileHashtagsReg = re.compile(regHashtags)
    regPolarity = r'Polarity:(.*?)Score:'
    compilePolarityreg = re.compile(regPolarity)
    regScore = r'Score:(.*?)Priority:'
    compileScorereg = re.compile(regScore)
    regPriority = r'Priority:(.*?)Relevant:'
    compilePriorityReg = re.compile(regPriority)
    regRelevant = r'Relevant: (.*\b)?'
    compileRelevantReg = re.compile(regRelevant)
    for line in file:
        idList = re.findall(compileIdReg, line)
        if len(idList) > 0:
            tweetId = idList[0]
        tweetList = re.findall(compileTweetReg, line)
        if len(tweetList) > 0:
            tweet = tweetList[0].strip()
        entityList = re.findall(compileEntityReg, line)
        if len(entityList) > 0:
            entity = entityList[0].strip()
        categoryList = re.findall(compileCategoryReg, line)
        if len(categoryList) > 0:
            category = categoryList[0].strip()
        hashtagsList = re.findall(compileHashtagsReg, line)
        if len(hashtagsList) > 0:
            hashTag = hashtagsList[0].strip()
        polarityList = re.findall(compilePolarityreg, line)
        if len(polarityList) > 0:
            polarity = polarityList[0].strip()
        scoreList = re.findall(compileScorereg, line)
        if len(scoreList) > 0:
            score = scoreList[0].strip()
        priorityList = re.findall(compilePriorityReg, line)
        if len(priorityList) > 0:
            priority = priorityList[0].strip()
        relevantList = re.findall(compileRelevantReg, line)
        if len(relevantList) > 0:
            entityRelevent = relevantList[0]
        query = "insert into Tweet_Table values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',' %s');" % (tweetId, tweet, hashTag, category, entity, entityRelevent, polarity, score, priority)
        queryArr.append(query)
        
    cursor = database.cursor()
    for query in queryArr:
        try:
            cursor.execute(query)
            database.commit()
        except:
            print "exception happend"
            database.rollback()
            return "task failed"
    return "task finished"
            
        
        




def insertEntityToDB(database, file):
    entityQuery = []
    for line in file:
        tweetId = 0
        entity = ""
        reg = r'(\d*?):'
        compileReg = re.compile(reg)
        idList = re.findall(compileReg, line)
        for iden in idList:
            tweetId = iden
            break
        reg = r'Entity: (.*\b)'
        compileReg = re.compile(reg)
        entityList = re.findall(compileReg, line)
        for entities in entityList:
            entity = entities
            break
        query = "insert into Tweet_Entity values (%d, '%s');" % (int(tweetId), entity)
        entityQuery.append(query)
    cursor = database.cursor()
    for query in entityQuery:
        try:
            cursor.execute(query)
            database.commit()
        except:
            database.rollback()

def insertTweetAndIDToDB(database, file):
    queryArr = []
    for line in file:
        tweetId = 0
        tweetContent = ""
        reg = r'(\d*?):'
        compileReg = re.compile(reg)
        idList = re.findall(compileReg, line)
        for iden in idList:
            tweetId = iden
            break
        reg = r'\d*?:(.*\b)'
        compileReg = re.compile(reg)
        tweetContents = re.findall(compileReg, line)
        for tweet in tweetContents:
            tweetContent = tweet
            break
        query = "insert into Tweet_Entity values (%d, '%s');" % (int(tweetId), tweetContent.replace("\'", "\\'"))
        queryArr.append(query)
        print query
    cursor = database.cursor()
    for query in queryArr:
        try:
            cursor.execute(query)
            database.commit()
        except:
            database.rollback()



# def insertStockToDatabase(stock):
#     queryArr = []
#     for incomeQrecord in stock.incomeQuarterDataArr:
#         insertIncomeQsql = "insert into IncomeStatement_Quarter values ('%s', '%s', '%s'" % (stock.Market, stock.Symbol, incomeQrecord.EndingDate)
#         for recordData in incomeQrecord.dataRecord:
#             insertIncomeQsql += ", %f" % (recordData)
#         insertIncomeQsql += ");"
#         queryArr.append(insertIncomeQsql)
#     for incomeArecord in stock.incomeAnnualDataArr:
#         insertIncomeAsql = "insert into IncomeStatement_Annual values ('%s', '%s', '%s'" % (stock.Market, stock.Symbol, incomeArecord.EndingDate)
#         for recordData in incomeArecord.dataRecord:
#             insertIncomeAsql += ", %f" % (recordData)
#         insertIncomeAsql += ");"
#         queryArr.append(insertIncomeAsql)

#     for balanceQrecord in stock.balanceQuarterDataArr:
#         insertbalanceQsql = "insert into Balance_Quarter values ('%s', '%s', '%s'" % (stock.Market, stock.Symbol, balanceQrecord.EndingDate)
#         for recordData in balanceQrecord.dataRecord:
#             insertbalanceQsql += ", %f" % (recordData)
#         insertbalanceQsql += ");"
#         queryArr.append(insertbalanceQsql)

#     for balanceArecord in stock.balanceAnnualDataArr:
#         insertbalanceAsql = "insert into Balance_Annual values ('%s', '%s', '%s'" % (stock.Market, stock.Symbol, balanceArecord.EndingDate)
#         for recordData in balanceArecord.dataRecord:
#             insertbalanceAsql += ", %f" % (recordData)
#         insertbalanceAsql += ");"
#         queryArr.append(insertbalanceAsql)

#     for cashFlowQrecord in stock.cashFlowQuarterDataArr:
#         insertcashFlowQsql = "insert into Cash_Flow_Quarter values ('%s', '%s', '%s'" % (stock.Market, stock.Symbol, cashFlowQrecord.EndingDate)
#         for recordData in cashFlowQrecord.dataRecord:
#             insertcashFlowQsql += ", %f" % (recordData)
#         insertcashFlowQsql += ");"
#         queryArr.append(insertcashFlowQsql)
    
#     for cashFlowArecord in stock.cashFlowAnnualDataArr:
#         insertcashFlowAsql = "insert into Cash_Flow_Annual values ('%s', '%s', '%s'" % (stock.Market, stock.Symbol, cashFlowArecord.EndingDate)
#         for recordData in cashFlowArecord.dataRecord:
#             insertcashFlowAsql += ", %f" % (recordData)
#         insertcashFlowAsql += ");"
#         queryArr.append(insertcashFlowAsql)

#     database = MySQLdb.connect("localhost","root","000000","financialDataSchema" )
#     cursor = database.cursor()
#     for query in queryArr:
#         try:
#             cursor.execute(query)
#             database.commit()
#         except:
#             database.rollback()

#     database.close()


# get all http query (url) of financials, urlArr contains all urls indicating web page of financial information
database = MySQLdb.connect("localhost","root","000000","RepLab" )
EntityFile = open("AnalyseResult.txt")
success = insertFileToDB(database, EntityFile)
print success
database.close()


EntityFile.close()
