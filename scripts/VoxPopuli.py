import subprocess
import datetime
import json
import pymysql
import re
import random

class Tweet:
    def __init__(self, tweetid = '', datetime = '', retweettotweetid = '', replytotweetid = '', replytotweeter = '', text = '', tweeterid = '', tweetername = '', tweeter = '', tweeterlocation = ''):

        self.variables = {}
        self.variables['tweetid'] = tweetid
        self.variables['datetime'] = datetime
        self.variables['retweettotweetid'] = retweettotweetid
        self.variables['replytotweetid'] = replytotweetid
        self.variables['replytotweeter'] = replytotweeter
        self.variables['text'] = re.sub('[\r\n]+','<NL>',text)
        self.variables['tweeterid'] = tweeterid
        self.variables['tweetername'] = tweetername
        self.variables['tweeter'] = tweeter
        self.variables['tweeterlocation'] = tweeterlocation
        self.matches = {}
        self.annotations = {}
        
    def set(self,key,value):
        self.variables[key] = value

    def get(self,key):
        if key in self.variables:
            return self.variables[key]
        else:
            return 'NULL'

    def patternmatch(self,patterns,errorpatterns,prepattern='(\s|\.|,|;|:|!|\?|\@|\#|^)',postpattern='(\s|\.|,|;|:|!|\?|$)',ignorecase=True):
        matched = False
        for key in patterns:
            nrpatternmatches = 0
            for pattern in patterns[key]:
                matchpattern = prepattern+'('+pattern+')'+postpattern
                if ignorecase:
                    matches = re.findall('('+matchpattern+')',self.variables['text'],re.IGNORECASE)
                else:
                    matches = re.findall('('+matchpattern+')',self.variables['text'])
                if matches:
                    nrpatternmatches += len(matches)

                nrerrorpatternmatches = 0
                if key in errorpatterns:
                    for errorpattern in errorpatterns[key]:
                        if ignorecase:
                            errormatches = re.findall('('+errorpattern+')',self.variables['text'],re.IGNORECASE)
                        else:
                            errormatches = re.findall('('+errorpattern+')',self.variables['text'])
                        if errormatches:
                            nrerrorpatternmatches += len(errormatches)

                if nrpatternmatches > nrerrorpatternmatches:
                    matched = True
                    for match in matches:
                        self.addmatch(key,match[2])

        return matched
        
    def addmatch(self,key,pattern):
        if key not in self.matches:
            self.matches[key] = []
        self.matches[key].append(pattern)

    def getmatches(self):
        for key in self.matches:
            print(key)
            for pattern in self.matches[key]:
                print('  ' + str(pattern))
                
    def setannotation(self,annotatorid,annotationfield,annotationvalue):
        if annotationfield not in self.annotations:
            self.annotations[annotationfield] = {}
        self.annotations[annotationfield][annotatorid] = annotationvalue

    def getannotations(self):
        return self.annotations

    def getannotationvalue(self,annotationlabel,decidingannotatorid=''):
        returnvalue = ''
        if annotationlabel in self.annotations:
            currentvalues = []
            for annotatorid in self.annotations[annotationlabel]:
                value = self.annotations[annotationlabel][annotatorid]
                currentvalues.append(value)
                if decidingannotatorid == annotatorid:
                    returnvalue = value
            if not decidingannotatorid:
                returnvalue = max(set(currentvalues),key=currentvalues.count)
        else:
            print("annotationlabel "+annotationlabel+" does not exist")    
        return returnvalue

class TweetCorpus:
    def __init__(self):

        pass

    def getuserlocations(self):
        userlocations = {}
        for tweetid in self.tweets:
            userlocation = self.tweets[tweetid].variables['tweeterlocation']
            if not userlocation in userlocations:
                userlocations[userlocation] = 0
            userlocations[userlocation] += 1

        return userlocations

    def filteronpatterns(self,patterns,errorpatterns={},prepattern='(\s|\.|,|;|:|!|\?|\@|\#|^)',postpattern='(\s|\.|,|;|:|!|\?|$)',ignorecase=True,removemismatch=True):
        nonmatchingtweetids = []
        for tweetid in self.tweets:
            if not self.tweets[tweetid].patternmatch(patterns,errorpatterns,prepattern,postpattern,ignorecase):
                nonmatchingtweetids.append(tweetid)
        if removemismatch:
            for tweetid in nonmatchingtweetids:
                del self.tweets[tweetid]

        for tweetid in self.tweets:
            self.tweets[tweetid].getmatches()
            
        print(len(self.tweets))
    
    def writecsv(self):
        #text = re.sub('\n','<NL>',pymysql.escape_string(jsondict['text']))
        pass
    
class TweetCorpusNonAnnotated(TweetCorpus):
    def __init__(self):

       self.tweets = {}

    def readjsonfile(self,jsonfilename):
        correcttweetnr = 0
        errortweetnr = 0
        doubletweetnr = 0
        linenr = 0
        jsonfile = open(jsonfilename)
        for jsonline in jsonfile:
            linenr += 1
            try:
                jsondict = json.loads(jsonline)
                text = pymysql.escape_string(jsondict['text'])
                tweetid = jsondict['id_str']
                datetime = jsondict['created_at']
                tweeterid = jsondict['user']['id_str']
                tweetername = jsondict['user']['name']
                tweeter = jsondict['user']['screen_name']
                if tweetid in self.tweets:
                    #print(tweetid + " already seen")
                    if self.tweets[tweetid].variables['tweeter'] != tweeter:
                        print("tweetid same, but tweeter differs!")
                    doubletweetnr += 1
                tweeterlocation = jsondict['user']['location']
                try:
                    retweettotweetid = jsondict['retweeted_status']['id_str']
                except:
                    retweettotweetid = 'NULL'
                try:
                    replytotweetid = re.sub('None','NULL',jsondict['in_reply_to_status_id_str'])
                except:
                    replytotweetid = 'NULL'
                try:
                    replytotweeter = re.sub('None','NULL',jsondict['in_reply_to_screen_name'])
                except:
                    replytotweeter = 'NULL'

                tweet = Tweet(tweetid = tweetid, datetime = datetime, retweettotweetid = retweettotweetid, replytotweetid = replytotweetid, replytotweeter = replytotweeter, text = text, tweeterid = tweeterid, tweetername = tweetername, tweeter = tweeter, tweeterlocation = tweeterlocation)
                self.tweets[tweetid] = tweet
                correcttweetnr += 1
            except:
                errortweetnr += 1
                #print(jsonfilename,linenr)

        return correcttweetnr, errortweetnr, doubletweetnr
                
    def readgzfiles(self,firstdatehour,lastdatehour,pathname='/vol/bigdata2/datasets2/twitter'):

        startdatehour = datetime.datetime.strptime(firstdatehour,"%Y%m%d%H") # starting time
        enddatehour = datetime.datetime.strptime(lastdatehour,"%Y%m%d%H") # ending time
        datehour = startdatehour

        while datehour <= enddatehour:
            jsonfilename = "tweets_" + datehour.strftime("%Y%m%d%H")
            storedzipfilename = pathname + "/" + datehour.strftime("%Y%m%d%H")[:6] + "/" + datehour.strftime("%Y%m%d%H")[:8] + "-" + datehour.strftime("%Y%m%d%H")[-2:] + ".out.gz"
            zipfilename = jsonfilename + ".gz"

            command = "cp"
            #print command,storedzipfilename,zipfilename
            subprocess.call([command,storedzipfilename,zipfilename])

            command = "gunzip"
            argument = "-f"
            #print command,argument,zipfilename
            subprocess.call([command,argument,zipfilename])

            print(jsonfilename)
            correcttweetnr, errortweetnr, doubletweetnr = self.readjsonfile(jsonfilename)
            print(len(self.tweets))
            #print(correcttweetnr, errortweetnr, doubletweetnr)
            
            command = "rm"
            #print command,jsonfilename
            subprocess.call([command,jsonfilename])

            datehour = datehour + datetime.timedelta(hours=1)

class TweetCorpusAnnotated(TweetCorpus):
    def __init__(self):

        self.tweets = {}
        self.trainset = []
        self.devtestset = []
        self.testset = []

    def readmysql(self,db,keylabel,annotationkeylabel):
        labelnames = {}
        values = {}

        try:
            tweetdb = pymysql.connect(host=db['host'], user=db['user'], passwd=db['passwd'],db=db['dbname'])
        except:
            raise ValueError("Error in opening dbase")
        try:
            cur = tweetdb.cursor()
            cur.execute("SELECT * FROM "+db['tablename'])
            results = cur.fetchall()
            columnnr = 0
            for desc in cur.description:
                labelnames[columnnr] = desc[0]
                columnnr += 1
            for row in results:
                for columnnr in range(len(labelnames)):
                    values[labelnames[columnnr]] = row[columnnr]
                    if labelnames[columnnr] == keylabel:
                        keylabelvalue = values[labelnames[columnnr]]
                    if labelnames[columnnr] == annotationkeylabel:
                        annotationkeylabelvalue = values[labelnames[columnnr]]
                    if labelnames[columnnr] == 'text':
                        #print(values[labelnames[columnnr]])
                        values[labelnames[columnnr]] = re.sub('[\r\n]+','<NL>',values[labelnames[columnnr]])
                        #print(values[labelnames[columnnr]])
                if keylabel in values:
                    if not keylabelvalue in self.tweets:
                        self.tweets[keylabelvalue] = Tweet()
                    for columnnr in range(len(labelnames)):
                        if annotationkeylabel in values:
                            self.tweets[keylabelvalue].setannotation(annotationkeylabelvalue,labelnames[columnnr],values[labelnames[columnnr]])
                        else:
                            self.tweets[keylabelvalue].set(labelnames[columnnr],values[labelnames[columnnr]])                                         

        except:
            raise ValueError("Error in reading dbase")

        try:
            tweetdb.close()
        except:
            pass

    def annotationsummary(self,requestedannotationfields=[],decidingannotatorid=''):
        annotatorids = {}
        values = {}
        nrannotations = {}
        annotatorcombinations = {}
        for tweetid in self.tweets:
            for annotationfield in self.tweets[tweetid].getannotations():
                if len(requestedannotationfields) > 0 and annotationfield in requestedannotationfields:
                    currentvalues = []
                    currentannotators = []
                    if not annotationfield in values:
                        values[annotationfield] = {}
                    for annotatorid in self.tweets[tweetid].annotations[annotationfield]:
                        currentannotators.append(annotatorid)
                        if not annotatorid in annotatorids:
                            annotatorids[annotatorid] = 0
                        annotatorids[annotatorid] += 1
                        value = self.tweets[tweetid].annotations[annotationfield][annotatorid]
                        #if decidingannotatorid == annotatorid:
                            #if not value in values[annotationfield]:
                                #values[annotationfield][value] = 0
                            #values[annotationfield][value] += 1
                        currentvalues.append(value)
                    currentannotators.sort()
                    annotatorcombination = ",".join(currentannotators)
                    if not annotatorcombination in annotatorcombinations:
                        annotatorcombinations[annotatorcombination] = 0
                    annotatorcombinations[annotatorcombination] += 1
                    nrcurrentannotations = len(currentvalues)
                    if not nrcurrentannotations in nrannotations:
                        nrannotations[nrcurrentannotations] = 0
                    nrannotations[nrcurrentannotations] += 1
                    #if not decidingannotatorid:
                        #mostfrequentvalue = max(set(currentvalues),key=currentvalues.count)
                        #if not mostfrequentvalue in values[annotationfield]:
                            #values[annotationfield][mostfrequentvalue] = 0
                        #values[annotationfield][mostfrequentvalue] += 1

                    annotationvalue = self.tweets[tweetid].getannotationvalue(annotationfield,decidingannotatorid)
                    if not annotationvalue in values[annotationfield]:
                        values[annotationfield][annotationvalue] = 0
                    values[annotationfield][annotationvalue] += 1

        return(annotatorids,annotatorcombinations,nrannotations,values)
    

        
    def removenonannotated(self):
        nonannotatedtweetids = []
        for tweetid in self.tweets:
            if len(self.tweets[tweetid].annotations) == 0:
                nonannotatedtweetids.append(tweetid)
        for tweetid in nonannotatedtweetids:
            del self.tweets[tweetid]

    def createtraintestset(self,trainsetsize=0,testsetsize=0,devtestsetsize=0,trainsetperc=90,testsetperc=10,devtestsetperc=0):
        totnrtweets = len(self.tweets)
        if trainsetperc + testsetperc + devtestsetperc > 100:
            print("trainsetperc + testsetperc + devtestsetperc higher than 100")
            exit(1)
        if not trainsetsize:
            trainsetsize = int(totnrtweets*trainsetperc/100)
        if not testsetsize:
            testsetsize = int(totnrtweets*testsetperc/100)
        if not devtestsetsize:
            devtestsize = int(totnrtweets*devtestsetperc/100)
        if trainsetsize + testsetsize + devtestsize > totnrtweets:
            print("trainsetsize + testsetsize + devtestsetsize higher than number of tweets")
            exit(1) 
        tweetids = list(self.tweets.keys())
        random.shuffle(tweetids)
        for tweetid in tweetids:
            if len(self.trainset) < trainsetsize:
                self.trainset.append(tweetid)
            elif len(self.testset) < testsetsize:
                self.testset.append(tweetid)
            else:
                self.devtestset.append(tweetid)
                
    def writetextandlabels(self,annotationlabel='',outputfilename='output',decidingannotatorid='',minimumpercentageperlabel=0):
        textperlabel = {}
        textandlabel = []
        textandlabelselection = []
        notweets = {}
        minnrtweets = 999999999999
        maxnrtweetsperlabel = 9999999999999
        for tweetid in self.tweets:
            text = self.tweets[tweetid].get('text')
            label = annotationlabel+"_"+self.tweets[tweetid].getannotationvalue(annotationlabel,decidingannotatorid)
            #print(text,annotationvalue)
            if not label in textperlabel:
                textperlabel[label] = []
            textperlabel[label].append(text)
            textandlabel.append((text,label))
        for label in textperlabel:
            notweets[label] = 0
            print(label,len(textperlabel[label]))
            if len(textperlabel[label]) < minnrtweets:
                minnrtweets = len(textperlabel[label])
        if minimumpercentageperlabel:
            maxnrtweetsperlabel = int(minnrtweets * 100 / minimumpercentageperlabel / len(textperlabel))
        print(maxnrtweetsperlabel)
                
        outputfiletext = open(outputfilename+'.txt',"w")
        outputfilelabels = open(outputfilename+'.labels',"w")
        #for nr in range(0,maxnrtweetsperlabel):
            #for label in textperlabel:
                #text = textperlabel[label][nr]
        for (text,label) in textandlabel:
            if notweets[label] < maxnrtweetsperlabel:
                textandlabelselection.append((text,label))
                notweets[label] += 1
        random.shuffle(textandlabelselection)
        for (text,label) in textandlabelselection:
            outputfiletext.write(text+"\n")
            outputfilelabels.write(label+"\n")
