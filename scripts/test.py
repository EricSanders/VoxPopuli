import VoxPopuli
from voxpopulivariables import *

partypatterns = {}
partypatterns["vvd"] = ["v\\.?v\\.?d\\.?"]
partypatterns["pvda"] = ["p\\.?v\\.?d\\.?a\\.?","partij\\s+v(oor\\s+|an\\s+|.)?d(e|.)?\\s+arbeid"]
partypatterns["sp"] = ["s\\.?p\\.?"]
partypatterns["pvv"] = ["p\\.?v\\.?v\\.?","partij\\s+v(oor\\s+|an\\s+|.)?d(e|.)?\\s+vrijheid"]
partypatterns["cda"] = ["c\\.?d\\.?a\\.?"]
partypatterns["d66"] = ["d\'?66"]
partypatterns["gl"] = ["g\\.?l\\.?","groen.?links"]
partypatterns["cu"] = ["c\\.?u\\.?","christen.?unie"]
partypatterns["pvdd"] = ["p\\.?v\\.?d\\.?d\\.?","partij\\s+v(oor\\s+|an\\s+|.)?d(e|.)?\\s+dieren"]
partypatterns["sgp"] = ["s\\.?g\\.?p\\.?"]
partypatterns["50plus"] = ["50[^\\d]?\s*(\\+|plus)"]
partypatterns["denk"] = ["denk(_)?(nl)?"]
partypatterns["fvd"] = ["f\\.?v\\.?d\\.?","forum\\s*v(oor)?\\s*democratie"]

partyerrorpatterns = {}
partyerrorpatterns["denk"] =['aan\\s+denk',
'dan\\s+denk',
#'en\\s+denk',
'ik\\s+denk',
'maar\\s+denk',
'denk\\s+aan',
'denk\\s+alleen',
'denk\\s+als',
'denk\\s+bij',
'denk\\s+daar',
'denk\\s+dan',
'denk\\s+dat',
#'denk\\s+da',
'denk\\s+dit',
'denk\\s+eens',
'denk\\s+een',
'denk\\s+eerder',
'denk\\s+er',
'denk\\s+even',
'denk\\s+ff',
'denk\\s+het',
'denk\\s+hier',
'denk\\s+ik',
'denk\\s+je',
'denk\\s+jij',
'denk\\s+maar',
'denk\\s+mee',
'denk\\s+na',
'denk\\s+niet',
'denk\\s+nog',
'denk\\s+nu',
'denk\\s+ook',
'denk\\s+over',
#'denk\\s+te',
'denk\\s+toch',
'denk\\s+weer',
'denk\\s+wel'
                             ]

'''
testcorpus = VoxPopuli.TweetCorpusNonAnnotated()
testcorpus.readgzfiles(firstdatehour='2011012016',lastdatehour='2011012016',pathname=tweetjsonfilesdir)

#userlocations = testcorpus.getuserlocations()
#for userlocation in userlocations:
    #print(userlocation, userlocations[userlocation])

testcorpus.filteronpatterns(partypatterns,partyerrorpatterns,prepattern='(\s|\.|,|;|:|!|\?|\@|\#|^)',postpattern='(\s|\.|,|;|:|!|\?|$)',ignorecase=True,removemismatch=True)

'''

testcorpus2 = VoxPopuli.TweetCorpusAnnotated()
testcorpus2.readmysql(db1,'tweetid','')
testcorpus2.readmysql(db2,'tweetid','userid')
print("All tweets:")
print(len(testcorpus2.tweets))
testcorpus2.removenonannotated()
print("Annotated tweets:")
print(len(testcorpus2.tweets))


#testcorpus2.writetextandlabels(annotationlabel='politiek',outputfilename='output5050',decidingannotatorid='',minimumpercentageperlabel=50)

(annotatorids,annotatorcombinations,nrannotations,values) = testcorpus2.annotationsummary(['politiek'],decidingannotatorid='eric')

#exit(0)

print("Annotators:")
for annotatorid in annotatorids:
    print(annotatorid,annotatorids[annotatorid])
print("Annotator combinations:")
for annotatorcombination in annotatorcombinations:
    print(annotatorcombination,annotatorcombinations[annotatorcombination])
print("Annotation numbers per number of annotators:")
for nr in nrannotations:
    print(nr, nrannotations[nr])
print("Annotation values:")
for annotationfield in values:
    print(annotationfield)
    for value in values[annotationfield]:
        print(value,values[annotationfield][value])



        
exit(0)
print(len(testcorpus2.tweets))
testcorpus2.createtraintestset(trainsetperc=80,testsetperc=20)
print(len(testcorpus2.trainset))
print(len(testcorpus2.testset))
print(len(testcorpus2.devtestset))
