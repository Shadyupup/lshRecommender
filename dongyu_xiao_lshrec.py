import itertools
from pyspark import SparkContext
import sys


def movieUserTable(inputuser):
    userset = inputuser.split(",")
    userid = userset[0]
    movieset = [0 for _ in range(100)]
    for movieid in userset[1:]:
        movieset[int(movieid)] = 1
    return [userid, movieset]


def minhash(movieUserTable):
    key = movieUserTable[0]
    moviesets = movieUserTable[1]
    signature = [sys.maxsize for _ in range(20)]
    for i in range(20):
        for x in range(0, 100):
            if moviesets[x] == 1:
                hashIndex = (3 * x + 13 * i) % 100
                signature[i] = min(signature[i], hashIndex)
    return [key, signature]


def bandlize(userSigTable):
    res = []
    value = userSigTable[0]
    valuelist = userSigTable[1]
    for i in range(5):
        res.append([str(i) + "," + str(valuelist[i * 4:i * 4 + 4]), value])
    return res


def printf(iterator):
    mylist = list(iterator)
    print(mylist)


def jaccard(candidatepairs):
    candidate1 = set(movieToUserTable[candidatepairs[0]])
    candidate2 = set(movieToUserTable[candidatepairs[1]])
    intersac = len(candidate1 & candidate2)
    union = len(candidate1 | candidate2)
    return (candidatepairs[0], (candidatepairs[1], float(intersac) / union))


def getallpair(pairs):
    res=[]
    key=pairs[0]
    value=pairs[1]
    res.append([key,value])
    res.append([value,key])
    return res


def mapInput(inputFile):
    inputArray = inputFile.split(",")
    return [inputArray[0], inputArray[1:]]


def getUser(jaccardtable):
    primarykey=jaccardtable[0]
    valuelist=[]
    for valuepair in jaccardtable[1]:
        key=int(valuepair[0][1:])
        value=valuepair[1]
        valuelist.append([key,value])
    usersid=[]
    user=sorted(valuelist, key=lambda x: [x[1],-x[0]], reverse=True)[:5]
    for userid in user:
        usersid.append(userid[0])
    userslist=[]
    for item in usersid:
        userslist.append("U"+str(item))
    return [primarykey,userslist]


def getMovie(getsimilaruser):
    key=getsimilaruser[0]
    counterTable={}
    for userid in getsimilaruser[1]:
        for movieid in movieToUserTable[userid]:
            if movieid not in counterTable.keys():
                counterTable[movieid]=1
            else:
                counterTable[movieid]+=1
    movielist=sorted(counterTable.items(),key=lambda x: (x[1],-int(x[0])), reverse=True)[:3]
    res=[]
    for movie in movielist:
        res.append(movie[0])
    return [key,sorted(res,key=lambda x:int(x))]

if __name__ == "__main__":
    sc = SparkContext(appName="Dongyu")
    inputFile = sc.textFile(sys.argv[1], 2)
    movieToUser = inputFile.map(movieUserTable)

    inputTable=inputFile.map(mapInput)
    movieToUserTable = dict(inputTable.collect())

    userSigTable = movieToUser.map(minhash)

    bandtable = userSigTable.flatMap(bandlize)

    candidatepairs = bandtable.groupByKey().mapValues(lambda x: list(x)).filter(lambda x: len(x[1]) > 1).map(
        lambda x: itertools.combinations((x[1]), 2)).flatMap(lambda x: list(x)).distinct().flatMap(getallpair)

    jaccardtable = candidatepairs.map(jaccard).groupByKey().mapValues(lambda x:list(x))

    getsimilaruser=jaccardtable.map(getUser)

    getmovie=getsimilaruser.map(getMovie)
    res=sorted(getmovie.collect(),key=lambda x:int(x[0][1:]))

    output = open(sys.argv[2], "w")
    for pair in res:
        output.write(pair[0] + "," + ','.join(pair[1]) + "\n")
    output.close()
