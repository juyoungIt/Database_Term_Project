import string
import urllib.request as r
import urllib.parse as p
import json
import pandas as pd
import ssl
import pymysql
import spacy
from idna import unicode
from nltk.corpus import stopwords
import sys
import re
import hanja
from konlpy.tag import Okt


# for data collect
print('Data loading start')
url = "URL here..."
serviceKey = 'Access Key here...'  # critical key... please be careful about security issues...
numOfCnt = 200
maxPage = 48  # the maximum page of data set
source = pd.DataFrame(columns=['title', 'body', 'writer', 'date', 'category', 'institution', 'file',
                               'fileContent', 'fileName', 'fileURL', 'institutionURL'])
# page 49, 50 have issue (only use 9,600 data)
print('loading')
for pageNum in range(1, maxPage + 1):
    print('Page', pageNum, 'is done...')
    page = pageNum
    option = "serviceKey=" + serviceKey
    request = "&numOfCnt=" + p.quote(str(numOfCnt)) + "&page=" + p.quote(str(page))
    url_full = url + option + request
    context = ssl._create_unverified_context()
    response = r.urlopen(url_full, context=context).read().decode('utf-8')
    jsonArray = json.loads(response)
    if jsonArray.get("header").get("resultCode") != 200:
        print("Error!!!")
        print(jsonArray.get("header"))
        quit()
    items = jsonArray.get("body").get("contents")
    for item in items:
        source = source.append(item, ignore_index=True)
print('Data loading is done...')

# make column name table
print('Database initialize start')
cols = []
for colName in source.columns:
    cols.append(colName)

# database connection info
db = pymysql.connect(
    user='username here...',
    passwd='your password here...',
    host='ip address here...',
    db='database name here...',
    charset='utf8mb4'
)


def isNaN(str):
    return str != str


def preprocessing(x):
    for columnName in cols:
        if isNaN(source.loc[x, columnName]) or source.loc[x, columnName] == '':
            source.loc[x, columnName] = None
        else:
            if not source.loc[x, columnName] is None:
                source.loc[x, columnName] = source.loc[x, columnName].strip()


# sql query transmission
cursor = db.cursor()
init1 = 'SET FOREIGN_KEY_CHECKS = 0;'
init2 = 'TRUNCATE TABLE body;'
init3 = 'TRUNCATE TABLE file;'
init4 = 'TRUNCATE TABLE doc;'
init5 = 'TRUNCATE TABLE institution;'
init6 = 'TRUNCATE TABLE inv_index;'
init7 = 'TRUNCATE TABLE terms;'
init8 = 'SET FOREIGN_KEY_CHECKS = 1;'
cursor.execute(init1)
cursor.execute(init2)
cursor.execute(init3)
cursor.execute(init4)
cursor.execute(init5)
cursor.execute(init6)
cursor.execute(init7)
cursor.execute(init8)
db.commit()
print('Database initialize done...')

# for database connection (mysql)
# sql statement for database insertion
sqlForBody = 'INSERT INTO body(doc_id, body) VALUES (%s, %s);'
sqlForDoc = 'INSERT INTO doc(doc_id, title, date, category, writer, inst_id) VALUES (%s, %s, %s, %s, %s, %s);'
sqlForFile = 'INSERT INTO file(doc_id, filename, file_url) VALUES (%s, %s, %s);'
sqlForInst = 'INSERT INTO institution(inst_id, inst_name, inst_url) VALUES (%s, %s, %s);'

# transmit sql query
cursor = db.cursor()
temp = {}
valForInst = ()
tmp = ()
count = 0

print('query processing start (normalization table)')
# insert doc table data (foreign key constraint)
for idx in range(0, maxPage * numOfCnt):
    preprocessing(idx)
    tmp = (idx + 100, source.loc[idx, 'institution'], source.loc[idx, 'institutionURL'])
    if tmp[1] not in temp.keys():
        temp[tmp[1]] = count
        count += 1
        valForInst = (temp[tmp[1]], source.loc[idx, 'institution'], source.loc[idx, 'institutionURL'])
        # print(valForInst)
        cursor.execute(sqlForInst, valForInst)  # for institution
    db.commit()  # commit the working result

for idx in range(0, maxPage * numOfCnt):
    valForDoc = (idx, source.loc[idx, 'title'], source.loc[idx, 'date'],
                 source.loc[idx, 'category'], source.loc[idx, 'writer'], temp[source.loc[idx, 'institution']])
    cursor.execute(sqlForDoc, valForDoc)  # for doc - to prevent foreign key constraint
    db.commit()  # commit the working result

for idx in range(0, maxPage * numOfCnt):
    # eliminate nan value in the values
    preprocessing(idx)

    valForBody = (idx, source.loc[idx, 'body'])
    valForFile = (idx, source.loc[idx, 'fileName'], source.loc[idx, 'fileURL'])

    # transmit and execute sql query
    cursor.execute(sqlForBody, valForBody)  # for body
    cursor.execute(sqlForFile, valForFile)  # for file
    # cursor.execute(sqlForInst, valForInst)  # for institution

    db.commit()  # commit the working result

print('query processing done!! (normalization table)')

# for tokenization
print('Tokenization start')
# merge document contents (title + author + body)
mergeString = []
for idx in range(0, maxPage * numOfCnt):
    title = source['title'][idx]
    body = source['body'][idx]
    writer = source['writer'][idx]
    # exception handling for NonType Processing
    if title is None:
        title = ' '
    if body is None:
        body = ' '
    if writer is None:
        writer = ' '

    text = hanja.translate(title, "substitution")
    title = text
    result = title + ' ' + writer + ' ' + body
    mergeString.append(result)



def isHangul(text):
    # Check the Python Version
    pyVer3 = sys.version_info >= (3, 0)

    if pyVer3:  # for Ver 3 or later
        encText = text
    else:  # for Ver 2.x
        if type(text) is not unicode:
            encText = text.decode('utf-8')
        else:
            encText = text

    hanCount = len(re.findall(u'[\u3130-\u318F\uAC00-\uD7A3]+', encText))
    return hanCount > 0


def remove_punctuation(text):
    sent = []
    for t in text.split(' '):
        no_punct = ''.join([c for c in t if c not in string.punctuation])
        sent.append(no_punct)
    sentence = ' '.join(s for s in sent)
    return sentence



nlp = spacy.load('en_core_web_sm')

allDoc = []   # total document's termsㅇ
total_set = []  # total term list
for idx in range(0, maxPage * numOfCnt):
    if (idx+1) % 200 == 0:
        print('page = ', int((idx+1)/200))
    rmv_sw_sentence = []
    temp = mergeString[idx]
    temp1 = temp.replace(' ', '')
    # in case of english
    if not isHangul(temp1):
        # eliminate additional unnecessary words
        rmv_punc_sentence = remove_punctuation(mergeString[idx])
        rmv_punc_sentence = rmv_punc_sentence.replace('’s', "")
        rmv_punc_sentence = rmv_punc_sentence.replace('\t', "")
        rmv_punc_sentence = rmv_punc_sentence.replace('\r', "")
        rmv_punc_sentence = rmv_punc_sentence.replace('\n', "")
        rmv_punc_sentence = rmv_punc_sentence.replace('·', "")

        # print(rmv_punc_sentence)
        lower_sentence = rmv_punc_sentence.lower()
        # print(lower_sentence)
        doc = nlp(lower_sentence.strip())
        tok_lem_sentence = [token.lemma_ for token in doc]
        # print(tok_lem_sentence)
        stop_words = set(stopwords.words('english'))
        # print(tok_lem_sentence, '\n')
        for w in tok_lem_sentence:
            if w not in stop_words:
                total_set.append(w)
        rmv_sw_sentence = [w for w in tok_lem_sentence if not w in stop_words] # 하나의 문서에 대한 영어 결과
        # removed_word = [w for w in tok_lem_sentence if not w in rmv_sw_sentence]
        # print('removed_word = ', removed_word)
    # in case of korean
    else:
        okt = Okt()
        content = mergeString[idx]
        content = content.replace('’', "")
        content = content.replace('’s', "")
        content = content.replace('\t', "")
        content = content.replace('\r', "")
        content = content.replace('\n', "")
        content = content.replace('·', "")
        pos = okt.pos(content, stem=True)
        for term, tag in pos:
            if tag not in ['Josa', 'Punctuation', 'KoreanParticle', 'Foreign', 'Unknown']:
                rmv_sw_sentence.append(term)
                total_set.append(term)
    allDoc.append(rmv_sw_sentence)
print("Tokenization done!!")

# implement inverted index
result_dict = {}    # make new dict
for word in total_set:
    if word in result_dict:
        result_dict[word] += 1
    elif word == ' ' or word == '   ':
        continue
    else:
        result_dict[word] = 1

# implement inverted index
termsList = []
print('query processing start (terms table)')
# update the terms table
sqlForTerms = 'INSERT INTO terms(terms_id, term) VALUES (%s, %s)'
key = list(result_dict.keys())  # get the keys list
term_id = 0
for term in key:
    valForTerms = (term_id, term.lower())
    rev_valForTerms = (term.lower(), term_id)
    termsList.append(rev_valForTerms)
    term_id += 1  # increase the term_id values
    cursor.execute(sqlForTerms, valForTerms)
    db.commit()
print('query processing done!! (terms table)')

# make term_dict
term_dict = dict(termsList)

# update inv_idx table
print('query processing start (inverted index table)')
sqlForInv_idx = 'INSERT INTO inv_index(terms_id, doc_id, freq) VALUES (%s, %s, %s);'
for idx in range(0, len(allDoc)):
    temp_dict = {}  # temporary dictionary
    for word in allDoc[idx]:
        if word in temp_dict:
            temp_dict[word.lower()] += 1
        elif word == ' ' or word == '   ' or word == '\'' or word == '"' or word == '´' or word == '’' or word == '-':
            continue
        else:
            temp_dict[word.lower()] = 1
    term = list(temp_dict.keys())
    value = list(temp_dict.values())

    for j in range(0, len(term)):
        term_id = term_dict.get(term[j])
        valForInv_idx = (term_id, idx, value[j])
        # print(term[j], ' ', valForInv_idx)
        cursor.execute(sqlForInv_idx, valForInv_idx)
        db.commit()

print('query processing done!! (inverted index table)')