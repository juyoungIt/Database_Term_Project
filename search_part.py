# search function by using inverted index
import pymysql
from rank_bm25 import BM25Okapi

# database connection information
db = pymysql.connect(
    user='dbuser',
    passwd='PYeongbong!376',
    host='192.168.56.101',
    db='Term_project',
    charset='utf8mb4'
)

# make cursor for interaction with database
cursor = db.cursor()

# make sql statement
sql_loadTerms = 'SELECT term FROM inv_index JOIN terms USING (terms_id) WHERE doc_id = (%s);'
sql_candidate = 'SELECT doc_id FROM inv_index JOIN terms USING (terms_id) WHERE term = (%s);'

# enter the search keyword from the users
search_word = input('Enter the Search Word : ')
tokenized_search_word = search_word.lower().split(" ")
# print(tokenized_search_word) - for test

# load candidate document by using search word
doc_list = []
for term in tokenized_search_word:
    val_candidate = term
    cursor.execute(sql_candidate, val_candidate)
    result = [item[0] for item in cursor.fetchall()]  # for formatting
    doc_list.append(list(result))

# merge document list - for BM25 algorithm
# if search keyword consist multiple words
merge_doc_list = []
for each_doc in doc_list:
    merge_doc_list.extend(each_doc)  # merge the all doc_list
merge_doc_list = sorted(set(merge_doc_list))  # sort doc_list by using doc_id value

# use the MB25 algorithm
tokenized_corpus = []
for doc_id in merge_doc_list:
    val_loadTerms = doc_id  # load the doc_id
    cursor.execute(sql_loadTerms, val_loadTerms)
    tmp = [item[0] for item in cursor.fetchall()]  # for formatting
    tokenized_corpus.append(tmp)

# give the dataset for BM25 Algorithm
try:
    bm25 = BM25Okapi(tokenized_corpus)
    # rank 처리된 doc_id의 list를 반환
    search_result = bm25.get_scores(tokenized_search_word)  # load the matching score
    table = {}  # table for doc_id & score
    for idx in range(len(search_result)):
        table[merge_doc_list[idx]] = search_result[idx]

    table = sorted(table.items(), key=lambda x: x[1], reverse=True)  # sort by using value(score)
    table = dict(table)  # convert list to dictionary

    # make the final rank list
    rank_result = []
    for key, value in table.items():
        rank_result.append(key)
except:
    rank_result = []


print(rank_result)  # print the rank result
