import os
import concurrent.futures
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

try:
    print('Connecting to the PostgreSQL databse...')
    mongodbUrl = os.getenv('MONGODB_URL')
    client = MongoClient(mongodbUrl)
    db = client.ahunbackup
except pymongo.errors.ServerSelectionTimeoutError as err:
    print(err)
    sys.exit(1)


# while True:
#     try:
#         print('Lising to database')
#         for insert_change in db['vibes'].watch(
#             [{'$match': {'operationType': 'insert'}}]
#         ):
#             print(insert_change)
#     except pymongo.errors.PyMongoError as ex:
#         print(ex)


def watchVibes():
    while True:
        print('Watching vibes')


def watchUsers():
    """ Watch `users` collection and build recommendation for user """
    while True:
        print('Watch users')


def watchUseredges():
    """ Watch `useredges` collection and build recommendation based on that """
    while True:
        print('Watch users edges')


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.submit(watchVibes)
    executor.submit(watchUsers)
    executor.submit(watchUseredges)