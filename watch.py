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

    # Redis conneciton
    print('Connecting to the Redis databse...')
    r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
except pymongo.errors.ServerSelectionTimeoutError as err:
    print(err)
    sys.exit(1)


def watchVibes():
    """ Watch `vibes` collection """
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
            
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)



def watchUsers():
    """ Watch `users` collection and build recommendation for user """
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)


def watchUseredges():
    """ Watch `useredges` collection and build recommendation based on that """
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.submit(watchVibes)
    executor.submit(watchUsers)
    executor.submit(watchUseredges)