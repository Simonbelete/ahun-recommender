import os
import redis
import concurrent.futures
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def watchInsertVibes():
    """ Watch `vibes` collection """
    try:
        print('Connecting to the PostgreSQL databse...')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        #db = client.ahunbackup
        db = client.ahuntest

        # Redis conneciton
        print('Connecting to the Redis databse...')
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)

    # Listen to mongodb changes
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                # Conains id of followers
                # In order to avoid placing vibe to the posters user vibe treat the current user as a follower
                followers = [ insert_change['fullDocument']['user'] ]
                # Append at the top of all followers
                # Get user's followers
                for f in db['useredges'].find({'destination': insert_change['fullDocument']['user']}):
                    followers.append(f['source'])
                    # Get follower activity type
                    if db['users'].find({'_id': f['source'], 'interests': {'$in': insert_change['fullDocument'].get('activityType', [])}}).count() > 0:
                        r.lpush(REDIS_PREFIX + str(f['source']) + ':recommended-high', str(insert_change['fullDocument']['_id']))
                    else:
                        r.lpush(REDIS_PREFIX + str(f['source']) + ':recommended-medium', str(insert_change['fullDocument']['_id']))

                for f in db['users'].find({'_id': {'$nin': followers}}):
                    r.lpush(REDIS_PREFIX + str(f['_id']) + ':recommended-reserve', str(insert_change['fullDocument']['_id']))

                # TODO: add snapshot in order to restart from the previous watch 
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)


def watchDeleteVibes():
    """ Watch `vibes` collection deletation """
    try:
        print('Connecting to the PostgreSQL databse...')
        mongodbUrl = os.getenv('MONGODB_URL')
        client = MongoClient(mongodbUrl)
        #db = client.ahunbackup
        db = client.ahuntest

        # Redis conneciton
        print('Connecting to the Redis databse...')
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'))
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        sys.exit(1)

    while True:
        try:
            # Loop throught every user and try removing the vibe id
            for f in r.scan_iter(REDIS_PREFIX + '*'):
                print(f)

            # TODO: add snapshot in order to restart from the previous watch 
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
                calculateVibeWeight(insert_change)
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)


def watchVibeseen():
    """ Watch `vibeseen` collection and remove vibe from redis """
    while True:
        try:
            for insert_change in db['vibes'].watch(
                [{'$match': {'operationType': 'insert'}}]
            ):
                calculateVibeWeight(insert_change)
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
                calculateVibeWeight(insert_change)
        except pymongo.errors.PyMongoError as ex:
            # TODO: log the execption
            print(ex)


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.submit(watchInsertVibes)
    executor.submit(watchUsers)
    executor.submit(watchUseredges)