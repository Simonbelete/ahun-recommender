import os
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


while True:
    try:
        for insert_change in db.collection.watch(
            [{'$match': {'operationType': 'insert'}}]
        ):
            print(insert_change)
    except pymongo.errors.PyMongoError as ex:
        print(ex)