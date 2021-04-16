import os
import sys
import time
import redis
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from resource import getrusage, RUSAGE_SELF

load_dotenv()

"""
    Interests Score
"""
DEFAULT_INTEREST_WEIGHT = 0.1
FOLLOW_WEIGHT = 0.1
DEFAULT_WEIGHT = 0.1
FOLLOWING_WEIGHT = 0.25
FOLLOWING_INTEREST_WEIGHT = 0.5

try:
    print('Connecting to the PostgreSQL databse...')
    mongodbUrl = os.getenv('MONGODB_URL')
    client = MongoClient(mongodbUrl)
    db = client.ahunbackup

    # Redis conneciton
    print('Connecting to the Redis databse...')
    r = redis.Redis(host='127.0.0.1', port=6379)
except Exception as err:
    print(err)
    sys.exit(1)


start = time.perf_counter()
"""
    Start all mesurable process
"""

users_collection = db['users']
# TODO: To optimize select users that have followers
users = users_collection.find({}).limit(1)

for user in users:
    follow_weight = FOLLOW_WEIGHT
    total_weight = 0
    recommended_vibes = [] # contains follwing's not seen vibes

    # User's interests
    interests = [str(f) for f in user.get('interests', [])]
    
    # Get user's already seen vibes
    seen_vibes = [str(v['_id']) for v in db['vibeseens'].find({'userId': user['_id']})]
    
    # Get non blocked following
    following = [str(f['_id']) for f in db['useredges'].find({'source': user['_id'], 'request': 'FOLLOW'})]
    
    #vibes = db['vibes'].find({'_id': {'$nin': seen_vibes}})

    # Get not-seen vibes from followed user's and that are also user's interests
    vibes_followed_interests = [{'vibe_id': f, 'weight': FOLLOWING_INTEREST_WEIGHT} for f in db['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': {'$in': following}, 'activityType': {'$in': interests}})]

    # Get vibes that are not in interests
    vibes_followed = [{'vibe_id': f, 'weight': FOLLOWING_WEIGHT} for f in db['vibes'].find({'_id': {'$nin': seen_vibes}, 'user': {'$in': following}})]

    not_in_vibes = seen_vibes + vibes_followed + vibes_followed_interests

    other_vibes = [json.dumps({'vibe_id': str(f['_id']), 'weight': DEFAULT_WEIGHT}) for f in db['vibes'].find({'_id': {'$nin': not_in_vibes}})]

    recommended_vibes = other_vibes
    print(recommended_vibes)

    user_id = "user:" + str(user['_id']) + ":recommended"
    # Store the recommendation to redis
    r.lpush(user_id, *recommended_vibes)
    

"""
    End of all precess
"""
finish = time.perf_counter()

print(f'Finished in {round(finish-start, 2)}')
print("Peak memory (MiB):",
      int(getrusage(RUSAGE_SELF).ru_maxrss / 1024))