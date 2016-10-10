from firebase import firebase
import time
import json
import os
start_time = time.time()
firebase = firebase.FirebaseApplication('https://nutritiondb-3314c.firebaseio.com/', None)
#result = firebase.get('/USDA_DB/14416', None)
# print ("got %d items " % len(result))
with open(os.path.join(os.getcwd(),'usda_document.json')) as f:
    db_dict = json.load(f)
    print "pushing large dict.."
    result = firebase.put('v2/','USDA_DB', db_dict, params={'print':'silent'})

# result = firebase.delete('v1/','USDA_DB')
print result
print("--- request completed in %s seconds ---" % (time.time() - start_time))
