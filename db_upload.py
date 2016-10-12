from firebase import firebase
import time
import json
import os
from tqdm import tqdm
def firebase_upload(dns, url, name, document):
    '''
    url_endpoint - destination to store document data
    document - local file location of document
    '''
    client = firebase.FirebaseApplication(dns, None)
    error_count = 0
    while True:
        try:
            if isinstance(document, str):
                with open(os.path.join(os.getcwd(),document)) as f:
                    db_dict = json.load(f)
                    print "pushing large dict.."
                    result = client.put(url, name,
                                      db_dict, params={'print':'silent'})
            elif isinstance(document, dict):
                result = client.put(url, name,
                                    document, params={'print':'silent'})
        except Exception as e:
            print e
            if error_count > 20:
                break
            else:
                error_count +=1
                continue
        else:
            error_count = 0
            break
#start_time = time.time()
firebase_url = 'https://nutritiondb-3314c.firebaseio.com'
#firebase = firebase.FirebaseApplication(url, None)
#result = firebase.get('/USDA_DB/14416', None)
# print ("got %d items " % len(result))
with open('usda_doc.json', 'r') as f:
    food_dict = json.load(f)
    for food, packet in tqdm(food_dict.iteritems()):
        firebase_upload(firebase_url, firebase_url+'/v2/USDA/', food, packet)

