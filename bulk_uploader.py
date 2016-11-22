import multiprocessing
import json
import firebase

class Uploader:
    '''
    Firebase uploader with multiprocessing capabilities
    '''
    def __init__(self, destination_url, source_file):
        self.dns = destination_url.split('[A-z]+/[A-z]+')[0]
        print self.dns
        self.url = destination_url
        with open(source_file, 'r') as fp:
            self.document_dict = json.load(fp)
        self.MAXCPU = multiprocessing.cpu_count()

    def upload(self, document_tuple):
        '''
        input: ('food_code': 'food_document')
        uploads given dictionary to firebase destination
        '''
        curproc = multiprocessing.current_process()
        client = firebase.FirebaseApplication(self.dns, None)
        return client.put(self.url, document_tuple[0], document_tuple[1],
                            params={'print' : 'silent'})


def mp_upload():
        print '{} documents, {} procs'.format(len(self.document_dict.keys()),
                                            self.MAXCPU)
        pool = multiprocessing.Pool(self.MAXCPU)
        print pool.map(self.upload, self.document_dict.items())
        pool.close()
        pool.join()



if __name__ == '__main__':

    firebase_url = 'https://nutritiondb-3314c.firebaseio.com/v2/USDA'
    document_source = 'usda_food_documentv2.json'

    uploader = Uploader(firebase_url, document_source)
    document_count = len(uploader.document_dict.keys())
    uploader.status = 0
    def func(x, d = Uploader(
        'https://nutritiondb-3314c.firebaseio.com/v2/USDA',
        'usda_food_documentv2.json')):
        curproc = multiprocessing.current_process()
        uploader.status += 1
        print curproc, "Started Process, args={}, {}/{}".format(x[0],
                                                uploader.status, document_count)
        for attempt in range(4):
            try:
                d.upload(x)
                return True
            except Exception:
                print "failed @ attempt %s" % attempt
    MAXCPU = multiprocessing.cpu_count()
    print '{} documents, {} procs'.format(len(uploader.document_dict.keys()),
                                            MAXCPU)
    pool = multiprocessing.Pool(MAXCPU+20)
    try:
        print pool.map(func, uploader.document_dict.items())
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    else:
        pool.close()
    pool.join()


