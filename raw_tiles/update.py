from raw_tiles.output import output_tile
import psycopg2
import boto3
import tempfile
import multiprocessing as mp
import json
import logging


class Expiries(object):
    def __init__(self, z):
        self.s3 = boto3.resource('s3')
        self.z = z
        self.tiles = set()

    def process_message(self, message):
        for record in message.get('Records', []):
            event_name = record['eventName']
            if 'ObjectCreated' in event_name:
                source_bucket = record['s3']['bucket']['name']
                key = record['s3']['object']['key']
                self.add_tile_list(source_bucket, key)

    def add_tile_list(self, source_bucket, key):
        with tempfile.TemporaryFile() as tmp:
            bucket = self.s3.Bucket(source_bucket)
            bucket.download_fileobj(key, tmp)
            tmp.seek(0)

            # format of tile list is lines of z/x/y
            for line in tmp:
                z, x, y = map(int, line.split('/'))
                x >>= (z - self.z)
                y >>= (z - self.z)
                self.tiles.add((x, y))


# http://stackoverflow.com/questions/10117073/how-to-use-initializer-to-set-up-my-multiprocess-pool
worker_context = None
def worker_init(tables, db_params, z, output_bucket):
    global worker_context
    logger = mp.log_to_stderr()
    logger.setLevel(logging.INFO)
    worker_context = {
        'tables': tables,
        'conn': psycopg2.connect(db_params),
        's3': boto3.resource('s3'),
        'output_bucket': output_bucket,
        'z': z,
        'logger': logger
    }


def worker(job):
    global worker_context

    try:
        (x, y) = job
        logger = worker_context['logger']
        z = worker_context['z']
        for table in worker_context['tables']:
            output_tile(worker_context['s3'], worker_context['output_bucket'],
                        worker_context['conn'], table, z, x, y)
            logger.info('Rendered: %s/%d/%d/%d' % (table, z, x, y))

    except Exception, e:
        logger.error('Failed: %s' % e.message)

    return True


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tables", action='append',
                        default=[], help="dump these tables")
    parser.add_argument("-z", "--zoom", type=int, default=10,
                        help="Zoom level to generate tiles at")
    parser.add_argument("-j", "--workers", type=int, default=1,
                        help="Number of concurrent workers to use")
    parser.add_argument("db", help="Database connection string")
    parser.add_argument("bucket", help="S3 bucket to put generated tiles in")
    parser.add_argument("queue", help="SQS queue URL to listen for updates")
    args = parser.parse_args()

    sqs = boto3.resource('sqs')
    sqs_queue = sqs.Queue(args.queue)

    # set default if nothing was provided on the command line.
    tables = args.tables
    if not tables:
        tables = ['planet_osm_point', 'planet_osm_line', 'planet_osm_polygon']

    pool = mp.Pool(processes=args.workers, initializer=worker_init,
                   initargs=(args.tables, args.db, args.zoom, args.bucket))

    while True:
        exp = Expiries(args.zoom)

        messages = sqs_queue.receive_messages()
        for message in messages:
            data = json.loads(message.body)
            exp.process_message(data)

        jobs = []
        for (x, y) in exp.tiles:
            import sys
            print>>sys.stderr, "Queueing %d/%d/%d" % (args.zoom, x, y)
            jobs.append(pool.apply_async(worker, ((x, y),)))

        for j in jobs:
            j.get()

        if messages:
            entries = []
            for m in messages:
                entries.append({'Id': m.message_id,
                                'ReceiptHandle': m.receipt_handle})
            sqs_queue.delete_messages(Entries=entries)
