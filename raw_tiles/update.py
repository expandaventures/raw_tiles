from raw_tiles.output import output_tile
import psycopg2
import boto3
import tempfile
import multiprocessing as mp


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
        obj = self.s3.get_object(Bucket=source_bucket, Key=key)
        body = obj['Body']
        # format of tile list is lines of z/x/y
        for line in body:
            z, x, y = map(int, line.split('/'))
            x >>= (self.z - z)
            y >>= (self.z - z)
            self.tiles.add((x, y))


def worker(tables, db_params, z, output_bucket, queue):
    conn = psycopg2.connect(db_params)
    s3 = boto3.resource('s3')

    try:
        for (x, y) in iter(queue.get, 'END'):
            for table in tables:
                output_tile(s3, output_bucket, conn, table, z, x, y)

    except Exception, e:
        logger = mp.log_to_stderr()
        logger.error('Failed: %s' % e.message)

    return True


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tables", action='append',
                        default=['planet_osm_point', 'planet_osm_line',
                                 'planet_osm_polygon'],
                        help="dump these tables")
    parser.add_argument("-z", "--zoom", type=int, default=10,
                        help="Zoom level to generate tiles at")
    parser.add_argument("-j", "--workers", type=int, default=1,
                        help="Number of concurrent workers to use")
    parser.add_argument("db", help="Database connection string")
    parser.add_argument("bucket", help="S3 bucket to put generated tiles in")
    parser.add_argument("queue", help="SQS queue URL to listen for updates")
    args = parser.parse_args()

    sqs = boto3.client('sqs')
    sqs_queue = sqs.Queue(args.queue)

    queue = mp.Queue()
    pool = mp.Pool(processes=args.workers)
    for i in range(0, args.workers):
        pool.apply_asyc(worker, (args.tables, args.db, args.zoom, args.bucket,
                                 queue))

    try:
        while True:
            exp = Expiries(z)

            messages = sqs_queue.receive_messages()
            for message in messages:
                exp.process_message(message)

            for (x, y) in exp.tiles:
                queue.put((x, y))

            sqs_queue.delete_messages(Entries=messages)

    except Exception, e:
        for i in range(0, num_workers):
            queue.put('END')
        raise
