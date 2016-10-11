from raw_tiles.output import output_tile
import psycopg2
import boto3
import tempfile


if __name__ == '__main__':
    import sys
    tables = ['planet_osm_point', 'planet_osm_line', 'planet_osm_polygon']

    db_params = sys.argv[1]
    z = int(sys.argv[2])
    output_bucket = sys.argv[3]

    max_coord = 1 << z
    if len(sys.argv) > 4:
        x_range = map(int, sys.argv[4].split('-'))
    else:
        x_range = [0, max_coord]

    conn = psycopg2.connect(db_params)

    s3 = boto3.resource('s3')

    for x in range(*x_range):
        for y in range(0, max_coord):
            for table in tables:
                output_tile(s3, output_bucket, conn, table, z, x, y)
