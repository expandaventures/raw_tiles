import raw_tiles.tile_format.msgpack_format as msgpack_format
from raw_tiles.util import st_box2d_for_tile
import gzip
import tempfile


def output_fmt(fmt, conn, table, z, x, y):
    st_box2d = st_box2d_for_tile(z, x, y)

    query = "select osm_id as fid, st_asbinary(way) as wkb, " \
            "to_json(tags) as json from %s where " \
            "way && %s" % (table, st_box2d)

    with conn.cursor() as cur:
        cur.execute(query)
        for row in cur:
            fid = row[0]
            wkb = row[1]
            props = row[2]

            fmt.write(fid, wkb, props)


def output_io(io, conn, table, z, x, y):
    fh = gzip.GzipFile(fileobj=io, mode='wb', compresslevel=9)
    with msgpack_format.write(fh) as fmt:
        output_fmt(fmt, conn, table, z, x, y)
    fh.flush()
    fh.close()


def output_tile(s3, output_bucket, conn, table, z, x, y):
    with tempfile.NamedTemporaryFile() as tmp:
        output_io(tmp, conn, table, z, x, y)
        key = "%s/%d/%d/%d.msgpack.gz" % (table, z, x, y)
        s3.meta.client.upload_file(tmp.name, output_bucket, key)
