"""
Module to hold static SQL statements for improved readability in the rest of the program
"""

# copy `imagery.download_url` table from postgres to a csv file in disk;
# `imagery.download_url` is the imgage set to be downloaded; format: download_url: text; cloud_cover: numeric
save_url_csv = """
            psql -h {db_host} -p {db_port} -U geo_admin -d data -c "\COPY (select * from imagery.download_url) TO {cwd}/{download_url} (format csv, delimiter ',');"
"""

# Dynamically create AOI in postgres by querying desired fields/plots layers (e.g. pfo sites/subsites and production fields)
"""
:param des_aoi: desired aoi table
:param des_continent: desired continent; default: 'SA'
"""

create_des_aoi = """
            CREATE TABLE imagery.des_aoi as(
            (select distinct A.geom from velocity_pfo.sites A
            JOIN (select geom from geopolitical.world_l0 where %s in %s) AS B
            ON ST_INTERSECTS(A.geom, B.geom)
            AND A.deleted = 'F')
            Union(
            select distinct A.geom from velocity_pfo.production_fields A
            JOIN (select * from geopolitical.world_l0 where %s in %s) AS B
            ON ST_INTERSECTS(A.geom, B.geom)
            AND A.deleted = 'F'));
"""
# correct geometry of invalid all aoi if exists
update_des_aoi = """
            ALTER table imagery.des_aoi add column id serial;
            UPDATE imagery.des_aoi set geom = st_multi(st_collectionextract(st_makevalid(geom), 3)) where id in (select id from imagery.des_aoi where st_isvalid(geom) = false);
"""
##### end: dynamically create AOI in postgres by querying pfo sites/subsites and production fields

# identify if imagery.download_url exits then drop the table
rm_des_aoi_download_tables = """
            SELECT to_regclass('imagery.download_url'); DROP TABLE imagery.download_url;
            SELECT to_regclass('imagery.des_aoi'); DROP TABLE imagery.des_aoi;
            SELECT to_regclass('imagery.des_aoi_grid'); DROP TABLE imagery.des_aoi_grid;
            """

class L7SQL(object):
    # download index csv file of available Landsat data, unzip and rename it

    scene_list = "gunzip -f {cwd}/index.csv.gz && mv {cwd}/index.csv {cwd}/google_l7_index.csv"

    # create table in postgres for the scene list
    create_scene_list = """
    CREATE TABLE imagery.google_l7_scene_list (
                SCENE_ID TEXT,
                PRODUCT_ID TEXT,
                SPACECRAFT_ID TEXT,
                SENSOR_ID TEXT,
                DATE_ACQUIRED TEXT,
                COLLECTION_NUMBER TEXT,
                COLLECTION_CATEGORY TEXT,
                SENSING_TIME TIMESTAMP,
                DATA_TYPE TEXT,
                PATH INTEGER,
                ROW INTEGER,
                CLOUD_COVER NUMERIC,
                NORTH_LAT NUMERIC,
                SOUTH_LAT NUMERIC,
                WEST_LON NUMERIC,
                EAST_LON NUMERIC,
                TOTAL_SIZE BIGINT,
                BASE_URL TEXT)
    """
    # ingest data of the scene list to the table in postgres
    scene_list_ingest = """
                psql -h {db_host} -p {db_port} -U geo_admin -d data -c "\COPY imagery.google_l7_scene_list FROM '{cwd}/google_l7_index.csv' delimiter ',' csv header;"
    """
    # delete non L7 scenes and create acquisition time column for each scene as well as create indexes
    update_scene_list = """
                DELETE FROM imagery.google_l7_scene_list WHERE NOT (SPACECRAFT_ID = 'LANDSAT_7');
                ALTER table imagery.google_l7_scene_list add path1 text; 
                UPDATE imagery.google_l7_scene_list A set path1= to_char(path::integer, 'fm000');
                ALTER table imagery.google_l7_scene_list drop column path;
                ALTER table imagery.google_l7_scene_list rename path1 to path;
                ALTER table imagery.google_l7_scene_list add row1 text; 
                UPDATE imagery.google_l7_scene_list set row1= to_char("row"::integer, 'fm000');
                ALTER table imagery.google_l7_scene_list drop column row;
                ALTER table imagery.google_l7_scene_list rename row1 to "row";
                CREATE INDEX google_l7_path ON imagery.google_l7_scene_list USING btree (path);
                CREATE INDEX google_l7_row ON imagery.google_l7_scene_list USING btree (row);
                CREATE INDEX google_l7_date ON imagery.google_l7_scene_list USING btree (SENSING_TIME);
    """

    ##### start: get landsat path/row from wrs by spatial join of des_aoi
    create_sat_grid = """
            CREATE TABLE imagery.des_aoi_grid as (
            select distinct A.path, A."row", A.geom from imagery.wrs_landsat A
            JOIN imagery.des_aoi AS B
            ON ST_INTERSECTS(A.geom, B.geom))
    """
    ##### end: get landsat path/row from wrs by spatial join of des_aoi

    ##### start: get landsat path/row from wrs by spatial join of sa_fieldsfts points (static table in np) if continent is SA
    update_sat_grid = """
            ALTER table imagery.des_aoi_grid add path1 text; 
            UPDATE imagery.des_aoi_grid A set path1= to_char(path::integer, 'fm000');
            ALTER table imagery.des_aoi_grid drop column path;
            ALTER table imagery.des_aoi_grid rename path1 to path;
            ALTER table imagery.des_aoi_grid add row1 text; 
            UPDATE imagery.des_aoi_grid set row1= to_char("row"::integer, 'fm000');
            ALTER table imagery.des_aoi_grid drop column row;
            ALTER table imagery.des_aoi_grid rename row1 to "row";
            CREATE INDEX des_aoi_grid_path ON imagery.des_aoi_grid USING btree (path);
            CREATE INDEX des_aoi_grid_row ON imagery.des_aoi_grid USING btree (row);
    """
    ##### end: get landsat path/row from wrs by spatial join of sa_fieldsfts points (static table in np) and pfo polygons in SA (dynamic from above)

    # create `imagery.download_url` for AOI;  format: download_url: text; cloud_cover: numeric
    download_url = """
    CREATE TABLE imagery.download_url as (
                select distinct C.base_url as download_url FROM (SELECT distinct A.base_url, 'l7' || A.path || A.row || substring(A.sensing_time::text,1,4)|| substring(A.sensing_time::text,6,2)|| substring(A.sensing_time::text,9,2) as scene_id
                from imagery.google_l7_scene_list A, imagery.des_aoi_grid B
                WHERE A.path = B.path
                AND A.row = B.row
                AND (substring(A.sensing_time::text,1,4)|| substring(A.sensing_time::text,6,2)|| substring(A.sensing_time::text,9,2))::integer >= %s
                AND (substring(A.sensing_time::text,1,4)|| substring(A.sensing_time::text,6,2)|| substring(A.sensing_time::text,9,2))::integer < %s) C
                WHERE C.scene_id not in 
                (select substring(location,59,16) from imagery.evi where location ilike '%%2018%%'));

    """
    # this query identifies if imagery.google_l7_scene_list exits then drop the table
    scene_list_exists = """
                SELECT to_regclass('imagery.google_l7_scene_list'); 
                DROP TABLE imagery.google_l7_scene_list;
    """

class L8SQL(object):
    # download index scene list file of available Landsat8 data, unzip and rename it

    scene_list = "gunzip -f {cwd}/scene_list.gz && mv {cwd}/scene_list {cwd}/aws_l8_scene_list"

    # create table in postgres for the scene list
    create_scene_list = """
    CREATE TABLE imagery.aws_l8_scene_list(
                    productid text,
                    entityid text, 
                    acquisitiondate text, 
                    cloud_cover numeric, 
                    preocssinglevel text, 
                    path integer, 
                    "row" integer, 
                    min_lat numeric, 
                    min_lon numeric, 
                    max_lat numeric, 
                    max_lon numeric, 
                    download_url text) 
    """
    # ingest data of the scene list to the table in postgres
    scene_list_ingest = """
                psql -h {db_host} -p {db_port} -U geo_admin -d data -c "\COPY imagery.aws_l8_scene_list FROM '{cwd}/aws_l8_scene_list' delimiter ',' csv header;"
    """
    # alter path&row datatypes to text and create indexes for the scene list table
    update_scene_list = """
                ALTER table imagery.aws_l8_scene_list add path1 text; 
                UPDATE imagery.aws_l8_scene_list A set path1= to_char(path::integer, 'fm000');
                ALTER table imagery.aws_l8_scene_list drop column path;
                ALTER table imagery.aws_l8_scene_list rename path1 to path;
                ALTER table imagery.aws_l8_scene_list add row1 text; 
                UPDATE imagery.aws_l8_scene_list set row1= to_char("row"::integer, 'fm000');
                ALTER table imagery.aws_l8_scene_list drop column row;
                ALTER table imagery.aws_l8_scene_list rename row1 to "row";
                CREATE INDEX aws_l8_path ON imagery.aws_l8_scene_list USING btree (path);
                CREATE INDEX aws_l8_row ON imagery.aws_l8_scene_list USING btree (row);
                CREATE INDEX aws_l8_date ON imagery.aws_l8_scene_list USING btree (acquisitiondate);
    """

    ##### start: get landsat path/row from wrs by spatial join of des_aoi
    create_sat_grid = """
            CREATE TABLE imagery.des_aoi_grid as (
            select distinct A.path, A."row", A.geom from imagery.wrs_landsat A
            JOIN imagery.des_aoi AS B
            ON ST_INTERSECTS(A.geom, B.geom))
    """
    ##### end: get landsat path/row from wrs by spatial join of des_aoi

    ##### start: update imagery.des_aoi_grid by adding path/rows from fts fields in SA
    upsert_sat_grid = """
            INSERT INTO imagery.des_aoi_grid(path, "row", geom)
            SELECT distinct A.path, A."row", A.geom from imagery.wrs_landsat A
            JOIN imagery.sa_fieldsfts AS B
            ON ST_INTERSECTS(A.geom, B.geom)
    """
    ##### end: update imagery.des_aoi_grid by adding path/rows from fts fields in SA


    ##### start: get landsat path/row from wrs by spatial join of sa_fieldsfts points (static table in np) if continent is SA
    update_sat_grid = """
            ALTER table imagery.des_aoi_grid add path1 text; 
            UPDATE imagery.des_aoi_grid A set path1= to_char(path::integer, 'fm000');
            ALTER table imagery.des_aoi_grid drop column path;
            ALTER table imagery.des_aoi_grid rename path1 to path;
            ALTER table imagery.des_aoi_grid add row1 text; 
            UPDATE imagery.des_aoi_grid set row1= to_char("row"::integer, 'fm000');
            ALTER table imagery.des_aoi_grid drop column row;
            ALTER table imagery.des_aoi_grid rename row1 to "row";
            CREATE INDEX des_aoi_grid_path ON imagery.des_aoi_grid USING btree (path);
            CREATE INDEX des_aoi_grid_row ON imagery.des_aoi_grid USING btree (row);
    """
    ##### end: get landsat path/row from wrs by spatial join of sa_fieldsfts points (static table in np) and pfo polygons in SA (dynamic from above)


    # create `imagery.download_url` for AOI,  format: download_url: text; cloud_cover: numeric
    download_url = """
                CREATE table imagery.download_url as (
                select distinct C.download_url FROM (SELECT distinct A.download_url, 'l8' || A.path || A.row || substring(A.acquisitiondate,1,4)|| substring(A.acquisitiondate,6,2)|| substring(A.acquisitiondate,9,2) as scene_id
                from imagery.aws_l8_scene_list A, imagery.des_aoi_grid B
                WHERE A.path = B.path
                AND A.row = B.row
                AND (substring(A.acquisitiondate,1,4)|| substring(A.acquisitiondate,6,2)|| substring(A.acquisitiondate,9,2))::integer >= %s
                AND (substring(A.acquisitiondate,1,4)|| substring(A.acquisitiondate,6,2)|| substring(A.acquisitiondate,9,2))::integer < %s
                AND A.cloud_cover <= %s) C
                WHERE C.scene_id not in 
                (select substring(reverse(split_part(reverse(location), '/', 1)), 1, 16) from imagery.evi
                where (substring(capture_date::text,1,4) || substring(capture_date::text,6,2) || substring(capture_date::text,9,2))::integer >= 20180101)
                );
    """
    # this query identifies if imagery.google_l7_scene_list exits then drop the table
    scene_list_exists = """
                SELECT to_regclass('imagery.aws_l8_scene_list');
                DROP TABLE imagery.aws_l8_scene_list;
    """

class SentinelSQL(object):
    # download index csv file of available Landsat data, unzip and rename it
    scene_list = """
                gunzip -f {cwd}/index.csv.gz && mv {cwd}/index.csv {cwd}/google_sentinel2_index.csv
    """
    # create table in postgres for the scene list
    create_scene_list = """
                CREATE TABLE imagery.google_sentinel2_index(
                            granule_id text, 
                            productid text, 
                            DATATAKE_IDENTIFIER text,
                            MGRS_TILE text, 
                            SENSING_TIME text,
                            TOTAL_SIZE integer,
                            CLOUD_COVER numeric,
                            GEOMETRIC_QUALITY_FLAG text,
                            GENERATION_TIME text,
                            NORTH_LAT numeric,
                            SOUTH_LAT numeric,
                            WEST_LON numeric, 
                            EAST_LON numeric, 
                            BASE_URL text)
    """
    # ingest data of the scene list to the table in postgres
    scene_list_ingest = """
                psql -h {db_host} -p {db_port} -U geo_admin -d data -c "\COPY imagery.google_sentinel2_index FROM '{cwd}/google_sentinel2_index.csv' delimiter ',' csv header;"
    """
    # create acquisition time column for each scene as well as create indexes
    update_scene_list = """
                CREATE INDEX google_sentinel2_mgrs_tile ON imagery.google_sentinel2_index USING btree (mgrs_tile);
    """
    ##### start: get sentinel grid from sentinel2grid by spatial join of des_aoi
    create_sat_grid = """
            CREATE TABLE imagery.des_aoi_grid as (
            select distinct A.name as mgrs_tile, A.geom from imagery.sentinel2grid A
            JOIN imagery.des_aoi AS B
            ON ST_INTERSECTS(A.geom, B.geom))
    """
    ##### end: get landsat path/row from wrs by spatial join of des_aoi

    ##### start: get landsat path/row from wrs by spatial join of sa_fieldsfts points (static table in np) if continent is SA
    update_sat_grid = """
            CREATE INDEX des_aoi_grid_index ON imagery.des_aoi_grid USING btree (mgrs_tile)
    """
    # create `imagery.download_url` for AOI,  format: download_url: text; cloud_cover: numeric
    download_url = """
            CREATE TABLE imagery.download_url as (
            select distinct C.base_url as download_url FROM (SELECT distinct A.base_url, 's2' || A.mgrs_tile || substring(A.generation_time::text,1,4)|| substring(A.generation_time::text,6,2)|| substring(A.generation_time::text,9,2) as scene_id
            from imagery.google_sentinel2_index A, imagery.des_aoi_grid B
            WHERE A.mgrs_tile = B.mgrs_tile
            AND (substring(A.generation_time::text,1,4)|| substring(A.generation_time::text,6,2)|| substring(A.generation_time::text,9,2))::integer >= %s
            AND (substring(A.generation_time::text,1,4)|| substring(A.generation_time::text,6,2)|| substring(A.generation_time::text,9,2))::integer < %s) C
            WHERE C.scene_id not in 
            (select substring(location,59,16) from imagery.evi where location ilike '%%2018%%'));
    """
    # this query identifies if imagery.google_sentinel2_scene_list exits then drop the table
    scene_list_exists = """
                SELECT to_regclass('imagery.google_sentinel2_index'); 
                DROP TABLE imagery.google_sentinel2_index;
    """


class NullSQL(object):
    @property
    def scene_list(self):
        raise NotImplementedError

    @property
    def create_scene_list(self):
        raise NotImplementedError

    @property
    def scene_list_ingest(self):
        raise NotImplementedError

    @property
    def update_scene_list(self):
        raise NotImplementedError

    @property
    def download_url(self):
        raise NotImplementedError

    @property
    def scene_list_exists(self):
        raise NotImplementedError

    @property
    def create_sat_grid(self):
        raise NotImplementedError