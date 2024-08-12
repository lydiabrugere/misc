import abc
import logging
import os
import warnings
import boto3
import botocore
import psycopg2
from psycopg2.extensions import AsIs
from orthoimagery_pipeline.common import config
from orthoimagery_pipeline.common import S3Download
from orthoimagery_pipeline.common import GoogleStorageDownload
from orthoimagery_pipeline.common.exceptions import SourceNotFoundException
import orthoimagery_pipeline.define.sql_templates as sql

logger = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

warnings.filterwarnings("ignore",category =UserWarning)


s3 = boto3.resource('s3')
secrets = config.get_secrets()


def parse_date(date_str):
    """

    :param date_str: YYYY-MM-DD
    :return:
    """
    return int(date_str[0:4]) * 10000 + int(date_str[5:7]) * 100 + int(date_str[8:10])


class DefineImageSet(object):
    """
    Abstract base class to provide high-level interface for a DefineImageSet job
    Note this class should not be instantiated directly, use child classes instead
    """

    __metaclass__ = abc.ABCMeta

    sql_companion = sql.NullSQL

    def __init__(self, working_dir, start_day, end_day, continent=None, l0_iso_code=None, cloud_cover=60):
        self.working_dir = working_dir
        self.download_url_csv = self.working_dir + '/' + config.download_url
        self.start_day = parse_date(start_day)
        self.end_day = parse_date(end_day)
        self.continent = continent
        self.l0_iso_code = l0_iso_code
        self.cloud_cover = cloud_cover
        self.conn = None
        self.output_files = [self.download_url_csv]

    @classmethod
    def define_image_set(cls, source, working_dir, start_day, end_day, continent=None, l0_iso_code=None, cloud_cover=60):

        """
        Factory method to create a DefineImageSet child class instance based on 'source' argument value
        """
        for subclass in cls.__subclasses__():
            if hasattr(subclass, 'source') and subclass.source == source:
                return subclass(working_dir, start_day, end_day, continent, l0_iso_code, cloud_cover)

        raise SourceNotFoundException

    ### 
    # Begin sequence of private methods to implement the high-level DefineImageSet algorithm 
    # 
    def _open_connection(self):
        self.conn = psycopg2.connect(config.get_conn_string())

    def _download_scene_list(self):
        logger.info('Downloading scene list from open source ...')
        os.system(self.scene_list())

    def _create_scene_list_table(self):
        try:
            logger.info('Creating scene list table...')
            cur = self.conn.cursor()
            cur.execute(self.create_scene_list())
            self.conn.commit()
            logger.info('Ingesting into scene list table...')
            ingest = self.sql_companion.scene_list_ingest.format(db_host=secrets['DB_HOST'],
                                                                 db_port=secrets['DB_PORT'],
                                                                 cwd=self.working_dir)
            os.system(ingest)
            cur.execute(self.sql_companion.update_scene_list)
            self.conn.commit()
            cur.close()
        except psycopg2.Error or OSError as e:
            logger.exception(
                'ERROR: when try to create a new scene list table in imagery schema and ingest above downloaded '
                'data to PG DB. Error: {}'.format(e))
            raise e

    def _create_des_aoi_table(self):
        if self.continent is not None and self.l0_iso_code is None:
            para = tuple(self.continent)
            aoi_sql_params = AsIs('continent'), para, AsIs('continent'), para
        elif self.l0_iso_code is not None and self.continent is None :
            para = tuple(self.l0_iso_code)
            aoi_sql_params = AsIs('l0_iso_code'), para, AsIs('l0_iso_code'), para
        else:
            logger.error('ERROR: Redefine your regions of interest. Either continent/continents or contry/contries need to be defined, but not both at the same time')
            exit(1)
        try:
            logger.info('Creating AOI geometry and its corresponding satellite girds table ...')
            cur = self.conn.cursor()
            cur.execute(sql.create_des_aoi, aoi_sql_params)
            self.conn.commit()
            cur.execute(sql.update_des_aoi)
            self.conn.commit()
            cur.close()
        except psycopg2.Error or OSError as e:
            logger.exception('ERROR: when try to create a table of dynamic AOI! Error: {}'.format(e))
            raise e

    def _create_sat_grid(self):
        try:
            logger.info('Creating Satellite path&row or grid table...')
            cur = self.conn.cursor()
            cur.execute(self.sql_companion.create_sat_grid)
            self.conn.commit()
            cur.close()
        except psycopg2.Error or OSError as e:
            logger.exception(
                'ERROR: when try to create a table of Landsat path&row or Sentinel grid table! Error: {}'.format(e))
            raise e
        '''
        update imagery.des_aoi_grid by adding path/rows from fts fields in SA
        '''
        if self.continent == ['SA'] and self.l0_iso_code is None:
            try:
                logger.info('Appending Satellite path&row for FTS fields in SA...')
                cur = self.conn.cursor()
                cur.execute(self.sql_companion.upsert_sat_grid)
                self.conn.commit()
                cur.close()
            except psycopg2.Error or OSError as e:
                logger.exception(
                    'ERROR: when try to append Satellite path&row for FTS fields in SA! Error: {}'.format(e))
                raise e
        else:
            pass
        try:
            logger.info('Updating Satellite path&row or grid table...')
            cur = self.conn.cursor()
            cur.execute(self.sql_companion.update_sat_grid)
            self.conn.commit()
            cur.close()
        except psycopg2.Error or OSError as e:
            logger.exception(
                'ERROR: when try to update a table of Landsat path&row or Sentinel grid table! Error: {}'.format(e))
            raise e

    def _create_download_url(self):
        try:
            logger.info('Creating download url table...')
            cur = self.conn.cursor()
            cur.execute(self.sql_companion.download_url, self.data)
            self.conn.commit()
            cur.close()
        except psycopg2.Error or OSError as e:
            logger.exception('ERROR: when try to create a table of the new download list! Error: {}'.format(e))
            raise e

    def _save_url_csv(self):
        logger.info('Saving url csv...')
        save_url_csv = sql.save_url_csv.format(db_host=secrets['DB_HOST'],
                                               db_port=secrets['DB_PORT'],
                                               cwd=self.working_dir,
                                               download_url=config.download_url)
        try:
            os.system(save_url_csv)
        except OSError as e:
            logger.exception('Could not create download_url csv from pg table! Error: {}'.format(e))
            raise e

    def _csv_to_s3(self):

        logger.info('Uploading CSV to S3...')
        filename = config.download_url
        num_urls = sum(1 for line in open(filename))
        logger.info('download list: %s', num_urls)
        try:
            response = s3.meta.client.head_bucket(Bucket=config.OUTPUT_BUCKET)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                s3.meta.client.upload_file(self.download_url_csv, config.OUTPUT_BUCKET,
                                           self.s3_define_folder + filename,
                                           ExtraArgs={'ServerSideEncryption': "AES256"})
                logger.info("image set csv has been uploaded to S3.")
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                logger.exception("403: Access to the bucket is denied! Error: {}".format(e))
                raise e
            elif error_code == 404:
                logger.exception("404: Bucket does not exist! Error: {}".format(e))
                raise e
        return

    def _remove_db_tables(self):

        # remove scene list tables
        try:
            logger.info('Removing temporary tables from the database')
            cur = self.conn.cursor()
            cur.execute(self.sql_companion.scene_list_exists)
            self.conn.commit()
            cur.execute(sql.rm_des_aoi_download_tables)
            self.conn.commit()
        except psycopg2.Error as e:
            logger.exception('ERROR: when try to clean up postres DB. Error: {}'.format(e))
            raise e

    def _clean_up(self):
        self.conn.close()

        # clean up intermediary files written to disk
        for output in self.output_files:
            if os.path.isfile(output):
                os.remove(output)

    ### 
    # end steps to the high-level algorithm
    #

    def __call__(self):
        """
        call procedure remains the same sequence of method calls for all DefineImageSet implementations
        Only need to implement the abstract properties per child class
        """
        self._open_connection()
        self._download_scene_list()
        self._create_scene_list_table()
        self._create_des_aoi_table()
        self._create_sat_grid()
        self._create_download_url()
        self._save_url_csv()
        self._csv_to_s3()
        self._remove_db_tables()
        self._clean_up()

    def scene_list(self):
        return self.sql_companion.scene_list.format(cwd=self.working_dir)

    ### 
    # abstract methods/properties to be overriden by child classes 
    # 

    @abc.abstractmethod
    def create_scene_list(self):
        raise NotImplementedError

    @property
    def data(self):
        raise NotImplementedError

    @property
    def s3_define_folder(self):
        raise NotImplementedError

        ### 
        # end abstract properties
        #


class L7ImageSet(DefineImageSet):
    scene_gs_loc = 'gs://gcp-public-data-landsat/index.csv.gz'
    source = 'l7'
    sql_companion = sql.L7SQL

    def scene_list(self):
        self.output_files.extend(['index.csv.gz', 'index.csv', 'google_l7_index.csv'])

        gs = GoogleStorageDownload()
        gs.download(self.scene_gs_loc, self.working_dir)

        return super(L7ImageSet, self).scene_list()

    def create_scene_list(self):
        return self.sql_companion.create_scene_list

    @property
    def data(self):
        return AsIs(self.start_day), AsIs(self.end_day)

    @property
    def s3_define_folder(self):
        return config.L7_DEFINE_FOLDER + 'out/'


class L8ImageSet(DefineImageSet):
    source = 'l8'
    sql_companion = sql.L8SQL

    def scene_list(self):
        self.output_files.extend(['scene_list.gz', 'scene_list', 'aws_l8_scene_list'])

        # aws s3 cp s3://landsat-pds/c1/L8/scene_list.gz {cwd};
        s3_scene_file = 's3://landsat-pds/c1/L8/scene_list.gz'
        sd = S3Download()
        sd.download(s3_scene_file, self.working_dir, overwrite=True)

        return super(L8ImageSet, self).scene_list()

    def create_scene_list(self):
        return self.sql_companion.create_scene_list

    @property
    def data(self):
        return AsIs(self.start_day), AsIs(self.end_day), AsIs(self.cloud_cover)


    @property
    def s3_define_folder(self):
        return config.L8_DEFINE_FOLDER + 'out/'


class SentinelImageSet(DefineImageSet):
    scene_gs_loc = 'gs://gcp-public-data-sentinel-2/index.csv.gz'
    source = 'sentinel'
    sql_companion = sql.SentinelSQL

    def scene_list(self):
        self.output_files.extend(['index.csv.gz', 'index.csv', 'google_sentinel2_index.csv'])

        gs = GoogleStorageDownload()
        gs.download(self.scene_gs_loc, self.working_dir)

        return super(SentinelImageSet, self).scene_list()

    def create_scene_list(self):
        return self.sql_companion.create_scene_list

    @property
    def data(self):
        return AsIs(self.start_day), AsIs(self.end_day)

    @property
    def s3_define_folder(self):
        return config.SENTINEL_DEFINE_FOLDER + 'out/'
