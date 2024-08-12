import logging
import os
from copy import deepcopy

import psycopg2
import rasterio
import requests
from shapely import geometry as shg

from orthoimagery_pipeline.common import config
from parsers import *

logger = logging.getLogger(__name__)

logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('rasterio').setLevel(logging.CRITICAL)

date_parser = SentinelDateParser(LandsatDateParser(NullDateParser()))
secrets = config.get_secrets()
headers_base = {'username': secrets['GS_USERNAME'], 'roles': secrets['GS_ROLES']}


def get_gs_headers(content_type):
    """
    Create a copy headers_base with the addition of content-type
    :param content_type: Value to associate with "Content-type" key
    :return: updated dictionary
    """
    new_headers = deepcopy(headers_base)
    new_headers['Content-type'] = content_type

    return new_headers


def post_granule(url, workspace, coverage, granule_path):
    """
    Posts a granule path to GeoServer REST API
    :param url: coverage url
    :param workspace: workspace where granule coveragestore is located
    :param coverage: coverage mosaic where granule will be published
    :param granule_path: path to granule file on disk
    :return: None
    """

    data = 'file://' + granule_path

    r = requests.post(url,
                      headers=get_gs_headers('text/plain'),
                      data=data)

    assert r.status_code in (200, 201, 202)

    return


def put_bbox_update(url, data):
    """
    Updates a coveragestore bbox via GeoServer REST API
    :param url: coverage url
    :param data: data to to send with PUT
    :return: None
    """

    r = requests.put(url,
                     headers=get_gs_headers('application/xml'),
                     data=data)

    assert r.status_code in (200, 201, 202)

    return


class PublishingService:
    """
    :param host: GeoServer host
    :param port: GeoServer port
    :return: None

    """
    coverage_endpoint = '/geoserver/rest/workspaces/{workspace}/coveragestores/{coverage}'
    feature_endpoint = '/geoserver/rest/workspaces/{workspace}/datastores/imagery/featuretypes/{featuretypes}.xml'
    coverage_bbox_data = '<coverage><enabled>true</enabled></coverage>'
    feature_bbox_data = '<featureType><enabled>true</enabled></featureType>'

    def __init__(self, host=secrets['GS_HOST'], port=None):
        self.coverage_base = 'http://{host}:{port}'.format(host=host, port=port) + self.coverage_endpoint if port \
            else 'http://{host}'.format(host=host) + self.coverage_endpoint
        self.feature_base = 'http://{host}:{port}'.format(host=host, port=port) + self.feature_endpoint if port \
            else 'http://{host}'.format(host=host) + self.feature_endpoint
        self.conn = None

    def _get_granule_url(self, workspace, coverage):
        return self.coverage_base.format(workspace=workspace, coverage=coverage) + '/external.imagemosaic'

    def _get_bbox_update(self, workspace, coverage):
        return self.coverage_base.format(workspace=workspace,
                                         coverage=coverage) + '/coverages/{coverage}.xml?recalculate=nativebbox,latlonbbox'.format(
            coverage=coverage)

    def _get_footprint_bbox_update(self, workspace, coverage):
        featuretypes = coverage + '_footprint'
        return self.feature_base.format(workspace=workspace,
                                        featuretypes=featuretypes) + '?recalculate=nativebbox,latlonbbox'.format(
            featuretypes=featuretypes)

    def publish_granule(self, workspace, coverage, granule_path):
        """
        Publishes a single granule to an imagemosaic via GeoServer REST
        :param workspace: workspace where mosaic coveragestore reside
        :param coverage: name of coverage to add granule to
        :param granule_path: path to granule
        :return: None
        """
        granule_url = self._get_granule_url(workspace, coverage)
        bbox_update_url = self._get_bbox_update(workspace, coverage)
        footprint_update_url = self._get_footprint_bbox_update(workspace, coverage)
        post_granule(granule_url, workspace, coverage, granule_path)
        put_bbox_update(bbox_update_url, data=self.coverage_bbox_data)
        put_bbox_update(footprint_update_url, data=self.feature_bbox_data)

    def publish_batch(self, datadir, workspace, tablename, source):
        """
        Batch publish a directory of granules by updating mosaic index table manually
        :param datadir: path of directory containing granules
        :param workspace: workspace where mosaic coveragestore resides
        :param tablename: name of table to do batch insert (usually the same as the name of coverage)
        """
        self._open_connection()
        granules = self._get_granules_from_dir(datadir)
        self._publish_db(granules, tablename, source)
        self._close_connection()
        bbox_update_url = self._get_bbox_update(workspace, tablename)
        footprint_update_url = self._get_footprint_bbox_update(workspace, tablename)
        put_bbox_update(bbox_update_url, data=self.coverage_bbox_data)
        put_bbox_update(footprint_update_url, data=self.feature_bbox_data)

    @staticmethod
    def _get_granules_from_dir(datadir):
        return [os.path.join(datadir, entry) for entry in os.listdir(datadir) if entry.endswith(".tif")]

    @staticmethod
    def _get_row_from_granule(granule):
        try:
            logger.info('Creating granule row to publish in PG footprint table...')
            logger.info("{} : granule to be published".format(granule))
            capture_date = date_parser.parse(granule)
            with rasterio.open(granule) as src:
                bbox = src.bounds
                ll_long = bbox[0]
                ll_lat = bbox[1]
                ur_long = bbox[2]
                ur_lat = bbox[3]
            tif_geom = shg.box(ll_long, ll_lat, ur_long, ur_lat)
            return granule, tif_geom.to_wkt(), capture_date

        except NoParserAvailableException:
            logger.error('Unable to parse date from filename %s. Skipping.' % granule)
            return
        except Exception as e:
            logger.exception('Failed to ingest granules to PG footprint table.\nError: {}'.format(e))
            return

    def _open_connection(self):
        self.conn = psycopg2.connect(config.get_conn_string())

    def _close_connection(self):
        self.conn.close()

    def _publish_db(self, granules, tablename, source):
        try:
            cur = self.conn.cursor()
            select_query = "SELECT location FROM imagery.{0};".format(tablename)
            cur.execute(select_query)
            results = cur.fetchall()
        except psycopg2.Error or OSError as e:
            logger.exception('{}: ERROR: Failed to connect to PG DB.'.format(e))
            exit(1)

        if source == "l7":
            sensor = "Landsat 7 ETM+"
        elif source == "l8":
            sensor = "Landsat 8 OLI"
        elif source == "sentinel":
            sensor = "Sentinel 2 MSI"
        else:
            sensor = "Unknown"
        pub_ready_count = 0
        rows = [self._get_row_from_granule(g) for g in granules]
        valid_rows = [r for r in rows if r is not None]
        if len(valid_rows) == 0:
            pass
            logger.info("{} : no granule to be published in the folder".format(rows))
        else:
            insert_query = "INSERT INTO imagery.{0} (location,geom,capture_date,sensor) VALUES (%s,ST_GeomFromText(%s,4326),%s,%s);".format(
            tablename)
            for granule, geom_str, date in valid_rows:
                granule_efs_location = '/efs/geoserver/data/' + granule[17:]
                new_granule = granule_efs_location.split('/')[-1]
                observed_granules = [i[0].split('/')[-1] for i in results]
                if new_granule not in observed_granules:
                    pub_ready_count += 1
                    try:
                        cur.execute(insert_query, (granule_efs_location, geom_str, date, sensor))
                        self.conn.commit()
                    except psycopg2.Error or OSError as e:
                        logger.exception('{}: ERROR: failed to ingest data to PG DB. \n{}'.format(fname, e))
                        exit(1)
                else:
                    logger.info("Granules already exist in the current EVI endpoint")

            logger.info("A total of {} granules have been ingested to imagery.evi Endpoint.".format(pub_ready_count))
            cur.close()