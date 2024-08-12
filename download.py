import abc
import logging
import os
import boto3
import pandas as pd
from orthoimagery_pipeline.common import config
from orthoimagery_pipeline.common import S3Download
from orthoimagery_pipeline.common import GoogleStorageDownload
from orthoimagery_pipeline.common.exceptions import SourceNotFoundException

logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('nose').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

s3 = boto3.resource('s3')
bucket = s3.Bucket(config.OUTPUT_BUCKET)


class DownloadContent(object):
    """
    Abstract base class to provide high-level interface for a DownloadContent job
    Note this class should not be instantiated directly, use child classes instead
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, working_dir):
        self.working_dir = working_dir
        self.download_url_csv = os.path.join(self.working_dir, config.download_url)
        self.output_files = [self.download_url_csv]

    @classmethod
    def download_content(cls, source, working_dir):
        """
        Factory method to create a DownloadContent child class instance based on source argument value
        """
        for subclass in cls.__subclasses__():
            if hasattr(subclass, 'source') and subclass.source == source:
                return subclass(working_dir)
        raise SourceNotFoundException

    ### 
    # Begin sequence of private methods to implement the high-level DownloadContent algorithm 
    #
    ''' download csv file from validate content module loc360imagery bucket'''

    def _download_image_set(self):
        logger.info('Downloading CSV from S3 Validate Content s3 Bucket...')
        s3.meta.client.download_file(config.OUTPUT_BUCKET, self.s3_validate_folder + config.download_url,
                                     self.download_url_csv)
        return

    def _clean_up(self):

        # clean up intermediary files written to disk
        for output in self.output_files:
            if os.path.isfile(output):
                os.remove(output)

    def __call__(self):
        """
        call procedure remains the same sequence of method calls for all ValidateContent implementations
        only need to implement the abstract properties per child class
        """
        self._download_image_set()
        self._download_tif()
        self._clean_up()

    @abc.abstractproperty
    def s3_validate_folder(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _download_tif(self):
        raise NotImplementedError


class L7DownloadContent(DownloadContent):
    source = 'l7'

    @property
    def s3_validate_folder(self):
        return config.L7_VALIDATE_FOLDER + 'out/'

    def _download_tif(self):
        url = pd.read_csv(self.download_url_csv, header=None)
        url.columns = ['download_url']
        num_expected_download = url.shape[0]
        for i in range(url.shape[0]):
            file_path = url['download_url'][i]
            parts = file_path.split('/')
            prefix = '/'.join(parts[3:-1])
            gs = GoogleStorageDownload()
            gs.recursive_download(file_path, config.L7_DATA_FOLDER, folder_map=True, overwrite=True, prefix_to_replace=prefix)
        num_found_download = len(os.walk(config.L7_VALIDATE_FOLDER).next()[1])
        logger.info('Expected number of scenes to download: {}; Actual number of downloaded scenes: {}'.format(num_expected_download, num_found_download))

        return


class L8DownloadContent(DownloadContent):
    source = 'l8'

    @property
    def s3_validate_folder(self):
        return config.L8_VALIDATE_FOLDER + 'out/'

    def _download_tif(self):
        url = pd.read_csv(self.download_url_csv, header=None)
        url.columns = ['download_url']
        num_expected_download = url.shape[0]
        for i in range(url.shape[0]):
            file_path = url['download_url'][i]
            scene_id = file_path.split('/')[-2]
            sd = S3Download()
            exclude_re_pattern = r'.*?\.((jpg)|(JPG)|(ovr)|(OVR)|(imd)|(IMD))$'
            dst_dir = config.L8_DATA_FOLDER + scene_id
            sd.recursive_download(file_path, dst_dir, exclude_re_pattern=exclude_re_pattern, overwrite=True)
        num_found_download = len(os.walk(config.L8_DATA_FOLDER).next()[1])
        logger.info('Expected number of scenes to download: {}; Actual number of downloaded scenes: {}'.format(num_expected_download, num_found_download))

        return

class SentinelDownloadContent(DownloadContent):
    source = 'sentinel'

    @property
    def s3_validate_folder(self):
        return config.SENTINEL_VALIDATE_FOLDER + 'out/'

    def _download_tif(self):
        url = pd.read_csv(self.download_url_csv, header=None)
        url.columns = ['download_url']
        num_expected_download = url.shape[0]
        for i in range(url.shape[0]):
            file_path = url['download_url'][i]
            prefix_to_replace = file_path.replace('gs://', '').split('/')[1:-1]
            prefix_to_replace = '/'.join(prefix_to_replace)
            gs = GoogleStorageDownload()
            gs.recursive_download(file_path, config.SENTINEL_DATA_FOLDER, folder_map=True, overwrite=True,
                                  prefix_to_replace=prefix_to_replace)
        num_found_download = len(os.walk(config.SENTINEL_DATA_FOLDER).next()[1])
        logger.info('Expected number of scenes to download: {}; Actual number of downloaded scenes: {}'.format(num_expected_download, num_found_download))

        return
