import abc
import logging
import os
import re
from io import StringIO
import boto3
import pandas as pd

from orthoimagery_pipeline.common import config
from orthoimagery_pipeline.common import GoogleStorageDownload
from orthoimagery_pipeline.common.exceptions import SourceNotFoundException


logger = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('nose').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

s3 = boto3.resource('s3')
bucket = s3.Bucket(config.OUTPUT_BUCKET)


class ValidateContent:
    """
    Abstract base class to provide high-level interface for a Validate Content Strategy
    Note this class should not be instantiated directly, use child class instead
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, working_dir):
        self.working_dir = working_dir
        self.download_url_csv = os.path.join(self.working_dir, config.download_url)
        self.output_files = [self.download_url_csv]
        self.download_url_val = []
        self.download_url_inval = []

    @classmethod
    def find_validation_strategy(cls, working_dir, source):
        """
        Factory method to create a ValidateContent child class instance based on source argument value
        """

        for subclass in cls.__subclasses__():
            if hasattr(subclass, 'source') and subclass.source == source:
                return subclass(working_dir)

        raise SourceNotFoundException

    ###
    # Begin sequence of private methods to implement the high-level ValidateContent algorithm
    ###
    ''' download csv file from define imageset module loc360imagery bucket'''

    def _download_image_set(self):
        logger.info('Downloading CSV from S3 Define Image Set s3 Bucket...')
        s3.meta.client.download_file(config.OUTPUT_BUCKET, self.s3_define_folder + config.download_url,
                                     self.download_url_csv)
        return

    '''Write valid and invalid download url lists to in-memory files and upload to S3'''

    def _csv_to_s3(self):
        valid_urls = u'\n'.join(self.download_url_val)
        invalid_urls = u'\n'.join(self.download_url_inval)

        with StringIO(valid_urls) as valid_url_list:
            s3_key = self.s3_validate_folder + config.download_url
            logger.info('Uploading valid url CSV to s3 bucket...')
            bucket.upload_fileobj(valid_url_list, s3_key, ExtraArgs={'ServerSideEncryption': "AES256"})

        with StringIO(invalid_urls) as invalid_url_list:
            s3_key = self.s3_validate_folder + config.download_url_inval
            logger.info('Uploading invalid url CSV to s3 bucket...')
            bucket.upload_fileobj(invalid_url_list, s3_key, ExtraArgs={'ServerSideEncryption': "AES256"})

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
        self._validate_bands()
        self._validate_metadata()
        self._csv_to_s3()
        self._clean_up()

    # Abstract properties and methods child classes to provide #
    @abc.abstractproperty
    def s3_define_folder(self):
        raise NotImplementedError

    @abc.abstractproperty
    def s3_validate_folder(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _validate_bands(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _validate_metadata(self):
        raise NotImplementedError


class L8ValidateContent(ValidateContent):
    source = 'l8'
    band_regex = re.compile('B([1-7]|9|1[0-1]).TIF$')  # exlcude .TIF.ovr

    @property
    def s3_define_folder(self):
        return config.L8_DEFINE_FOLDER + 'out/'

    @property
    def s3_validate_folder(self):
        return config.L8_VALIDATE_FOLDER + 'out/'

    def _validate_bands(self):
        url = pd.read_csv(self.download_url_csv, header=None)
        url.columns = ['download_url']

        for i in range(len(url.download_url.unique())):

            baseurl = str(url.download_url.unique()[i][35:-10])
            baseurl_parts = baseurl.split('/')
            l8_bucket_name = baseurl_parts[0]
            l8_url_prefix = '/'.join(baseurl_parts[1:])
            l8_bucket = s3.Bucket(l8_bucket_name)
            prefixed_files = l8_bucket.objects.filter(Prefix=l8_url_prefix)
            valid_files = [f.key for f in prefixed_files if self.band_regex.search(f.key)]

            if len(valid_files) == 10:  # l8 contains 10 bands and 10 ovr files
                logger.info('Base URL %s is valid.' % baseurl)
                self.download_url_val.append(baseurl)
            else:
                logger.info('Base URL %s is invalid.' % baseurl)
                self.download_url_inval.append(baseurl)

    def _validate_metadata(self):

        for i in range(len(self.download_url_val)):

            baseurl = str(self.download_url_val[i])
            baseurl_parts = baseurl.split('/')
            l8_bucket_name = baseurl_parts[0]
            l8_url_prefix = '/'.join(baseurl_parts[1:])

            l8_bucket = s3.Bucket(l8_bucket_name)
            prefixed_files = l8_bucket.objects.filter(Prefix=l8_url_prefix)
            mtl_files = [f.key for f in prefixed_files if f.key.endswith('_MTL.txt')]

            if len(mtl_files) == 0:
                logger.info('Metadata file missing for baseurl %s. Moving this baseurl to invalid list.' % baseurl)
                self.download_url_val.remove(baseurl)
                self.download_url_inval.append(baseurl)


class S2ValidateContent(ValidateContent):

    source = 'sentinel'
    band_regex = re.compile('(B0[1-8].jp2)|(B8A.jp2)|(B09.jp2)|(B1[0-2].jp2)')

    @property
    def s3_define_folder(self):
        return config.SENTINEL_DEFINE_FOLDER + 'out/'

    @property
    def s3_validate_folder(self):
        return config.SENTINEL_VALIDATE_FOLDER + 'out/'

    def _validate_bands(self):

        url = pd.read_csv(self.download_url_csv, header=None)
        url.columns = ['download_url']

        for i in range(len(url.download_url.unique())):
            baseurl = str(url.download_url.unique()[i])

            gs = GoogleStorageDownload()
            blob_prefix = baseurl.replace('gs://{}/'.format(baseurl.split('/')[2]), '')
            re_pattern = r'{}/GRANULE/.*?/IMG_DATA/.*?(B0[1-8].jp2)|(B8A.jp2)|(B09.jp2)|(B1[0-2].jp2)$'.format(
                blob_prefix)
            valid_files = gs.list(baseurl, include_re_pattern=re_pattern)

            '''
            ls_url = 'gsutil ls ' + baseurl + '/GRANULE/*/IMG_DATA/'
            files_all = subprocess.check_output(ls_url, shell=True)  # TODO: is there a Python API for gsutils?
            files = files_all.split('\n')
            valid_files = [f for f in files if self.band_regex.search(f)]
            '''

            if len(valid_files) == 13:  # sentinel2 should come with all 13 bands
                logger.info('Base URL %s is valid.' % baseurl)
                self.download_url_val.append(baseurl)
            else:
                logger.info('Base URL %s is invalid.' % baseurl)
                self.download_url_inval.append(baseurl)

    def _validate_metadata(self):
        for i in range(len(self.download_url_val)):
            baseurl = str(self.download_url_val[i])

            gs = GoogleStorageDownload()
            blob_prefix = baseurl.replace('gs://{}/'.format(baseurl.split('/')[2]), '')
            re_pattern = r'{}/INSPIRE.xml$'.format(blob_prefix)
            x = gs.list(baseurl, include_re_pattern=re_pattern)

            '''
            ls_url = 'gsutil ls ' + baseurl + '/INSPIRE.xml'
            x = subprocess.check_output(ls_url, shell=True)

            if x == baseurl + '/INSPIRE.xml\n':
            '''

            if len(x) > 0 and x[0]['blob_name'] == blob_prefix + '/INSPIRE.xml':
                pass
            else:
                logger.info('Metadata file is missing for baseurl %s. Moving this baseurl to invalid list.' % baseurl)
                self.download_url_val.remove(baseurl)
                self.download_url_inval.append(baseurl)


class L7ValidateContent(ValidateContent):
    source = 'l7'
    band_regex = re.compile('(B[1-5].TIF)|(B6_VCID_[1-2].TIF)|(B[7-8].TIF)')

    @property
    def s3_define_folder(self):
        return config.L7_DEFINE_FOLDER + 'out/'

    @property
    def s3_validate_folder(self):
        return config.L7_VALIDATE_FOLDER + 'out/'

    def _validate_bands(self):

        url = pd.read_csv(self.download_url_csv, header=None)
        url.columns = ['download_url']

        for i in range(len(url.download_url.unique())):
            baseurl = str(url.download_url.unique()[i])

            gs = GoogleStorageDownload()
            in_re_pattern = r'(B[1-5])|(B6_VCID_[1-2])|(B[7-8]).TIF$'
            ex_re_pattern = r'.*?\.gz'
            valid_files = gs.list(baseurl, include_re_pattern=in_re_pattern, exclude_re_pattern=ex_re_pattern)

            '''
            ls_url = 'gsutil ls ' + baseurl
            files_all = subprocess.check_output(ls_url, shell=True)
            files = files_all.split('\n')
            valid_files = [f for f in files if self.band_regex.search(f)]
            '''

            if len(valid_files) == 9:  # l7 should come with 9 bands
                logger.info('Base URL %s is valid.' % baseurl)
                self.download_url_val.append(baseurl)
            else:
                logger.info('Base URL %s is invalid.' % baseurl)
                self.download_url_inval.append(baseurl)

    def _validate_metadata(self):
        for i in range(len(self.download_url_val)):
            baseurl = str(self.download_url_val[i])

            gs = GoogleStorageDownload()
            meta_fn = '{}/{}_MTL.txt'.format(baseurl.replace('gs://{}/'.format(baseurl.split('/')[2]), ''),
                                             baseurl.split('/')[-1])
            re_pattern = r'{}$'.format(meta_fn)
            x = gs.list(baseurl, include_re_pattern=re_pattern)

            '''
            ls_url = 'gsutil ls ' + baseurl + '/' + baseurl.split('/')[-1] + '_MTL.txt'
            x = subprocess.check_output(ls_url, shell=True)

            if x == baseurl + '/' + baseurl.split('/')[-1] + '_MTL.txt\n':
            '''

            if len(x) > 0 and x[0]['blob_name'] == meta_fn:
                pass
            else:
                logger.info('Metadata file is missing for baseurl %s. Moving this baseurl to invalid list.' % baseurl)
                self.download_url_val.remove(baseurl)
                self.download_url_inval.append(baseurl)
