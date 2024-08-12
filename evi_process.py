import abc
import fnmatch
import logging
import os
import shutil
import warnings
import subprocess
from errno import ENOENT

import numpy as np
import rasterio
from osgeo import gdal

from orthoimagery_pipeline.common import config
from orthoimagery_pipeline.common.exceptions import SourceNotFoundException


# this is to mute warnings for nan numpy array comparison when mask invalid values
# and rasterio gdal-style transform deprecation
warnings.filterwarnings("ignore",category =RuntimeWarning)
warnings.filterwarnings("ignore",category =FutureWarning)

logger = logging.getLogger(__name__)
logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('nose').setLevel(logging.CRITICAL)
logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
logging.getLogger('rasterio').setLevel(logging.CRITICAL)

def to_delete(filename):
    """Helper function to identify files for deletion"""
    return filename.endswith('img') or \
           filename.endswith("img.aux.xml") or \
           filename.endswith("EVI.tif") or \
           filename.endswith("wgs84.tif") or \
           filename.endswith("wgs84_masked.tif")


class EVIProcess:
    """
    Abstract base class to provide high-level interface for a EVI Process job
    Note this class should not be instantiated directly, use child classes instead
    """

    __metaclass__ = abc.ABCMeta

    # TODO: Implement base_dir, pub_dir, and s3_dir as abstract properties
    def __init__(self):
        self.sub_dirs = [os.path.join(self.base_dir, elem) for elem in os.listdir(self.base_dir) if
                         os.path.isdir(os.path.join(self.base_dir, elem))]

    @classmethod
    def evi_process(cls, source):
        """
        Factory method to create a EVI process child class instance based on 'source' argument value
        """
        for subclass in cls.__subclasses__():
            if hasattr(subclass, 'source') and subclass.source == source:
                return subclass()

        raise SourceNotFoundException

    def _img_to_gs_pub(self):

        # move publish-ready granules to geoserver EFS
        for scene_folder in self.sub_dirs:
            os.chdir(scene_folder)
            granules = [os.path.join(evi[0], file)
                        for evi in os.walk(scene_folder)
                        for file in evi[2]
                        if fnmatch.fnmatch(file, '*wgs84_masked_tiled*')]

            if len(granules) == 0:
                logger.warning("{} : no publish-ready granules exist".format(scene_folder))
                exit(1)
            for file in granules:
                try:
                    filename = file.split(os.sep)[-1]
                    new_dest = os.path.join(self.pub_dir, filename)
                    if not os.path.exists(self.pub_dir):
                        os.mkdir(self.pub_dir)
                    mv = "sudo mv" + " " + file + " " + new_dest
                    subprocess.check_call(mv, shell=True)
                except subprocess.CalledProcessError as e:
                    logger.exception("Failed to move publish-ready granules to Geoserver EFS, \nError: {}".format(e))
                    exit(1)

    def _clean_up(self):

        # clean up intermediary files written to disk from cloud masking and EVI calculation
        for scene_folder in self.sub_dirs:
            os.chdir(scene_folder)
            files2delete = [os.path.join(evi[0], data_file) for evi in os.walk(scene_folder) for data_file in evi[2]
                            if to_delete(data_file)]
            if len(files2delete) == 0:
                logger.warning("{} : no intermediate files need to be deleted".format(scene_folder))
                exit(1)
            for file in files2delete:
                try:
                    os.remove(file)
                    logger.info('{}: Deleting intermediary files and raw images on EFS'.format(scene_folder))
                # TODO: Eliminate bare except clause
                except Exception as ex:
                    logger.exception("{} failed to remove intermediate files".format(scene_folder))
                    exit(1)

    def _img_to_s3(self):

        # move raw landsat scenes to S3
        for scene_folder in self.sub_dirs:

            try:
                s3_sync = "aws s3 sync " + scene_folder + " s3://" + self.s3_dir + scene_folder.split('/')[-1] + " --sse AES256 --only-show-errors"
                subprocess.check_call(s3_sync,shell=True)
                logger.info('{}: Uploading processed scenes to S3'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception("{} failed to upload. \nError: {}".format(scene_folder,e))
                exit(1)
            except OSError as e:
                logger.exception("{} failed to upload.Unable to call awscli.\nError: {}".format(scene_folder, e))
                exit(1)
        return

    def _efs2_clean(self):

        # remove raw scenes from EFS2
        for scene_folder in self.sub_dirs:

            try:
                shutil.rmtree(scene_folder)
            except OSError as e:
                # address Device or resource busy from shutil.rmtree
                try:
                    subprocess.Popen(['rm', '-rf', self.scene_folder])
                except OSError as e:
                    logger.exception('{}: Failed to remove scene folders. {}'.format(scene_folder, e))
                    pass

    def _geoprocess(self):
        sub_dirs = [os.path.join(self.base_dir, elem) for elem in os.listdir(self.base_dir)
                    if os.path.isdir(os.path.join(self.base_dir, elem))]  # filter out files from dir contents

        for scene_folder in sub_dirs:
            os.chdir(scene_folder)
            evi_files = [os.path.join(evi[0], found_file) for evi in os.walk(scene_folder) for found_file in evi[2] if
                         found_file.endswith('EVI.tif')]  # filter out evi files

            try:
                if len(evi_files) == 0:
                    raise IOError(ENOENT, "No EVI images exist.")
            except IOError as e:
                logger.exception("{} : No EVI images exist, \nError: {}".format(scene_folder, e))
                exit(1)

            for evi_file in evi_files:
                try:
                    wgs84 = evi_file.split("/")[-1].split(".")[0] + '_wgs84.tif'
                    warp = "gdalwarp -t_srs EPSG:4326 -dstnodata -9999 {input} {output}".format(input=evi_file,output=wgs84)
                    subprocess.check_call(warp,shell=True)
                    logger.info("{} has been transformed to WGS84 and assign Nodata as -9999".format(evi_file))
                except subprocess.CalledProcessError as e:
                    logger.exception("{} failed to transform to wgs84.gdalwarp call failed. \nError: {}".format(evi_file, e))
                    pass

                try:
                    wgs84_masked = wgs84.split(".")[0] + '_masked.tif'
                    with rasterio.open(wgs84) as src:
                        with rasterio.open(wgs84_masked, 'w', **src.profile) as dst:
                            band = src.read()
                            band[np.isinf(band)] = -9999
                            band[np.where(band <= 0.0)] = -9999
                            band[np.where(band > 1.0)] = -9999
                            dst.write(band)
                        dst.close()
                        logger.info("{} : value outside the range of (0, 1] has been assigned as -9999".format(wgs84_masked))
                # TODO: Eliminate bare except clause
                except Exception as e:
                    logger.exception("{} : Failed to assign -9999 to values outside range of (0,1]. {}".format(wgs84_masked, e))
                    pass
                try:
                    wgs84_masked_tiled = wgs84_masked.split(".")[0] + '_tiled.tif'
                    translate = 'gdal_translate -co "TILED=YES" -co "BLOCKXSIZE=512" -co "BLOCKYSIZE=512" %s %s' % (wgs84_masked, wgs84_masked_tiled)
                    subprocess.check_call(translate,shell=True)
                    logger.info("{} : tiles have been created".format(wgs84_masked_tiled))
                except subprocess.CalledProcessError as e:
                    logger.exception("{} tiles failed to be created. Error: {}".format(wgs84_masked_tiled, e))
                    pass
                try:
                    addo = 'gdaladdo -ro --config COMPRESS_OVERVIEW DEFLATE %s 2 4 8 16 32' % wgs84_masked_tiled
                    subprocess.check_call(addo,shell=True)
                    logger.info("{} : overviews have been created".format(wgs84_masked_tiled))
                except subprocess.CalledProcessError as e:
                    logger.exception("{}: Overview have not been created. \nError: {}".format(wgs84_masked_tiled,e))
                    pass
                try:
                    info = 'gdalinfo -stats %s' % wgs84_masked_tiled
                    subprocess.check_call(info,shell=True)
                    logger.info("{} : stats have been created".format(wgs84_masked_tiled))
                except subprocess.CalledProcessError as e:
                    logger.exception("{}: Stats failed to be created. \nError:{}".format(wgs84_masked_tiled,e))
                    pass

    # Begin sequence of private methods to implement the evi calculation algorithm ###
    def __call__(self):
        """
        call procedure remains the same sequence of method calls for all EVI process implementations
        only need to implement the abstract properties per child class
        """
        self._cloud_mask()
        self._cal_evi()
        self._geoprocess()
        self._img_to_gs_pub()
        self._clean_up()
        self._img_to_s3()
        self._efs2_clean()

    @abc.abstractmethod
    def _cloud_mask(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _cal_evi(self):
        raise NotImplementedError


class L7EVIProcess(EVIProcess):
    source = 'l7'
    base_dir = config.L7_DATA_FOLDER
    s3_dir = config.L7_EVI_FOLDER
    pub_dir = config.PUB_GRANULE_FOLDER

    def _cloud_mask(self):
        for scene_folder in self.sub_dirs:
            flag1 = False
            flag2 = False
            for file in os.listdir(scene_folder):
                if file[-10:] == "VCID_1.TIF":
                    flag1 = True
                if file[-5:] == "0.TIF":
                    flag2 = True
            if flag1:
                try:
                    cmd1 =  'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o {des_dir}/ref.img {source_dir}/L*_B[1,2,3,4,5,7].TIF'.format(
                        des_dir=scene_folder, source_dir=scene_folder)
                    subprocess.check_call(cmd1,shell=True)
                    logger.info('{}: Reflective bands have been stacked separately successfully!'.format(scene_folder))
                except subprocess.CalledProcessError as e:
                    logger.exception('{}: Raster stacks for reflective bands have not been created properly, check if ref.img and thermal.img exist. {}'.format(scene_folder, e))
                    exit(1)
                try:
                    cmd2 = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o {des_dir}/thermal.img {source_dir}/L*_B6_VCID_?.TIF'.format(
                        des_dir=scene_folder, source_dir=scene_folder)
                    subprocess.check_call(cmd2,shell=True)
                    logger.info('{}: Thermal bands have been stacked separately successfully!'.format(scene_folder))
                except subprocess.CalledProcessError as e:
                    logger.exception('{}: Raster stacks for thermal bands have not been created properly, check if ref.img and thermal.img exist. Error: {}'.format(scene_folder, e))
                    exit(1)
            elif flag2:
                try:
                    cmd1 = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o {des_dir}/ref.img {source_dir}/L*_B[1,2,3,4,5,7]0.TIF'.format(
                        des_dir=scene_folder, source_dir=scene_folder)
                    subprocess.check_call(cmd1,shell=True)
                    logger.info('{}: Reflective bands have been stacked separately successfully!'.format(scene_folder))
                except subprocess.CalledProcessError as e:
                    logger.exception('{}: Raster stacks have not been created properly, check if ref.img and thermal.img exist. Error: {}'.format(scene_folder, e))
                    exit(1)
                try:
                    cmd2 = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o {des_dir}/thermal.img {source_dir}/L*_B6[1,2].TIF'.format(
                        des_dir=scene_folder, source_dir=scene_folder)
                    subprocess.check_call(cmd2,shell=True)
                    logger.info('{}: Reflective bands have been stacked separately successfully!'.format(scene_folder))
                except subprocess.CalledProcessError as e:
                    logger.exception('{}: Raster stacks have not been created properly, check if ref.img and thermal.img exist. \nError: {}'.format(scene_folder, e))
                    exit(1)
            try:
                cmd3 = 'fmask_usgsLandsatMakeAnglesImage.py -m {source_dir}/*_MTL.txt -t {source_dir}/ref.img -o {des_dir}/angles.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd3,shell=True)
                logger.info('{}: Per-pixel image of the relevant angles has been created!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Images of relevant angles have not been created properly, check if angles.img exists. If not, check angles related parameters in the metadata file. \nError: {}'.format(scene_folder, e))
                exit(1)
            try:  # mask saturated pixels of high values
                cmd4 = 'fmask_usgsLandsatSaturationMask.py -i {source_dir}/ref.img -m {source_dir}/*_MTL.txt -o {des_dir}/saturationmask.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd4,shell=True)
                logger.info('{}: Highly saturated piexels have been masked!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Images of relevant angles have not been created properly, check if saturationmask.img exist. If not, check SaturationMask related parameters in the metadata file. \nError: {}'.format(scene_folder, e))
                exit(1)
            try:  # calculate Top of Atmosphere reflectance
                cmd5 = 'fmask_usgsLandsatTOA.py -i {source_dir}/ref.img -m {source_dir}/*_MTL.txt -z {source_dir}/angles.img -o {des_dir}/toa.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd5,shell=True)
                logger.info('{}: TOA reflectance has been calculated!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: TOA reflectance has not been calculated, check if ref.img, MTL.txt, angles.img and toa.img exist. \nError:{}'.format(scene_folder, e))
                exit(1)
            try:
                cmd6 = 'fmask_usgsLandsatStacked.py -t {source_dir}/thermal.img -a {source_dir}/toa.img -m {source_dir}/*_MTL.txt -z {source_dir}/angles.img -s {source_dir}/saturationmask.img -o {des_dir}/cloud.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd6,shell=True)
                logger.info('{}: Cloud has been masked!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Cloud has not been masked, check if thermal.img, toa.img, MTL.txt, angles.img, saturationmask.img and cloud.img exist. \nError: {}'.format(scene_folder, e))
                exit(1)

    def _cal_evi(self):

        for scene_folder in self.sub_dirs:
            os.chdir(scene_folder)
            toa_image = os.path.join(scene_folder, config.toaImage)
            try:
                img = gdal.Open(toa_image)
                b1 = img.GetRasterBand(1).ReadAsArray()
                b3 = img.GetRasterBand(3).ReadAsArray()
                b4 = img.GetRasterBand(4).ReadAsArray()
                b1 = b1.astype(float) / 10000
                b3 = b3.astype(float) / 10000
                b4 = b4.astype(float) / 10000
                with np.errstate(divide='ignore'):
                    evi = 2.5 * ((b4 - b3) / (b4 + 6 * b3 - 7.5 * b1 + 1))
                logger.info("{}: EVI calculation has completed successfully".format(scene_folder))
            except RuntimeError as e:
                logger.exception("EVI calculation has not completed, check toa_image and function used to calculate EVI. Error: {}".format(scene_folder, e))
                pass
            cloud = os.path.join(scene_folder, config.cloud)
            try:
                cloud = gdal.Open(cloud)
                cl = cloud.GetRasterBand(1).ReadAsArray()  # int value range: 0-5
                cloudmask = np.where(cl == 2)  # cloud is coded 2
                shadowmask = np.where(cl == 3)  # shadow is coded 3
                nanvalues = np.where(b1 == 32767)  #
                evi[cloudmask] = np.nan
                evi[shadowmask] = np.nan
                evi[nanvalues] = np.nan
                logger.info("{}: Cloud and shadow masks have been applied to EVI metrics".format(scene_folder))
            except RuntimeError as e:
                logger.exception("{}: Cloud and shadow masks failed to apply to EVI metrics. \nError: {}".format(scene_folder, e))
                pass
            try:
                geo_transform = img.GetGeoTransform()
                projection = img.GetProjection()
                cols = img.RasterXSize
                rows = img.RasterYSize

                out_file_name = scene_folder.split('/')[-1] + '_EVI' + '.tif'

                driver = gdal.GetDriverByName('GTiff')
                dataset = driver.Create(out_file_name, cols, rows, 1, gdal.GDT_Float32)
                dataset.SetGeoTransform(geo_transform)
                dataset.SetProjection(projection)
                band = dataset.GetRasterBand(1)
                band.WriteArray(evi)
                dataset = None
                logger.info("{}: EVI geotiff has been created.".format(scene_folder))
            except RuntimeError as e:
                logger.exception("{}: Failed to write EVI to a new geotiff. \nError: {}".format(scene_folder, e))
                pass


class L8EVIProcess(EVIProcess):
    source = 'l8'
    base_dir = config.L8_DATA_FOLDER
    s3_dir = config.L8_EVI_FOLDER
    pub_dir = config.PUB_GRANULE_FOLDER

    def _cloud_mask(self):
        for scene_folder in self.sub_dirs:
            try:
                cmd1 = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o {des_dir}/ref.img {source_dir}/LC08*_B[1-7,9].TIF'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd1,shell=True)
                logger.info('{}: Reflective bands have been stacked separately successfully!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Raster stacks for reflective bands have not been created properly, check if ref.img and thermal.img exist. {}'.format(scene_folder, e))
                exit(1)
            try:
                cmd2 = 'gdal_merge.py -separate -of HFA -co COMPRESSED=YES -o {des_dir}/thermal.img {source_dir}/LC08*_B1[0,1].TIF'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd2,shell=True)
                logger.info('{}: Thermal bands have been stacked separately successfully!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Raster stacks for thermal bands have not been created properly, check if ref.img and thermal.img exist. Error: {}'.format(scene_folder, e))
                exit(1)
            try:
                cmd3 = 'fmask_usgsLandsatMakeAnglesImage.py -m {source_dir}/*_MTL.txt -t {source_dir}/ref.img -o {des_dir}/angles.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd3,shell=True)
                logger.info('{}: Per-pixel image of the relevant angles has been created!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Images of relevant angles have not been created properly, check if angles.img exists. If not, check angles related parameters in the metadata file. \nError: {}'.format(scene_folder,e))
                exit(1)
            try:  # mask saturated pixels of high values
                cmd4 = 'fmask_usgsLandsatSaturationMask.py -i {source_dir}/ref.img -m {source_dir}/*_MTL.txt -o {des_dir}/saturationmask.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd4,shell=True)
                logger.info('{}: Highly saturated piexels have been masked!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Images of relevant angles have not been created properly, check if saturationmask.img exist. If not, check SaturationMask related parameters in the metadata file. \nError:{}'.format(scene_folder, e))
                exit(1)
            try:  # calculate Top of Atmosphere reflectance
                cmd5 = 'fmask_usgsLandsatTOA.py -i {source_dir}/ref.img -m {source_dir}/*_MTL.txt -z {source_dir}/angles.img -o {des_dir}/toa.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd5, shell=True)
                logger.info('{}: TOA reflectance has been calculated!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Failed to calculate TOA reflectance, check if ref.img, MTL.txt, angles.img and toa.img exist. \nError:{}'.format(scene_folder,e))
                exit(1)
            try:
                cmd6 = 'fmask_usgsLandsatStacked.py -t {source_dir}/thermal.img -a {source_dir}/toa.img -m {source_dir}/*_MTL.txt -z {source_dir}/angles.img -s {source_dir}/saturationmask.img -o {des_dir}/cloud.img'.format(
                    des_dir=scene_folder, source_dir=scene_folder)
                subprocess.check_call(cmd6, shell=True)
                logger.info('{}: Cloud is masked!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Failed to mask clouds, check if thermal.img, toa.img, MTL.txt, angles.img, saturationmask.img and cloud.img exist. \nError:{}'.format(scene_folder,e))
                exit(1)

    def _cal_evi(self):
        for scene_folder in self.sub_dirs:
            os.chdir(scene_folder)
            toa_image = os.path.join(scene_folder, config.toaImage)
            try:
                img = gdal.Open(toa_image)
                b2 = img.GetRasterBand(2).ReadAsArray()
                b4 = img.GetRasterBand(4).ReadAsArray()
                b5 = img.GetRasterBand(5).ReadAsArray()
                b2 = b2.astype(float) / 10000
                b4 = b4.astype(float) / 10000
                b5 = b5.astype(float) / 10000
                with np.errstate(divide='ignore'):
                    evi = 2.5 * ((b5 - b4) / (b5 + 6 * b4 - 7.5 * b2 + 1))
                logger.info("{}: EVI calculation has completed successfully".format(scene_folder))
            except RuntimeError as e:
                logger.exception("EVI calculation has not completed, check toa_image and function used to calculate EVI. Error: {}".format(scene_folder, e))
                exit(1)
            cloud = os.path.join(scene_folder, config.cloud)
            try:
                cloud = gdal.Open(cloud)
                cl = cloud.GetRasterBand(1).ReadAsArray()  # int value range: 0-5
                cloudmask = np.where(cl == 2)  # cloud is coded 2
                shadowmask = np.where(cl == 3)  # shadow is coded 3
                nanvalues = np.where(b2 == 32767)
                evi[cloudmask] = np.nan
                evi[shadowmask] = np.nan
                evi[nanvalues] = np.nan
                logger.info("{}: Cloud and shadow masks have been applied to EVI metrics".format(scene_folder))
            except RuntimeError as e:
                logger.exception("{}: Cloud and shadow masks failed to apply to EVI metrics. Error: {}".format(scene_folder,e))
                exit(1)

            try:
                geo_transform = img.GetGeoTransform()
                projection = img.GetProjection()
                cols = img.RasterXSize
                rows = img.RasterYSize
                in_name = scene_folder.split("/")[-1]
                out_file_name = scene_folder + '/l8' + in_name.split("_")[2] + in_name.split("_")[3] + 'T000000_EVI.tif'

                driver = gdal.GetDriverByName('GTiff')
                dataset = driver.Create(out_file_name, cols, rows, 1, gdal.GDT_Float32)
                dataset.SetGeoTransform(geo_transform)
                dataset.SetProjection(projection)
                band = dataset.GetRasterBand(1)
                band.WriteArray(evi)
                dataset = None
                logger.info("{}: An EVI geotiff has been created".format(scene_folder))
            except RuntimeError as e:
                logger.exception("{}: Failed to write EVI to a new geotiff. \nError: {}".format(scene_folder, e))
                exit(1)


class SentinelEVIProcess(EVIProcess):
    source = 'sentinel'
    base_dir = config.SENTINEL_DATA_FOLDER
    s3_dir = config.SENTINEL_EVI_FOLDER
    pub_dir = config.PUB_GRANULE_FOLDER

    def _cloud_mask(self):
        for scene_folder in self.sub_dirs:
            mtd_folder = scene_folder + "/GRANULE/*"
            granule_folder = scene_folder + "/GRANULE/*/IMG_DATA"
            try:
                # make a stack of all bands at the 20m resolution (a compromise between speed and detail). Bands are in order of numeric band number:
                cmd1 = "gdalbuildvrt -resolution user -tr 20 20 -separate {scene_folder}/allbands.vrt {granule_dir}/*_B0[1-8].jp2 {granule_dir}/*_B8A.jp2 {granule_dir}/*_B09.jp2 {granule_dir}/*_B1[0-2].jp2".format(
                    scene_folder=scene_folder, granule_dir=granule_folder)
                subprocess.check_call(cmd1,shell=True)
                logger.info('{}: Bands have been stacked separately successfully!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Raster stacks have not been created properly, check if allbands.vrt. Error: {}'.format(scene_folder,e))
                exit(1)
            try:
                # make a separate image of the per-pixel sun and satellite angles. INSPIRE.xml
                cmd2 = "fmask_sentinel2makeAnglesImage.py -i {mtd_folder}/*.xml -o {scene_folder}/angles.img".format(
                    mtd_folder=mtd_folder, scene_folder=scene_folder)
                subprocess.check_call(cmd2,shell=True)
                logger.info('{}: Image of the per-pixel sun and satellite angles have been created successfully!'.format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception('{}: Image of the per-pixel sun and satellite angles have not been created properly, check if angles.img exist. \nError: {}'.format(scene_folder,e))
                exit(1)
            try:
                # create the cloud mask output image. This assumes the bands are in a particular order (as created in the vrt above)
                cmd3 = "fmask_sentinel2Stacked.py -a {scene_folder}/allbands.vrt -z {scene_folder}/angles.img -o {scene_folder}/cloud.img".format(
                    scene_folder=scene_folder)
                subprocess.check_call(cmd3,shell=True)
                logger.info("{}: Cloud image has been created successfully".format(scene_folder))
            except subprocess.CalledProcessError as e:
                logger.exception("{}: Cloud image has not been created properly, check if cloud.img exist. \nError: {}".format(scene_folder))
                exit(1)

    def _cal_evi(self):
        for scene_folder in self.sub_dirs:
            os.chdir(scene_folder)
            toa_image = os.path.join(scene_folder, config.toaImage_s2)
            try:
                img = gdal.Open(toa_image)
                b2 = img.GetRasterBand(2).ReadAsArray()
                b4 = img.GetRasterBand(4).ReadAsArray()
                b8 = img.GetRasterBand(8).ReadAsArray()
                b2 = b2.astype(float) / 10000
                b4 = b4.astype(float) / 10000
                b8 = b8.astype(float) / 10000
                with np.errstate(divide='ignore'):
                    evi = 2.5 * ((b8 - b4) / (b8 + 6 * b4 - 7.5 * b2 + 1))
                    logger.info("{}: EVI calculation has completed successfully".format(scene_folder))
            except RuntimeError as e:
                logger.exception("EVI calculation has not completed, check toa_image and function used to calculate EVI. Error: {}".format(scene_folder, e))
                exit(1)
            cloud = os.path.join(scene_folder, config.cloud)
            try:
                cloud = gdal.Open(cloud)
                cl = cloud.GetRasterBand(1).ReadAsArray()  # int value range: 0-5
                cloudmask = np.where(cl == 2)  # cloud is coded 2
                shadowmask = np.where(cl == 3)  # shadow is coded 3
                nanvalues = np.where(b2 == 32767)
                evi[cloudmask] = np.nan
                evi[shadowmask] = np.nan
                evi[nanvalues] = np.nan
                logger.info("{}: Cloud and shadow masks have been applied to EVI metrics".format(scene_folder))
            except RuntimeError as e:
                logger.exception("{}: Cloud and shadow masks failed to apply to EVI metrics. \nError: {}".format(scene_folder, e))
                exit(1)

            try:
                geo_transform = img.GetGeoTransform()
                projection = img.GetProjection()
                cols = img.RasterXSize
                rows = img.RasterYSize
                in_name = scene_folder.split('/')[-1].split('.')[0]
                out_file_name = 's2' + in_name.split('_')[5] + in_name.split('_')[2][:8] + 'T000000_EVI.tif'
                driver = gdal.GetDriverByName('GTiff')
                dataset = driver.Create(out_file_name, cols, rows, 1, gdal.GDT_Float32)
                dataset.SetGeoTransform(geo_transform)
                dataset.SetProjection(projection)
                band = dataset.GetRasterBand(1)
                band.WriteArray(evi)
                dataset = None
                logger.info("{}: EVI geotiff has been created".format(scene_folder))
            except RuntimeError as e:
                logger.exception("{}: Failed to write EVI to a new geotiff. \nError: {}".format(scene_folder, e))
                exit(1)