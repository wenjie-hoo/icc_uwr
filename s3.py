import os
import boto3
import configparser
from loguru import logger
 
conf = configparser.ConfigParser()
conf.read('config.py')
CN_REGION_NAME = conf.get('s3_credentials', 'region_name')
CN_S3_AKI = conf.get('s3_credentials', 'aws_access_key_id')
CN_S3_SAK = conf.get('s3_credentials','aws_secret_access_key')
BUCKET_NAME = conf.get('s3_credentials','bucket_name')
 
s3 = boto3.client('s3', region_name=CN_REGION_NAME,
                  aws_access_key_id=CN_S3_AKI, aws_secret_access_key=CN_S3_SAK)
 
 
def upload_files(path_local, path_s3):
    logger.info(f'Start upload files.')
 
    if not upload_single_file(path_local, path_s3):
        logger.error(f'Upload files failed.')
 
    logger.info(f'Upload files successful.')
 
 
def upload_single_file(src_local_path, dest_s3_path):
    try:
        with open(src_local_path, 'rb') as f:
            s3.upload_fileobj(f, BUCKET_NAME, dest_s3_path)
    except Exception as e:
        logger.error(f'Upload data failed. | src: {src_local_path} | dest: {dest_s3_path} | Exception: {e}')
        return False
    logger.info(f'Uploading file successful. | src: {src_local_path} | dest: {dest_s3_path}')
    return True
 
 
def download_zip(path_s3, path_local):
    retry = 0
    while retry < 3:  # 下载异常尝试3次
        logger.info(f'Start downloading files. | path_s3: {path_s3} | path_local: {path_local}')
        try:
            s3.download_file(BUCKET_NAME, path_s3, path_local)
            file_size = os.path.getsize(path_local)
            logger.info(f'Downloading completed. | size: {round(file_size / 1048576, 2)} MB')
            break  # 下载完成后退出重试
        except Exception as e:
            logger.error(f'Download zip failed. | Exception: {e}')
            retry += 1
 
    if retry >= 3:
        logger.error(f'Download zip failed after max retry.')
 
 
def delete_s3_zip(path_s3, file_name=''):
    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=path_s3)
    except Exception as e:
        logger.error(f'Delete s3 file failed. | Exception: {e}')
    logger.info(f'Delete s3 file Successful. | path_s3 = {path_s3}')
 
 
def batch_delete_s3(delete_key_list):
    try:
        res = s3.delete_objects(
            Bucket=BUCKET_NAME,
            Delete={'Objects': delete_key_list}
        )
    except Exception as e:
        logger.error(f"Batch delete file failed. | Excepthon: {e}")
    logger.info(f"Batch delete file success. ")
  
 
def get_files_list(Prefix=None):
    logger.info(f'Start getting files from s3.')
    try:
        if Prefix is not None:
            all_obj = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=Prefix)
            # obj = s3.head_object(Bucket=BUCKET_NAME, Key=Prefix)
            # logger.info(f"obj = {obj}")
        else:
            all_obj = s3.list_objects_v2(Bucket=BUCKET_NAME)
 
    except Exception as e:
        logger.error(f'Get files list failed. | Exception: {e}')
        return
 
    contents = all_obj.get('Contents')
    logger.info(f"--- contents = {contents}")
    if not contents:
        return
 
    file_name_list = []
    for zip_obj in contents:
        # logger.info(f"zip_obj = {zip_obj}")
        file_size = round(zip_obj['Size'] / 1024 / 1024, 3)  # 大小
        # logger.info(f"file_path = {zip_obj['Key']}")
        # logger.info(f"LastModified = {zip_obj['LastModified']}")
        # logger.info(f"file_size = {file_size} Mb")
        # zip_name = zip_obj['Key'][len(start_after):]
        zip_name = zip_obj['Key']
 
        file_name_list.append(zip_name)
 
    logger.info(f'Get file list successful.')
 
    return file_name_list

def upload_pics(loc_dir, s3_path):
    
if __name__ == "__main__":
    # file_name_list = get_files_list()
    # print(file_name_list)
    # upload_files('./pics/1200.jpg','icc/pics/')
    upload_single_file('./pics/Asianelephant3.jpg','icc/pics/Asianelephant3.jpg')
    # logger.info(f"file_name_list = {file_name_list}")
    # path_local = './rootkey.csv'
    # path_s3 = 'rootkey.csv'  # s3路径不存在则自动创建
    # download_zip('icc/pics/dog1.jpg','dag.jpg')