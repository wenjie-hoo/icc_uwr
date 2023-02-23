import subprocess
import os
import boto3
import time
import imghdr
import re
import configparser
import shutil
import schedule
from cloudpathlib import CloudPath

conf = configparser.ConfigParser()
conf.read('config.py')
CN_REGION_NAME = conf.get('s3_credentials', 'region_name')
CN_S3_AKI = conf.get('s3_credentials', 'aws_access_key_id')
CN_S3_SAK = conf.get('s3_credentials','aws_secret_access_key')
bucket_name = conf.get('s3_credentials','BUCKET_NAME')
client = boto3.client('s3', region_name=CN_REGION_NAME,
                  aws_access_key_id=CN_S3_AKI, aws_secret_access_key=CN_S3_SAK)

# download pics
def check_duplicate(path, pics_list):
    files_list = []
    for root, dirs, files in os.walk(path):
        for f in files:
            files_list.append(f)
    return list(set(pics_list) - set(files_list))
  

def getfiles(pic_path):
    filenames=os.listdir(pic_path)
    return filenames

def mkdir_folder(folder_path,classes_list):
    if os.path.exists(folder_path):
        for i in classes_list:
            path = folder_path + str(i)+'/'
            if os.path.exists(path):
                continue
            else:
                os.mkdir(path)
        return
    else:
        os.mkdir(folder_path)
        mkdir_folder(folder_path,classes_list)
        
def download_from_s3(s3_path, loc_path):
    download_pics = CloudPath(s3_path)
    download_pics.download_to(loc_path)
    
def upload_to_s3():
    print('uploading to s3...')
    output = subprocess.getoutput('aws s3 cp ./cate_pics/ s3://wenjie-hoo/icc/categorized/ --recursive')
    print(output)
    print('done!')
        
def copy2cate(foler_path,pic,class_list):
    pic_path = './data/pics/'+str(pic)
    for i in class_list:
        cate_path = foler_path+str(i)+'/'
        try:
            shutil.copy2(pic_path, cate_path)
        except:
            print('Unable to copy pics')
            
def main():
    download_from_s3('s3://wenjie-hoo/icc/pics/','data/pics/')
    pics_path = './data/pics/'
    foler_path = './cate_pics/'
    with open('./coco_category.txt') as f:
        classes =f.read().splitlines() 
        classes.append('uncategorized')
    imgType_list = {'jpg','bmp','png','jpeg','rgb','tif'}
    mkdir_folder(foler_path,classes)    
    pics_list = check_duplicate('./cate_pics', getfiles(pics_path))
    # print(pics_list)
    for pic in pics_list:
        if imghdr.what(pics_path + pic) in imgType_list:
            print('processing'+pic+'...')
            yolo_cmd = './darknet detect cfg/yolov3-tiny.cfg yolov3-tiny.weights data/pics/'+str(pic)
            output = subprocess.getoutput(yolo_cmd)
            print(output)
            output = output[output.index("seconds")+8:]
            class_list = list(set(re.findall(r"(?=("+'|'.join(classes)+r"))", output)))
            if class_list:
                copy2cate(foler_path,pic,class_list)
            else:
                shutil.copy2(pics_path+str(pic), './cate_pics/uncategorized/')
    upload_to_s3()

if __name__ == "__main__":
    main()
    # schedule.every(3).minutes.do(main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)