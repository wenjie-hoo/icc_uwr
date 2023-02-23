from flask import Flask, redirect, render_template, url_for, request, flash
import boto3
from werkzeug.utils import secure_filename
import os

app=Flask(__name__)
app.secret_key = "my secret key"
cwd=os.getcwd()
print(cwd)

CN_S3_AKI = ''
CN_S3_SAK = ''
 
CN_REGION_NAME = 'eu-central-1'  # 区域
 
myS3Client = boto3.client('s3', region_name=CN_REGION_NAME,
                  aws_access_key_id=CN_S3_AKI, aws_secret_access_key=CN_S3_SAK)


if not os.path.isdir('TEMPDIR'):
    os.mkdir('TEMPDIR')

AllowedFileType=['jpg','bmp','png','jpeg','rgb','tif']

@app.route('/')
def main():
    list_of_buckets=[]
    response = myS3Client.list_buckets()
    for buckets in response['Buckets']:
        list_of_buckets.append(buckets['Name'])
    return render_template("index.html",bucketlist=list_of_buckets)

@app.route('/upload', methods=['POST'])
def fileupload():

    file = request.files['file']
    bucketname=request.form.get('bucketname')
    filename = secure_filename(file.filename)

    fextension=filename.split(".")

    if fextension[1] not in AllowedFileType:
        flash("Not a valid file to upload, Allowed extensions are 'jpg','bmp','png','jpeg','rgb','tif'")
        return redirect("/")

    if filename =="":
        flash("No files Selected.., Choose a file to upload")
        return redirect("/")

    filepath=cwd+"/TEMPDIR/"+filename
    print('filepath:',filepath)
    file.save(filepath)
    #begin upload to S3
    myS3Client.upload_file(filepath,bucketname,'icc/pics/'+filename)
    flash("File Upload Succesful")
    #remove temp file
    os.remove(filepath)
    return redirect("/")

if __name__=='__main__':
    app.run()