# S3FileManager
Simple python class to upload and download files from AWS S3

# Synopsis

Developed for a larger project, this simple class allows you to easily interface with Amazon Web Services Simple Storge Service using boto. Really just a wrapper for boto that attempts to make it easier to work with objects in S3 as more of a file management system. Currently it is implemented as a standalone class file. So for a python project is still rudimentary, but should work if implemented into a larger project. Based in part on https://github.com/mattnedrich/S3.FMA. Future development will turn this into a more comprehensive package. 

# Code Example

You must have set-up an AWS account stored your Security Credentials (Access Key ID & Secret Access Key) using the system wide password file. The filemanager can then be added to your project by the following call:

```
s3filemanager = S3Interface.S3FileManager()
```

and one can then loop through all the files in a bucket

if(s3filemanager.bucketExists("samplebucket")):
  bucketlist = s3filemanager.getFileNamesInBucket("samplebucket")
	for file in bucketlist:
  	print("file name:%s" % file)


# API

The class offers the following options:

Check if the bucket exists
``
bucketExists(bucket_name)
``

Get all the file names in the bucket to loop through
```
getFileNamesInBucket(aws_bucketname):
```

Check if a file exists
``
fileExists(filename, aws_bucketname)
``

Download a single file
```
downloadFile(filename, aws_bucketname, local_download_directory, overwrite=True)
```

Delete a file
```
deleteFile(filename, aws_bucketname)
```

Rename a file
```
renameFile(filename, newfilename, aws_bucketname)
```

Copy a file from one bucket to another
```
copyFile(filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = '', overwrite=True)
```

Safely copy a file from one bucket to another (if the file in the destination bucket exists it will rename it with a counter).
```
safeCopyFile(filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = '')
```

Move a file from one bucket to another
```
moveFile(filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = '', overwrite=True)
```

Safely move a file from one bucket to another
```
safeMoveFile(filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = '')
```

Upload a file to a bucket
```
uploadFile(filename, keyname, aws_bucketname)
```

Get a bucket info. This is a slightly lower level call than getfilenames
```
getBucket(aws_bucketname)
```

# Motivation

Developed as wrapper to boto access to S3 for easily uploading and downloading files from S3 using python. Was built as part of a more specific project that was designed to securely manage data files uploaded by SFTP to an EC2 server.

# License

Offered free under the GNU GENERAL PUBLIC LICENSE v3. Was developed because was having trouble using fuse over s3, and this was a much more reliable way to manage files on S3 through EC2, so hopefully useful for a project you are working on.
