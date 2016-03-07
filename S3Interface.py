import boto
import os

class S3FileManager:

	###############################################################################
	#
	# Class variables
	#
	##############################################################################
	_MAX_SIZE = 1e+9
	_CHUNK_SIZE = 52428800

	# Constructor
	def __init__(self):
		self._aws_connection = boto.connect_s3()

	# Public function to get a bucket
	def getBucket(self, aws_bucketname):
		return self._getBucket(aws_bucketname)

	# Public function to check if bucket exists
	def bucketExists(self, bucket_name):
		return self._bucketExists(bucket_name) != None

	# Public function to return file names in bucket
	def getFileNamesInBucket(self, aws_bucketname):
		if not self._bucketExists(aws_bucketname):
			raise Exception("S3FileManager.getFileNamesInBucket: Bucket doesn't exist: %s" % aws_bucketname)
		# Get the files
		bucket = self._aws_connection.get_bucket(aws_bucketname)
		return map(lambda aws_file_key: aws_file_key.name, bucket.list())

	# Public function to download file to local directory
	def downloadFile(self, filename, aws_bucketname, local_download_directory, overwrite=True):
		if not self._bucketExists(aws_bucketname):
			raise Exception("S3FileManager.downloadFile: Bucket doesn't exist: %s" % aws_bucketname)

		bucket = self._aws_connection.get_bucket(aws_bucketname)

		if not self._fileExists(filename, bucket):
			raise Exception("S3FileManager.downloadFile: File doesn't exist: %s" % filename)

		dirname = os.path.join(local_download_directory,os.path.dirname(filename))
		if not os.path.isdir(dirname):
			raise Exception("S3FileManager.downloadFile: Download folder doesn't exist: %s" % dirname)

		destfile = os.path.join(local_download_directory,filename)
		full_local_path = os.path.expanduser(destfile)

		if not os.path.isfile(destfile) or overwrite:
			key = bucket.lookup(filename)
			key.get_contents_to_filename(full_local_path)


	def deleteFile(self, filename, aws_bucketname):
		if not self._bucketExists(aws_bucketname):
			raise Exception("S3FileManager.deleteFile: Bucket doesn't exist: %s" % aws_bucketname)
		bucket = self._aws_connection.get_bucket(aws_bucketname)
		if not self._fileExists(filename, bucket):
			raise Exception("S3FileManager.deleteFile: File doesn't exist: %s" % filename)
		return self._deleteFile(filename, bucket)


	def renameFile(self, filename, newfilename, aws_bucketname):
		if filename == newfilename:
			raise Exception("S3FileManager.renameFile: Newfilename (%s) is the same name as filename (%s)" % (newfilename, filename))
		if not self._bucketExists(aws_bucketname):
			raise Exception("S3FileManager.renameFile: Bucket doesn't exist: %s" % aws_bucketname)
		bucket = self._aws_connection.get_bucket(aws_bucketname)
		if not self._fileExists(filename, bucket):
			raise Exception("S3FileManager.renameFile: File doesn't exist: %s" % filename)
		if self._fileExists(newfilename, bucket):
			raise Exception("S3FileManager.renameFile: File name to be renamed to already exists: %s" % newfilename)

		if self._copyFile(filename, bucket, aws_bucketname, newfilename) is not None:
			if self._fileExists(newfilename, bucket):
				if self._deleteFile(filename, bucket) is not None:
					return True
				else:
					return False
			else:
				return False
		else:
			return False


	def safeCopyFile(self,filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = ''):
		if not self._bucketExists(aws_bucketnamesrc):
			raise Exception("S3FileManager.safeCopyFile: Source bucket doesn't exist: %s" % aws_bucketnamesrc)

		if not self._bucketExists(aws_bucketnamedest):
			raise Exception("S3FileManager.safeCopyFile: Destination bucket doesn't exist: %s" % aws_bucketnamedest)

		srcbucket = self._aws_connection.get_bucket(aws_bucketnamesrc)

		if not self._fileExists(filename, srcbucket):
			raise Exception("S3FileManager.safeCopyFile: Source File doesn't exist: %s" % filename)

		destfile = filename
		if newfilename != '':
			destfile = newfilename
		basenamex = os.path.basename(destfile)
		head, tail = os.path.splitext(basenamex)
		dirname = os.path.dirname(destfile)
		destbucket = self._aws_connection.get_bucket(aws_bucketnamedest)
		count = 0
		while self._fileExists(destfile, destbucket):
			count += 1
			backupfile = os.path.join(dirname,'%s-%d%s' % (head, count, tail))
			if not self._fileExists(backupfile, destbucket):
				if self._copyFile(destfile, srcbucket, aws_bucketnamedest, backupfile) != None:
					self._deleteFile(destfile, destbucket)
				else:
					raise Exception("S3FileManager.safeCopyFile: Error in renaming backupfile: %s -> %s" % (destfile, backupfile))

		if self._copyFile(filename, srcbucket, aws_bucketnamedest, destfile) != None:
			return count
		else:
			raise Exception("S3FileManager.safeCopyFile: Error occurred in copying sourcefile (%s->%s) from source bucket "
							"(%s) to destination bucket (%s)" % (filename, destfile, aws_bucketnamesrc, aws_bucketnamedest))


	def copyFile(self, filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = '', overwrite=True):
		if not self._bucketExists(aws_bucketnamesrc):
			raise Exception("S3FileManager.copyFile: Source bucket doesn't exist: %s" % aws_bucketnamesrc)

		if not self._bucketExists(aws_bucketnamedest):
			raise Exception("S3FileManager.copyFile: Destination bucket doesn't exist: %s" % aws_bucketnamedest)

		srcbucket = self._aws_connection.get_bucket(aws_bucketnamesrc)

		if not self._fileExists(filename, srcbucket):
			raise Exception("S3FileManager.copyFile: Source File doesn't exist: %s" % filename)

		destfile = filename
		if newfilename != '':
			destfile = newfilename

		destbucket = self._aws_connection.get_bucket(aws_bucketnamedest)

		if not self._fileExists(destfile, destbucket) or overwrite:
			return self._copyFile(filename, srcbucket, aws_bucketnamedest, newfilename)
		else:
			return None


	def moveFile(self, filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = '', overwrite=True):
		if not self._bucketExists(aws_bucketnamesrc):
			raise Exception("S3FileManager.moveFile: Source bucket doesn't exist: %s" % aws_bucketnamesrc)

		if not self._bucketExists(aws_bucketnamedest):
			raise Exception("S3FileManager.moveFile: Destination bucket doesn't exist: %s" % aws_bucketnamedest)

		srcbucket = self._aws_connection.get_bucket(aws_bucketnamesrc)

		if not self._fileExists(filename, srcbucket):
			raise Exception("S3FileManager.moveFile: Source File doesn't exist: %s" % filename)

		destfile = filename
		if newfilename != '':
			destfile = newfilename

		destbucket = self._aws_connection.get_bucket(aws_bucketnamedest)

		if not self._fileExists(destfile, destbucket) or overwrite:
			if self._copyFile(filename, srcbucket, aws_bucketnamedest, destfile) is not None:
				return self._deleteFile(filename, srcbucket)
			else:
				raise Exception("S3FileManager.moveFile: Error occurred in moving sourcefile (%s->%s) from source bucket "
							"(%s) to destination bucket (%s)" % (filename, destfile, aws_bucketnamesrc, aws_bucketnamedest))
		else:
			return None

	def safeMoveFile(self, filename, aws_bucketnamesrc, aws_bucketnamedest,  newfilename = ''):
		if not self._bucketExists(aws_bucketnamesrc):
			raise Exception("S3FileManager: Source bucket doesn't exist")

		if not self._bucketExists(aws_bucketnamedest):
			raise Exception("S3FileManager: Destination bucket doesn't exist")

		srcbucket = self._aws_connection.get_bucket(aws_bucketnamesrc)

		if not self._fileExists(filename, srcbucket):
			raise Exception("S3FileManager: Source File doesn't exist")

		destfile = filename
		if newfilename != '':
			destfile = newfilename
		basenamex = os.path.basename(destfile)
		head, tail = os.path.splitext(basenamex)
		dirname = os.path.dirname(destfile)
		destbucket = self._aws_connection.get_bucket(aws_bucketnamedest)
		count = 0
		while self._fileExists(destfile, destbucket):
			count += 1
			backupfile = os.path.join(dirname,'%s-%d%s' % (head, count, tail))
			if not self._fileExists(backupfile, destbucket):
				if self._copyFile(destfile, srcbucket, aws_bucketnamedest, backupfile) != None:
					self._deleteFile(destfile, destbucket)
				else:
					raise Exception("S3FileManager.safeMoveFile: Error in renaming backupfile: %s -> %s" % (destfile, backupfile))

		if self._copyFile(filename, srcbucket, aws_bucketnamedest, destfile) is not None:
			if self._deleteFile(filename, srcbucket):
				return count
			else:
				raise Exception("S3FileManager.safeMoveFile: Error occurred in deleting file: %s" % filename)
		else:
			raise Exception("S3FileManager.safeMoveFile: Error occurred in moving sourcefile (%s->%s) from source bucket "
							"(%s) to destination bucket (%s)" % (filename, destfile, aws_bucketnamesrc, aws_bucketnamedest))


	def fileExists(self, filename, aws_bucketname):
		bucket = self._aws_connection.get_bucket(aws_bucketname)
		return self._fileExists(filename, bucket)


	def uploadFile(self, filename, keyname, aws_bucketname):
		if not self._bucketExists(aws_bucketname):
			raise Exception("S3FileManager.uploadFile: Bucket doesn't exist: %s" % aws_bucketname)

		if not os.path.isfile(filename):
			raise Exception("S3FileManager: File doesn't exist: %s" % filename)

		bucket = self._aws_connection.get_bucket(aws_bucketname)

		mb_size = os.path.getsize(filename)
		if mb_size < self._MAX_SIZE:
			self._standardUpload(filename, keyname, bucket)
		else:
			self._multipart_upload(filename,keyname, bucket, mb_size)

		if self._fileExists(keyname, bucket):
			return True
		else:
			return False

	##########################################################################################
	#
	# Class private methods
	#
	#########################################################################################
	def _deleteFile(self, filename, bucket):
		return bucket.delete_key(filename)


	def _copyFile(self, filename, srcbucket, aws_bucketnamedest, destfile):
		key = srcbucket.lookup(filename)
		return key.copy(aws_bucketnamedest,destfile)


	def _fileExists(self, filename, bucket):
		if bucket.get_key(filename) is not None:
			return True
		else:
			return False


	def _standardUpload(self, filename, keyname, bucket):
		k = boto.s3.key.Key(bucket)
		k.key = keyname
		k.set_contents_from_filename(filename,replace=True)


	def _multipart_upload(self, filename, keyname, bucket, mb_size):
		mp = bucket.initiate_multipart_upload(keyname)
		chunk_size = self._CHUNK_SIZE
		fp = open(filename,'rb')
		fp_num = 0
		while (fp.tell() < mb_size):
			fp_num += 1
			mp.upload_part_from_file(fp, fp_num, size=chunk_size)
		mp.complete_upload()


	def _getBucket(self, aws_bucketname):
		return self._aws_connection.get_bucket(aws_bucketname)


	def _bucketExists(self, bucket_name):
		return self._aws_connection.lookup(bucket_name) != None
