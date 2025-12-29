import re
import ast
import io
import time
import socket
import logging
import zlib
import gcsfs
from datetime import datetime, timedelta
from ssh2 import sftp
from ssh2.session import Session
from google.cloud import storage

from dataflow_options.utils import Logger

logger = Logger(__name__)

class SftpClient:

    def __init__(self, host, user, password, port=22) -> None:
        self.host = host
        self.port = port
        self.__user = user
        self.__password = password
        self.create_ssh_session()
    
    def create_ssh_session(self):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((self.host, self.port))
        self.__session = Session()
        self.__session.handshake(self.__sock)
        self.__session.userauth_password(self.__user, self.__password)
        logger.info('Created SSH session')
    
    def disconnect_session(self):
        try:
            self.__session.disconnect()
            self.__sock.close()
            logger.info('Disconnected SSH session')
        except Exception:
            logger.error('Exception occured', exc_info=True)
    
    @staticmethod
    def find_folder_name(args):
        if args.directories and len(eval(args.directories))>0:
            dirs:list[str] = eval(args.directories)
            return [d.rstrip('/') for d in dirs]
        db_config = ast.literal_eval(args.db_config)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((db_config['host'], db_config.get('port', 22)))
        session = Session()
        session.handshake(sock)
        session.userauth_password(db_config['user'], db_config['password'])
        sftp = session.sftp_init()
        parent_dir = db_config['path'] + ("" if db_config['path'].endswith('/') else '/')
        logger.info(f"Root directory {parent_dir}")
        #sftp_file_handle = sftp.opendir(db_config['path'])
        sftp_file_handle = sftp.opendir(parent_dir)
        #last_date  = str(args.filter_param)[:-9].replace('-','')
        last_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        today = datetime.now().strftime("%Y%m%d")
        date_match = re.search('\d{4}-\d{2}-\d{1,2}', str(args.extraction_sync_dt))
        if date_match:
            last_date = date_match.group(0).replace('-','')
        logger.info(f"Last folder date: {last_date}")
        directories = []
        for file_tuple in sftp_file_handle.readdir():
            file_or_dir_name = str(file_tuple[1], 'UTF-8')
            if re.match("\d{4}-\d{1,2}-\d{1,2}", file_or_dir_name):
                try:
                    if int(file_or_dir_name.replace('-','')) > int(last_date) and int(file_or_dir_name.replace('-','')) <= int(today):
                        directories.append(parent_dir + file_or_dir_name)
                    if int(file_or_dir_name.replace('-','')) > int(today):
                        logger.warn(f"warning future folder : {file_or_dir_name.replace('-','')}")
                except Exception as e:
                    logger.error('Exception occured', exc_info=True)

        session.disconnect()
        sock.close()
        logger.info(f"Found {len(directories)} directories {directories}")

        if args.pull_type.lower() == 'full' and len(directories)>0:
            directories = [sorted(directories, reverse=True)[0]]

        return directories

    def listdir(self, remote_dir, prefix=None, suffix=None):
        sftp_client = self.__session.sftp_init()
        logger.info(f"remote_dir {remote_dir}")
        sftp_file_handle = sftp_client.opendir(remote_dir)
        file_pattern = (prefix if prefix else "") + ".*" + (suffix if suffix else "") + "$"
        files_list = []
        for file_tuple in sftp_file_handle.readdir():
            filepath = remote_dir + ("" if remote_dir.endswith("/") else "/") + str(file_tuple[1], 'UTF-8')
            if re.search(file_pattern, filepath) and not filepath.split('/')[-1] in ['.','..']:
                files_list.append(filepath)
        logger.info(f'Found {len(files_list)} file(s) in {remote_dir} with pattern {file_pattern}')
        return files_list
    
    def download_to_gcs(self, remote_file_path, target_gcs_bucket, blob_prefix=None, write_schema=True, delimiter='|'):
        try:
            # Initialize storage client
            storage_client = storage.Client()
            bucket = storage_client.bucket(target_gcs_bucket)

            logger.info(f'Processing {remote_file_path}')

            filename = remote_file_path.split('/')[-1]
            if blob_prefix:
                blobs_prefix = blob_prefix if not blob_prefix.endswith('/') else blob_prefix[:-1]
            else:
                blobs_prefix = ''

            schema_file_prefix = "".join(blobs_prefix.split('/')[-1])

            logger.info(f'Bucket name: {target_gcs_bucket}  Blobs prefix: {blob_prefix} Schema file prefix: {schema_file_prefix}')

            # Initialize SFTP
            sftp_client = self.__session.sftp_init()
            sftp_file_handle = sftp_client.open(remote_file_path, sftp.LIBSSH2_FXF_READ, sftp.LIBSSH2_SFTP_S_IRUSR)
            ACCUMULATED_DATA_THRESHOLD = 512*(2**20) # 512MB
            logger.info(f'ACCUMULATED_DATA_THRESHOLD: {ACCUMULATED_DATA_THRESHOLD} bytes')
            accumulated_data, accumulated_data_size, total_bytes_read = bytearray(), 0, 0
            num_chunks, staging_blobs = 0, []
            header_chunk = None

            start_time = time.time()

            # Read and Upload the file to GCS as chunks
            logger.info(f'Started reading {remote_file_path}')
            for size, data in sftp_file_handle:
                # if not header_chunk:
                #     header_chunk = str(data, 'ISO-8859–1')
                    #header_chunk = str(data,'UTF-8')
                if accumulated_data_size < ACCUMULATED_DATA_THRESHOLD:
                    accumulated_data += data
                    accumulated_data_size += size
                else:
                    total_bytes_read += accumulated_data_size
                    num_chunks += 1
                    blob_name = f'{blobs_prefix}/staging/part-{num_chunks:04d}-{filename}'
                    blob = bucket.blob(blob_name)
                    blob.upload_from_file(io.BytesIO(accumulated_data))
                    logger.info(f'Uploaded {accumulated_data_size} bytes to gs://{target_gcs_bucket}/{blob_name}')   
                    accumulated_data = bytearray()
                    accumulated_data += data
                    accumulated_data_size = size
                    staging_blobs.append(blob_name)
            
            #Write last chunk
            num_chunks += 1
            blob_name = f'{blobs_prefix}/staging/part-{num_chunks:04d}-{filename}'
            blob = bucket.blob(blob_name)
            blob.upload_from_file(io.BytesIO(accumulated_data))
            logger.info(f'Uploaded {accumulated_data_size} bytes to gs://{target_gcs_bucket}/{blob_name}')
            staging_blobs.append(blob_name)
            # Compose the staging blobs
            logger.info(f'Composig all stagig blobs for {filename}')
            destination_blob_name = f'{blobs_prefix}/{filename}'
            destination_blob = bucket.blob(destination_blob_name)
            destination_blob.compose([bucket.get_blob(blob_name) for blob_name in staging_blobs])
            logger.info(f'Finished composing the staging blobs for {filename}')

            # Delete the staging blobs
            logger.info(f'Deleting the staging blobs for {filename}')
            for blob_name in staging_blobs:
                bucket.blob(blob_name).delete()
            logger.info(f'Finished deleting the staging blobs for {filename}')

            if write_schema and not bucket.blob(f'{blobs_prefix}/{schema_file_prefix}-schema.json').exists():
                logger.info(f"Started writing schema for {remote_file_path}")
                gcs_fs = gcsfs.GCSFileSystem()
                with gcs_fs.open(f"gs://{target_gcs_bucket}/{destination_blob_name}","rb") as f:
                    if destination_blob_name.endswith(".gz"):
                        decompressed_bytes = zlib.decompress(f.read(), zlib.MAX_WBITS|32)[:10240]
                        header_chunk = str(decompressed_bytes, 'UTF-8')
                    elif re.search('(.csv|.dat|.txt)$', destination_blob_name):
                        header_chunk = str(f.readline(), 'UTF-8')
                file_header = header_chunk.split('\n')[0]
                logger.info(f"Using '{delimiter}' as delimiter")
                columns = file_header.split(delimiter)
                schema = []
                for col in columns:
                    schema.append({"name": re.sub('[^A-Za-z0-9_]','_',col), "type":"STRING"})
                schema.extend(
                    [
                        #{"name":"FILE_DATE","type":"STRING","default_value_expression":"19900101000000"},
                        {"name":"SYNCSTARTDATETIME","type":"DATETIME","default_value_expression":"CURRENT_DATETIME()"}
                    ]
                )
                filename_without_ext = filename.split(".")[0]
                schema_blob = bucket.blob(f'{blobs_prefix}/{schema_file_prefix}-schema.json')
                schema_blob.upload_from_string(str(schema).replace("'",'"'))
                logger.info(f"Written schema to {blobs_prefix}/{schema_file_prefix}-schema.json")

            end_time = time.time()

            logger.info(f'Total time taken to process {remote_file_path} is {end_time-start_time} seconds')

            return True

        except Exception as e:
            logger.error("Exception occured", exc_info=True)
            raise e