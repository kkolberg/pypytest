'''
This script performs efficient concatenation of files stored in S3. Given a
folder, output location, and optional suffix, all files with the given suffix
will be concatenated into one file stored in the output location.
Concatenation is performed within S3 when possible, falling back to local
operations when necessary.
Run `python combineS3Files.py -h` for more info.
'''
from time import time
import os
import argparse
import logging
import boto3

# Script expects everything to happen in one bucket
BUCKET = ""  # set by command line args
# S3 multi-part upload parts must be larger than 5mb
MIN_S3_SIZE = 5500000
# Setup logger to display timestamp
logging.basicConfig(format='%(asctime)s => %(message)s')

def process_concatenation(args):
    """
    Coordinate concatenation of files in an S3 Bucket
    """
    logging.warning(
        "Assembling files in %s/%s to %s/%s, with a max size of %s bytes",
        BUCKET, args.folder, BUCKET, args.output, args.filesize)
    __s3 = new_s3_client()
    parts = generate_stats(__s3, args.folder, args.suffix, args.filesize)
    if args.mode == "stat":
        return

    if args.mode == "full":
        run_full_concatenation(__s3, parts, args.output)
    else:
        logging.warning(
            "Assembling index %s, file %s/%s",
            args.index,
            args.index + 1,
            len(parts))
        run_single_concatenation(
            __s3,
            parts[args.index],
            "{}-{}".format(args.output, args.index))


def generate_stats(__s3, folder_to_concatenate, file_suffix, max_filesize):
    """
    Run Concatenation
    """
    parts_list = collect_parts(__s3, folder_to_concatenate, file_suffix)
    logging.warning(
        "Found %s parts to concatenate in %s/%s", len(parts_list), BUCKET, folder_to_concatenate)
    grouped_parts_list = chunk_by_size(parts_list, max_filesize)
    logging.warning("Assemble %s output files", len(grouped_parts_list))
    return grouped_parts_list


def run_full_concatenation(__s3, grouped_parts_list, result_filepath):
    """
    Run Concatenation
    """
    for i, parts in enumerate(grouped_parts_list):
        logging.warning(
            "Assembling group %s/%s", i, len(grouped_parts_list))
        run_single_concatenation(
            __s3, parts, "{}-{}".format(result_filepath, i))


def run_single_concatenation(__s3, parts_list, result_filepath):
    """
    run single concatenation
    """
    if len(parts_list) > 1:
        # perform multi-part upload
        upload_id = initiate_concatenation(__s3, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(
            __s3, result_filepath, upload_id, parts_list)
        complete_concatenation(__s3, result_filepath, upload_id, parts_mapping)
    elif len(parts_list) == 1:
        # can perform a simple S3 copy since there is just a single file
        resp = __s3.copy_object(
            Bucket=BUCKET, CopySource="{}/{}".format(BUCKET, parts_list[0][0]), Key=result_filepath)
        logging.warning(
            "Copied single file to %s and got response %s", result_filepath, resp)
    else:
        logging.warning("No files to concatenate for %s", result_filepath)


def chunk_by_size(parts_list, max_filesize):
    """
    chunk by size
    """
    grouped_list = []
    current_list = []
    current_size = 0
    for __p in parts_list:
        current_size += __p[1]
        current_list.append(__p)
        if current_size > max_filesize:
            grouped_list.append(current_list)
            current_list = []
            current_size = 0
    # Catch any remainder
    if current_list.count > 0:
        grouped_list.append(current_list)

    return grouped_list


def new_s3_client():
    """
    initialize an S3 client with a private session so that multithreading
    doesn't cause issues with the client's internal state
    """
    session = boto3.session.Session()
    return session.client('s3')


def collect_parts(__s3, folder, suffix):
    """
    collect parts
    """
    return filter(lambda x: x[0].endswith(suffix), _list_all_objects_with_size(__s3, folder))


def _list_all_objects_with_size(__s3, folder):
    """
    list all objects with size
    """

    def resp_to_filelist(resp):
        """
        response to fileList
        """
        return [(x['Key'], x['Size']) for x in resp['Contents']]

    objects_list = []
    resp = __s3.list_objects(Bucket=BUCKET, Prefix=folder)
    objects_list.extend(resp_to_filelist(resp))
    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next
        # request
        logging.warning("Found %s objects so far", len(objects_list))
        last_key = objects_list[-1][0]
        resp = __s3.list_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
        objects_list.extend(resp_to_filelist(resp))

    return objects_list


def initiate_concatenation(__s3, result_filename):
    """
    performing the concatenation in S3 requires creating a multi-part upload
    and then referencing the S3 files we wish to concatenate as "parts" of
    that upload
    """
    resp = __s3.create_multipart_upload(Bucket=BUCKET, Key=result_filename)
    logging.warning(
        "Initiated concatenation attempt for %s, and got response: %s",
        result_filename,
        resp)
    return resp['UploadId']


def assemble_parts_to_concatenate(__s3, result_filename, upload_id, parts_list):
    """
    assemble parts to concatenate
    """
    parts_mapping = []
    part_num = 0

    s3_parts = ["{}/{}".format(BUCKET, p[0])
                for p in parts_list if p[1] > MIN_S3_SIZE]
    local_parts = [p[0] for p in parts_list if p[1] <= MIN_S3_SIZE]

    # assemble parts large enough for direct S3 copy
    # part numbers are 1 indexed
    for part_num, source_part in enumerate(s3_parts, 1):
        resp = __s3.upload_part_copy(Bucket=BUCKET,
                                     Key=result_filename,
                                     PartNumber=part_num,
                                     UploadId=upload_id,
                                     CopySource=source_part)
        logging.warning(
            "Setup S3 part #%s, with path: %s, and got response: %s",
            part_num, source_part, resp)
        parts_mapping.append(
            {'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

    # assemble parts too small for direct S3 copy by downloading them locally,
    # combining them, and then reuploading them as the last part of the
    # multi-part upload (which is not constrained to the 5mb limit)
    small_parts = []
    for source_part in local_parts:
        temp_filename = "/tmp/{}".format(source_part.replace("/", "_"))
        __s3.download_file(Bucket=BUCKET, Key=source_part,
                           Filename=temp_filename)

        with open(temp_filename, 'rb') as f:
            small_parts.append(f.read())
        os.remove(temp_filename)
        logging.warning(
            "Downloaded and copied small part with path: %s", source_part)

    if len(small_parts) > 0:
        last_part_num = part_num + 1
        last_part = ''.join(small_parts)
        resp = __s3.upload_part(Bucket=BUCKET, Key=result_filename,
                                PartNumber=last_part_num, UploadId=upload_id, Body=last_part)
        logging.warning(
            "Setup local part #%s from %s small files, and got response: %s",
            last_part_num, len(small_parts), resp)
        parts_mapping.append(
            {'ETag': resp['ETag'][1:-1], 'PartNumber': last_part_num})

    return parts_mapping


def complete_concatenation(__s3, result_filename, upload_id, parts_mapping):
    """
    complete concatenation
    """
    if len(parts_mapping) == 0:
        __s3.abort_multipart_upload(
            Bucket=BUCKET, Key=result_filename, UploadId=upload_id)
        logging.warning(
            "Aborted concatenation for file %s, with upload id #%s due to empty parts mapping",
            result_filename, upload_id)
    else:
        __s3.complete_multipart_upload(
            Bucket=BUCKET,
            Key=result_filename,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts_mapping})
        logging.warning(
            "Finished concatenation for file %s, with upload id #%s, and parts mapping: %s",
            result_filename, upload_id, parts_mapping)


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description="S3 file combiner")
    PARSER.add_argument(
        "--bucket",
        help="base bucket to use")
    PARSER.add_argument(
        "--folder",
        default="data/",
        help="folder whose contents should be combined")
    PARSER.add_argument(
        "--output",
        default="combined/part",
        help="output location for resulting merged files, relative to the specified base bucket")
    PARSER.add_argument(
        "--suffix",
        default=".json",
        help="suffix of files to include in the combination")
    PARSER.add_argument(
        "--filesize",
        type=int,
        default=1073741824,
        help="max filesize of the concatenated files in bytes")
    PARSER.add_argument(
        "--mode",
        default="full",
        help="full - full combination, " +
        "stat - stat out number of output files and sizes, " +
        "single - run a single rollup"
    )
    PARSER.add_argument(
        "--index",
        type=int,
        default=0,
        nargs="?",
        help="for single combine operations, the file to combine. 0-based index."
    )

    ARGS = PARSER.parse_args()
    BUCKET = ARGS.bucket

    START_TIME = time()
    process_concatenation(ARGS)
    END_TIME = time()
    logging.warning("@@@@Total Execution Time - " + str(END_TIME - START_TIME))

