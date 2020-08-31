import os
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote
from urllib.error import HTTPError
import json
import uuid
from datetime import datetime, timezone
import base64
import xml.etree.cElementTree as ET
import asyncio
import aiohttp

file_api_url = 'http://localhost:8082/api/file/v2/getLocation'
BLOB_SERVICE_API = '2016-05-31'
MAX_BLOB_SIZE = 268435456
MAX_BLOCK_SIZE = 33554432
UPLOAD_TIMEOUT = 15 * 60


def get_bearer_token():
    try:
        tenant_id = os.environ['TENANT_ID']

        body = {
            "grant_type": "client_credentials",
            "client_id": os.environ['ENV_PRINCIPAL_ID'],
            "client_secret": os.environ['ENV_PRINCIPAL_SECRET'],
            "resource": os.environ['ENV_APP_ID']
        }
    except KeyError as e:
        print("Environment variable %s not found. Please check your environment variables\n" % str(e))
        
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = urlencode(body).encode("utf8")
    request = Request(url=f'https://login.microsoftonline.com/{tenant_id}/oauth2/token', data=data, headers=headers)
    try:
        response = urlopen(request)
        response_body = response.read()
        resp = json.loads(response_body)
        token = resp["access_token"]
        print('token: ', token)
    except HTTPError as e:
        print("Failed to get bearer token due to HTTP error: %s\n" % str(e))
        raise
    except Exception as e:
        print("Failed to decode JSON data in the response: %s\n" % str(e))
        raise
    return token

async def get_file_location(partition_id):
    signed_url = ''

    try:
        file_id = str(uuid.uuid4())
        bearer_token = get_bearer_token()

        headers = {
            'Content-Type': 'application/json',
            'Data-Partition-Id': partition_id,
            'Authorization': 'Bearer ' + bearer_token
        }
        
        async with aiohttp.ClientSession() as session:
            r = await session.request('POST', url=file_api_url, headers=headers, data=json.dumps({"FileID": file_id}))
            async with r:
                if r.status == 200:
                    text = await r.text()
                    json_data = json.loads(text)
                    signed_url = json_data['Location']['SignedURL']
                else:
                    print('Error in retrieving signed URL: %s' % (r.json())['message'])
        
        return signed_url
    except aiohttp.ServerConnectionError as e:
        print('Failed to get file location %s. ServerDisconnectedError: %s' % (e.message))
        raise e
    except aiohttp.ServerTimeoutError as e:
        print('Failed to get file location %s. ServerTimeoutError: %s' % (e.message))
        raise e
    except aiohttp.ClientConnectorError as e:
        print('Failed to get file location %s. ClientConnectorError: %s' % (e.message))
        raise e
    except aiohttp.ClientConnectionError as e:
        print('Failed to get file location %s. ClientConnectionError: %s' % (e.message))
        raise e
    except aiohttp.ClientPayloadError as e:
        print('Failed to get file location %s. ClientPayloadError: %s' % (e.message))
        raise e
    except aiohttp.InvalidURL as e:
        print('Failed to get file location %s. InvalidURL: %s' % (e.message))
        raise e
    except aiohttp.ContentTypeError as e:
        print('Failed to get file location %s. ContentTypeError: %s' % (e.message))
        raise e
    except aiohttp.ClientResponseError as e:
        print('Failed to get file location %s. ClientResponseError: %s' % (e.message))
        raise e
    except aiohttp.ClientError as e:
        print('Failed to get file location %s. ClientException: %s' % (e.message))
        raise e
    except Exception as e:
        print('Failed to get file location %s. Unknown error: %s' % (str(e)))
        raise e

def upload_file(local_url, remote_url):
    try:
        file_size = os.path.getsize(local_url)
        loop = asyncio.get_event_loop()
        if file_size <= MAX_BLOB_SIZE:
            print('Local file size: %d. Invoking upload_blob function' % (file_size))
            loop.run_until_complete(upload_blob(local_url, remote_url, file_size))
        else:
            print('Local file size: %d. Invoking upload_blocks function' % (file_size))
            loop.run_until_complete(upload_blocks(local_url, remote_url, file_size))
        
        loop.close()
    except OSError:
        print("Failed to find the file at ", local_url)

async def upload_blob(local_url, remote_url, file_size):
    with open(local_url, 'rb') as f:
        file_data = f.read()
    
    try:
        headers = {
            'Date': str(datetime.now(timezone.utc)),
            'x-ms-version': BLOB_SERVICE_API,
            'Content-Length': str(file_size),
            'Content-Type': 'application/octet-stream',
            'x-ms-blob-type': 'BlockBlob'
        }
        timeout = aiohttp.ClientTimeout(total = UPLOAD_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            r = await session.request('PUT', url=remote_url, data=file_data, headers=headers)
        async with r:
            if r.status == 201:
                print('Succeeded in uploading file ', local_url)
            else:
                print('Failed to upload blob %s. Status code: %d' % (local_url, r.status))
    except Exception as e:
        print('Failed to upload blob %s: %s' % (local_url, str(e)))

async def upload_blocks(local_url, remote_url, file_size):
    remained_data_size = file_size
    block_ids = []
    uploads = []
    try:
        async with aiohttp.ClientSession() as session:
            with open(local_url, 'rb') as f:
                while remained_data_size > 0:
                    read_size = min(remained_data_size, MAX_BLOCK_SIZE)
                    data_block = f.read(read_size)

                    block_id = get_block_id()
                    block_ids.append(block_id)
                    upload_f = upload_block(session, remote_url, block_id, read_size, data_block)
                    uploads.append(upload_f)

                    remained_data_size -= read_size
                    asyncio.sleep(1)
        
            upload_results = await asyncio.gather(*uploads, return_exceptions=True)
            for i in range(len(upload_results)):
                if isinstance(upload_results[i], BaseException):
                    raise upload_results[i]
            commit_result = await commit_block_list(session, remote_url, block_ids)
            if isinstance(commit_result, BaseException):
                raise commit_result
    except Exception as e:
        print('Failed to upload block list to Azure blob storage: ', str(e))

async def upload_block(session, remote_url, block_id_str, read_size, data_block):
    try:
        target_url = f'{remote_url}&comp=block&blockid={block_id_str}'
        headers = {
            'Date': str(datetime.now(timezone.utc)),
            'x-ms-version': BLOB_SERVICE_API,
            'Content-Length': str(read_size)
        }
        print('Trying to send a block: %s with size of %d' % (block_id_str, read_size))
        
        timeout = aiohttp.ClientTimeout(total=UPLOAD_TIMEOUT)
        r = await session.request('PUT', url=target_url, data=data_block, headers=headers, timeout=timeout)
        print('response error: %d' % (r.status))

        async with r:
            if r.status == 201:
                print('Succeeded in uploading block: ', block_id_str)
            else:
                print('Failed to upload block with error code ', r.status)
    except aiohttp.ServerConnectionError as e:
        print('Failed to upload block %s. ServerDisconnectedError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ServerTimeoutError as e:
        print('Failed to upload block %s. ServerTimeoutError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ClientConnectorError as e:
        print('Failed to upload block %s. ClientConnectorError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ClientConnectionError as e:
        print('Failed to upload block %s. ClientConnectionError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ClientPayloadError as e:
        print('Failed to upload block %s. ClientPayloadError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.InvalidURL as e:
        print('Failed to upload block %s. InvalidURL: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ContentTypeError as e:
        print('Failed to upload block %s. ContentTypeError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ClientResponseError as e:
        print('Failed to upload block %s. ClientResponseError: %s' % (block_id_str, e.message))
        raise e
    except aiohttp.ClientError as e:
        print('Failed to upload block %s. ClientException: %s' % (block_id_str, e.message))
        raise e
    except Exception as e:
        print('Failed to upload block %s. Unknown error: %s' % (block_id_str, str(e)))
        raise e
    return r

def get_block_id():
    block_id = str(uuid.uuid4())
    block_id_byte = base64.urlsafe_b64encode(block_id.encode('utf-8'))
    block_id_str = str(block_id_byte, 'utf-8')
    return block_id_str

async def commit_block_list(session, remote_url, block_ids):
    try:
        target_url = f'{remote_url}&comp=blocklist'
        block_list_xml = create_block_list_xml(block_ids)
        print('block list xml: \n', block_list_xml)

        headers = {
            'Date': str(datetime.now(timezone.utc)),
            'x-ms-version': BLOB_SERVICE_API,
            'Content-Length': str(len(block_list_xml))
        }

        r = await session.request('PUT', url=target_url, data=block_list_xml, headers=headers)
        async with r:
            if r.status == 201:
                print('Succeeded in committing block list')
            else:
                print('Failed to commit block list: Status code: %d    Reason: %s' % (r.status, r.reason))
    except aiohttp.ServerConnectionError as e:
        print('Failed to commit block list. ServerDisconnectedError: %s' % (e.message))
        raise e
    except aiohttp.ServerTimeoutError as e:
        print('Failed to commit block list. ServerTimeoutError: %s' % (e.message))
        raise e
    except aiohttp.ClientConnectorError as e:
        print('Failed to commit block list. ClientConnectorError: %s' % (e.message))
        raise e
    except aiohttp.ClientConnectionError as e:
        print('Failed to commit block list. ClientConnectionError: %s' % (e.message))
        raise e
    except aiohttp.ClientPayloadError as e:
        print('Failed to commit block list. ClientPayloadError: %s' % (e.message))
        raise e
    except aiohttp.InvalidURL as e:
        print('Failed to commit block list. InvalidURL: %s' % (e.message))
        raise e
    except aiohttp.ContentTypeError as e:
        print('Failed to commit block list. ContentTypeError: %s' % (e.message))
        raise e
    except aiohttp.ClientResponseError as e:
        print('Failed to commit block list. ClientResponseError: %s' % (e.message))
        raise e
    except aiohttp.ClientError as e:
        print('Failed to commit block list. ClientException: %s' % (e.message))
        raise e
    except Exception as e:
        print('Failed to commit block list. Unknown error: %s' % (str(e)))
        raise e
    return r

def create_block_list_xml(block_ids):
    block_list = ET.Element('BlockList')
    for i in range(len(block_ids)):
        block_id = ET.SubElement(block_list, 'Latest')
        block_id.text = block_ids[i]
    return ET.tostring(block_list, encoding='utf-8', method='xml')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    signed_url = loop.run_until_complete(get_file_location('opendes'))
    
    print('signedURL: ', signed_url)

    # 70_driver_log.txt size: 26,624 bytes
    #local_file = '/mnt/c/Codespace/osdu/utils/sample_file/70_driver_log.txt'

    # icons.jar size: 28,358,119 bytes
    # local_file = '/mnt/c/Codespace/osdu/utils/sample_file/icons.jar'

    # Anaconda3-2020.02-Windows-x86_64.exe size: 488,908,696 bytes
    local_file = '/mnt/c/Software/Python/Anaconda3-2020.02-Windows-x86_64.exe'
    if signed_url != '':
        upload_file(local_file, signed_url)
    
    