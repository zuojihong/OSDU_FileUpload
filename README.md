# File upload sample code
This code snippet shows how to upload files to Azure Storage using REST API. The file upload can involve two kinds of APIs:
* PUT BLOB
* PUT BLOCK + PUT BLOCK LIST

Azure Storage REST API has multiple versions. According to [Azure Storage document](https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs), the API versions from 2016-05-31 up to 2019-07-07 can support max single blob size up to 256 MB and max block size up to 100 MB.

Therefore, we will use PUT BLOB API to upload files less than 256 MB in size. For files greater than 256 MB, it's necessary to divide the files into 100MB chunked blocks and upload the blocks using PUT BLOCK API. Afterwards, PUT BLOCK LIST API should be invoked to commit the file block ids.

To improve performance, we leverage Python asyncio and aiohttp libraries to implement the asynchronous HTTP requests where necessary.

# Dependencies
Python 3.6.9+