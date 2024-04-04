from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from io import BytesIO


class AzureHandler:

    def __init__(self):
        self.a = []

    def getBlobServiceClient(accountURL, sasToken):
        return BlobServiceClient(account_url=accountURL, credential=sasToken)

    def getContainer(blobServiceClient, container):
        return blobServiceClient.get_container_client(container=container)

    def getBlob(blobServiceClient, container, blob):
        return blobServiceClient.get_blob_client(container=container, blob=blob)

    def listBlobs(containerClient, startString):
        blobList = containerClient.list_blobs(name_starts_with=startString)
        return [blob.name for blob in blobList]
    #
    # def lastBlob(containerClient, startString):
    #     blobList = containerClient.list_blobs
    #     for blob in containerClient.list_blobs(name_starts_with=startString):
    #         print("Name: ",blob.name, "Creation time: ", blob.creation_time)
    #     return list([(blob.name, blob.creation_time) for blob in containerClient.list_blobs(name_starts_with=startString)])


    # def listBlobs(containerClient, startString):
    #     blob_info = []
    #     for blob in containerClient.list_blobs(name_starts_with=startString):
    #         blob_info.append((blob.name, blob.creation_time))
    #         print("Name:", blob.name, "Creation time:", blob.creation_time)
    #     return blob_info

    # def listBlobs(containerClient, startString):
    #     # for blob in containerClient.list_blobs(name_starts_with=startString):
    #     #     print("Name: ",blob.name, "Creation time: ", blob.creation_time)
    #     return list([(blob.name, blob.creation_time) for blob in containerClient.list_blobs(name_starts_with=startString)])

    def downloadToStream(blobClient, inputBlob):
        blobClient.download_blob().download_to_stream(inputBlob)

    def uploadBlobFile(containerClient, filePath, newBlobName):
        with open(file=filePath, mode="rb") as data:
            containerClient.upload_blob(name=newBlobName, data=data, overwrite=True)

    def uploadDfAsCsvToAzure(containerClient, DataFrame, newBlobName):
        with BytesIO() as input_blob:
            DataFrame.to_csv(input_blob)
            input_blob.seek(0)
            containerClient.upload_blob(name=newBlobName, data=input_blob, overwrite=True)

