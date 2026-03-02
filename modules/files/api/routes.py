from fastapi import APIRouter, File, UploadFile
from common.logger import logger
from modules.files.infrastructure.service import ListExcelFiles, UploadFiles, DeleteFiles


router = APIRouter()
log = logger("files")


@router.get("/list", summary="Get files in excel folder")
def list_files():
    log.info("GET /files/list")

    try:
        service = ListExcelFiles()
        files = service.execute()
        count = len(files)
        log.info(f"Listed {count} files from excel directory")
        return files

    except Exception as e:
        log.error(f"Failed to list files: {str(e)}", exc_info=True)
        raise


@router.post("/upload", summary="Upload file in excel folder")
def upload_files(file: UploadFile = File(...)):
    log.info(f"POST /files/upload (file: {file.filename})")

    try:
        service = UploadFiles()
        result = service.execute(file)
        log.info(f"File uploaded successfully: {file.filename} ({result['size']} bytes)")
        return result

    except Exception as e:
        log.error(f"Failed to upload file {file.filename}: {str(e)}", exc_info=True)
        raise


@router.delete("/delete/{filename}", summary="Delete file in excel folder")
def delete_files(filename: str):

    try:
        service = DeleteFiles()
        result = service.execute(filename)
        log.info(f"File deleted successfully: {filename}")
        return result

    except Exception as e:
        log.error(f"Failed to delete file {filename}: {str(e)}", exc_info=True)
        raise