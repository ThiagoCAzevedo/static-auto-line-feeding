from fastapi import APIRouter, File, UploadFile
from common.logger import logger
from modules.files.infrastructure.service import ListExcelFiles, UploadFiles, DeleteFiles
from common.response_handler import ResponseHandler


router = APIRouter()
log = logger("files")


@router.get("/list", summary="Get files in excel folder")
def list_files():
    log.info("GET /files/list")

    try:
        service = ListExcelFiles()
        files = service.execute()

        return ResponseHandler.success(
            data=files,
            message=f"Listed {len(files)} files from excel directory"
        )

    except Exception as e:
        log.error(f"Failed to list files: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))


@router.post("/upload", summary="Upload file in excel folder")
def upload_files(file: UploadFile = File(...)):
    log.info(f"POST /files/upload (file: {file.filename})")

    try:
        service = UploadFiles()
        result = service.execute(file)

        return ResponseHandler.success(
            data=result,
            message=f"File uploaded successfully: {file.filename}"
        )

    except Exception as e:
        log.error(f"Failed to upload file {file.filename}: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))


@router.delete("/delete/{filename}", summary="Delete file in excel folder")
def delete_files(filename: str):
    log.info(f"DELETE /files/delete/{filename}")

    try:
        service = DeleteFiles()
        result = service.execute(filename)

        return ResponseHandler.success(
            data=result,
            message=f"File deleted successfully: {filename}"
        )

    except Exception as e:
        log.error(f"Failed to delete file {filename}: {str(e)}", exc_info=True)
        return ResponseHandler.error(str(e))