import shutil
from fastapi import BackgroundTasks, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, UploadFile, File, Form
import thumbnail as thumbnail_mod
THUMB_UPLOAD_DIR = Path(r"C:\Users\muham\AppData\Local\Temp\opencode\thumbnail-uploads")
