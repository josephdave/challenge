

from ctypes import Structure
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from jose import JWTError, jwt
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pyspark.sql.types import StructType


router = APIRouter(prefix="", tags=["auth"])

basedir = Path(__file__).resolve().parent
env_path = basedir / '.env'
load_dotenv(dotenv_path=env_path)


print("sys.executable ->", sys.executable)

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "30"))
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

if not (SECRET_KEY and USER and PASSWORD):
    raise RuntimeError("SECRET_KEY, USER, and PASSWORD must be set in .env")





# -----------------------------------------------------------------------------
# JWT & OAuth2 setup
# -----------------------------------------------------------------------------
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def create_access_token(sub: str, expires_delta: timedelta):
    to_encode = {"sub": sub, "exp": datetime.utcnow() + expires_delta}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user = payload.get("sub")
        if user != USER:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return user

# -----------------------------------------------------------------------------
# token endpoint
# -----------------------------------------------------------------------------
@router.post("/token")
async def login(form: OAuth2PasswordRequestForm = Depends()):
    if form.username != USER or form.password != PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(
        sub=USER,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": token, "token_type": "bearer"}


# -----------------------------------------------------------------------------