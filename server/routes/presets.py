from fastapi import APIRouter
from server.models import PresetInfo
from server.presets import list_presets

router = APIRouter()


@router.get("/api/presets", response_model=list[PresetInfo])
async def presets():
    return list_presets()
