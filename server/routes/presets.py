from fastapi import APIRouter
from server.models import PresetInfo
from server.presets import list_presets
from server.utils.error_handler import safe_route

router = APIRouter()


@router.get("/api/presets", response_model=list[PresetInfo])
@safe_route(default_response=[])
async def presets():
    return list_presets()
