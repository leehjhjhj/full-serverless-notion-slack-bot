from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import enum

class StatusChoice(enum.Enum):
    CONFIRMED = "확정"
    CANCELLED = "취소 및 변경"
    UNDEFINED = "미정"

class NotionPage(BaseModel):
    connection_name: str
    page_id: str
    notion_database_id: str
    status: str
    time: datetime
    meeting_type: str
    meeting_url: str
    name: str