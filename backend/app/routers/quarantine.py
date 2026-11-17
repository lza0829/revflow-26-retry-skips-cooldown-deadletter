from fastapi import APIRouter, HTTPException, Query

from app.modules import quarantine_retry

router = APIRouter(tags=["未平隔离"])


@router.get("/quarantine")
def get_quarantine(
    state: str | None = Query(None, description="冷却中|可重试|死信|已解除"),
    include_released: bool = Query(False, description="是否包含已解除项"),
):
    return quarantine_retry.list_items(state_filter=state, include_released=include_released)


@router.get("/quarantine/retry-report")
def get_retry_report():
    return quarantine_retry.report()


@router.post("/quarantine/retry-due")
def post_retry_due(limit: int | None = Query(None, ge=1)):
    return quarantine_retry.retry_due(limit=limit)


@router.get("/quarantine/{item_id}/attempts")
def get_attempts(item_id: int):
    return quarantine_retry.list_attempts(item_id)


@router.post("/quarantine/{item_id}/retry")
def post_retry(item_id: int):
    try:
        return quarantine_retry.retry_item(item_id)
    except HTTPException:
        raise
    except Exception as exc:  # 事务失败已回滚
        raise HTTPException(500, f"重试事务失败：{exc}") from exc
