from fastapi import Request

from .clients import Upstream


def get_upstream(request: Request) -> Upstream:
    return request.app.state.upstream
