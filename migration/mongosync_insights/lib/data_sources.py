"""Build data-source toolbar badge payloads for live monitoring dashboards."""

from typing import Optional

STATUS_ACTIVE = "active"
STATUS_UNAVAILABLE = "unavailable"

_COLOR_BY_STATUS = {
    STATUS_ACTIVE: "green",
    STATUS_UNAVAILABLE: "yellow",
}


def build_data_sources(
    *,
    progress_configured: bool,
    metadata_configured: bool,
    progress_status: Optional[str],
    metadata_status: Optional[str],
    progress_label: str = "Progress API",
    metadata_label: str = "Metadata",
) -> Optional[dict]:
    """Return toolbar badge payload for configured data sources.

    Only configured sources appear as badges. ``progress_status`` / ``metadata_status``
    may be None when that source was not evaluated on the current poll (verifier slices).
    """
    badges = []

    if progress_configured:
        status = progress_status or STATUS_UNAVAILABLE
        badges.append({
            "id": "progress",
            "label": progress_label,
            "status": status,
            "color": _COLOR_BY_STATUS.get(status, "yellow"),
        })

    if metadata_configured:
        status = metadata_status or STATUS_UNAVAILABLE
        badges.append({
            "id": "metadata",
            "label": metadata_label,
            "status": status,
            "color": _COLOR_BY_STATUS.get(status, "yellow"),
        })

    if not badges:
        return None

    if progress_configured and metadata_configured:
        mode = "both"
    elif progress_configured:
        mode = "endpoint"
    elif metadata_configured:
        mode = "metadata"
    else:
        mode = "none"

    return {"mode": mode, "badges": badges}
