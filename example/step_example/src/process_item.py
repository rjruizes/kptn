def process_item(item: str, idx: int | None = None) -> None:
    """Simple mapped task that prints the incoming item (and index if present)."""
    prefix = f"[{idx}] " if idx is not None else ""
    print(f"{prefix}Processing {item}")
