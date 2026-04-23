from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from news_pipeline import process_news_pipeline


def main() -> None:
    result = process_news_pipeline(max_articles_per_source=20)
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()

