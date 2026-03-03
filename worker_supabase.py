import os
import sys


def main() -> None:
    # Make repo root importable
    repo_root = os.path.abspath(os.path.dirname(__file__))
    backend_dir = os.path.join(repo_root, "backend")
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)

    from backend.worker_supabase import main as worker_main  # type: ignore

    worker_main()


if __name__ == "__main__":
    main()
