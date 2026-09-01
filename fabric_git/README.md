# fabric_git/

Dedicated target directory for Fabric's own Git integration sync format
(`<name>.<ItemType>/notebook-content.py` + `.platform`, etc.), kept separate
from this repo's own `/notebooks`, `/infra`, `/pipelines`, `/warehouse`
layout so the two mechanisms don't collide. This file exists only so the
directory isn't empty before the first Fabric commit (git doesn't track
empty directories). See `infra/setup_git_integration.py`.
