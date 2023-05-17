




Feats

- [ ] Add anki input node
- [ ] Add cache system
  - [x] First implementation
  - [ ] `cache_info` has granularity (meaning, a `data` field per day/time-unit)
  - [ ] Graph or value is gathered from context in case it exists, kinda like `_get_value_from_context_or_run` or `_get_value_from_context_or_makegraph`
  - [ ] Add tests to cache system 😣
  - [ ] Configurable cache folder (maybe useful if user wants sharable caches?)

- [ ] Stats module to see what info is available...?
  - [ ] Info available
  - [ ] How much broken information per datasource?

- [ ] Internal pandas for Segments/Seg
  - [ ] First draft

- [ ] Run has `skip_cache` argument



Refactor

- [ ] Does `class Config:` really make sense?
- [ ] Current `prefect_task(name=self.__class__.__name__)` causes name conflicts, can we add a dynamic hash or anything of the sort?
- [ ] Rename `Node_anki.py` to `Reader_anki.py`? Like, bruh, we know it's gonna be nodes n such



Tests

- [x] Increase coverage in `Seg`

- [ ] Automatic tests for all the nodes?
  - [ ] All children are lists
  - [ ] All nodes have valid _hash functions that are distinct from themselves?
  - [ ] All the public (and maybe private) members of Node instances have typing on their arguments...?



Workflow

- [ ] Try https://github.com/nektos/act



Doubts

- [ ] Is the current approach to auto-update the package vesion a good idea?
- [ ] Should I remove `from __future__ import annotations` and deprecate 3.8?
- [ ] Should I include pydantic for `Segments` init?
- [ ] Also, always asserts for stuff like `def __getitem__(self, index: Time_interval) -> Segments:`, or pydantic or... when should I be strict with the parameter parsing?
- [ ] Should I do `from hashlib import md5` instead of `import hashlib;hashlib.md5()` ?
- [ ] Should I add https://github.com/dbrgn/coverage-badge?
- [ ] Publish on pipy?
- [ ] How to add custom color scheme to TODO extension VSCode




