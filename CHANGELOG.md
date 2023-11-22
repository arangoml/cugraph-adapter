## 2.0.0 (2023-11-22)

### Fix

* Setup docker. [Anthony Mahanna]

* README typo. [aMahanna]

### Other

* Housekeeping (#26) [Anthony Mahanna]

  * initial commit

  * new: controller example in README

  * new: `track_adb`, `track_cug`

  * cleanup: progress tracker

  * revive branch with more cleanup

  build workflow will most likely hang due to dead runners

  * temp: use `housekeeping` branch name

  * cleanup notebook

  * cleanup notebook (again)

  * checkpoint

  * checkpoint

  * checkpoint

  * bump

  * remove: imdb dump

  too large for test purposes

  * fix rich color

  * fix tests

  * Update test_adapter.py

  * Update test_adapter.py

  * update tests

  * update notebooks

  * fix arangorestore

  * fix arangorestore (2)

  * fix isort

  * cleanup

  * new: coveralls

  * more cleanup

  * Update README.md

  * bump

  * update notebook

  * cleanup workflows

  * lock python-arango

  * update release action

  * Update README.md

  * Update README.md

  * Update README.md

  * Update README.md

  * migrate to `pyproject.toml`

  * fix lint

  * Update config.yml

  * add `tests` to `mypy`

  * optimize: `__process_adb_vertex`

  * update `prepare_adb_vertex` docstring

* CircleCI setup (#28) [Anthony Mahanna]

  * Add .circleci/config.yml

  * Add .circleci/config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * lock lint packages

  * fix black

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * Update config.yml

  * bump

  * Update conftest.py

  * Update adapter.py

* Temp: docker workaround. [Anthony Mahanna]

* Remove: setup-python `cache` [Anthony Mahanna]

* Attempt fix: #27. [Anthony Mahanna]

  checking if running `pytest` doesn't generate any `import` complaints...

* Cleanup: build.yml & release.yml. [aMahanna]

* Cleanup. [aMahanna]

* Cleanup: `rich` progress. [aMahanna]

* Cleanup: `rich` spinners. [aMahanna]

* Changelog: release 1.1.0 (#25) [github-actions[bot]]

  !gitchangelog

* Optimize collection validation. [aMahanna]


## 1.1.0 (2022-07-22)

### New

* More adapter housekeeping (#20) [Anthony Mahanna]

  * initial commit

  * cleanup

  * new: debug logs on individual node & edge levels

  Is this too much?

  * cleanup

  * new: pass edge weight value to _identify_cugraph_edge controller method

  * new: test_cug_to_adb_invalid_collections case

  * fix: flake8

  * fix: mypy

  * fix: tests

  * update notebook

  * cleanup

  * fix: logger debug

  * remove: overwrite option in test

  * Revert "remove: overwrite option in test"

  This reverts commit ee5e53c167974c314abd245bcd1fd59596cab4f4.

  * new: __insert_adb_docs

  also: replaces the "weight" DataFrame column name with whatever specified in the **edge_attr** parameter

  * cleanup config: tests

  * replace: node/edge level debug statements in favor of `tqdm` progress ba…

  * fix: tqdm as a dependency

  * pragma: no cover

  * Update README.md

  * Update adapter.py

  * fix: black

* Adjust cuGraph to ArangoDB for increased accessibility to python-arango interface (#18) [Anthony Mahanna]

  * #17: initial commit

  * fix: typo

  * Update build.yml

  * disable overwrite

  (the self-hosted runners are using the same arangodb instance)

  * attempt: re-enable overwrite

  * Update README.md

  * Update README.md

  * remove: __validate_attributes()

  Uncessary noise, replaced in favor of proper docstring examples

  * chg: #18 (after redefining scope)

  * @aMahanna new: CodeQL Action v2

  * fix: mypy

  * fix: typo

  * bump: python-arango version

  * cleanup: documentation

* Custom edge attribute  (#15) [Anthony Mahanna]

  * initial commit

  * new: custom edge attribute for cuGraph to ArangoDB

### Fix

* Notebook wget. [aMahanna]

* Typo. [aMahanna]

* Dev.yml. [aMahanna]

* Inconsistent testing in cuGraph to ArangoDB (#22) [Anthony Mahanna]

  * #16: initial commit

  * temp: create 3 databases

  * remove: create_database

  * fix: gh action matrix

  * config cleanup

  * update pytest in actions

  * fix: typo

  * Update setup.cfg

### Other

* Replace `tqdm` with `rich` [aMahanna]

* Remove: duplicate log. [aMahanna]

* Fix cugraph notebook (#19) [Anthony Mahanna]

  * initial commit

  * add fix to output notebook

* Update README.md. [aMahanna]

* Update README.md. [Anthony Mahanna]

* Update README.md. [aMahanna]

* Changelog: release 1.0.0 (#14) [github-actions[bot]]

  !gitchangelog


## 1.0.0 (2022-05-25)

### New

* Cugraph-adapter notebook (#13) [Anthony Mahanna]

  * #6: initial commit

  * minor cleanup

* Cugraph to arangodb (#11) [Anthony Mahanna]

  * minor houeskeeping before notebook

  * fix: docstring

  * new: notebook draft

  * Update README.md

  * Update meta.yaml

  * cleanup: notebook

  * Update README.md

  * replace: underscore with hyphen

  * Update ArangoDB_cuGraph_Adapter.ipynb

  * remove: %%capture

  * revert: notebook

  hijacking branch to further introduce adapter features & housekeeping (notebook will come at a later time)

  * #3: initial commit

  * cleanup

  * Update adapter.py

  * fix: black & mypy

  * fix: typo

  * fix: str instead of int

  * fix: debug statements

  * enable full test run

  * bump coverage

  * fix: flake8

  * pragma no cover

  * cleanup, black

  * fix: mypy

  * fix: address comment

  * new: test case & cleanup

  * fix: typo

  * fix: illegal col name

  * fix: docstring and return value

  * update: build & analyze triggers

  * Update README.md

  * update: start enumerate() at 1

* Import shortcut. [aMahanna]

* Adapter housekeeping (#7) [Anthony Mahanna]

  * #5: initial commit

  * Update build.yml

  * fix: mypy

  * cleanup

  * fix: set default password to empty string

  * Update build.yml

  * temp: try for 3.9

  * new: support for python 3.9

  * new: verbose logging

  * fix: bring back ADBCUG Controller

  * cleanup

  * temp: try for 3.10

  * temp: try for 3.6

  * bump

  * revert temp commits

  * cleanup conda & release.yml

  * Update meta.yml

  * Update meta.yml

  * fix: meta conda

  * Update README.md

  * create: conda dir

### Fix

* Changelog release title. [aMahanna]

* Run coveralls within conda. [aMahanna]

* Pytest parameterization. [aMahanna]

### Other

* Changelog: release 0.0.2.dev (#12) [github-actions[bot]]

  !gitchangelog

* Cleanup. [aMahanna]

* Update release.yml. [aMahanna]

* Update README.md. [aMahanna]

* Update release.yml. [aMahanna]

* Update meta.yaml. [aMahanna]

* Update release.yml. [aMahanna]

* Temp: disable build phase in release.yml. [aMahanna]

* Prep for 0.0.1.dev release. [aMahanna]

* Update README.md. [aMahanna]

* Update README.md. [aMahanna]

* Merge pull request #4 from arangoml/add-cugraph. [maxkernbach]

  Add functionality to export into cuGraph

* Apply build changes to release. [maxkernbach]

* Update conftest.py. [aMahanna]

* Remove: duplicate subdirs in examples/data. [aMahanna]

* Swap: tcp for ssl. [aMahanna]

* Add example data. [max]

* Add arangorestore binary. [max]

* Change adapter name. [max]

* Formatting. [maxkernbach]

* Add newline at end of file. [max]

* Add prepare_arangodb_edge. [max]

* Merge branch 'add-cugraph' of https://github.com/arangoml/cugraph-adapter into add-cugraph. [max]

* Fix formatting. [maxkernbach]

* Controller. [max]

* Remove unused import. [max]

* Run mypy in conda env. [maxkernbach]

* Fix formatting. [maxkernbach]

* Add ADBCUG_Controller. [max]

* Fix typo. [max]

* Merge branch 'add-cugraph' of https://github.com/arangoml/cugraph-adapter into add-cugraph. [max]

* Run latest version of black. [maxkernbach]

* Update build.yml. [maxkernbach]

* Check black version. [maxkernbach]

* Remove unused imports. [max]

* Merge branch 'add-cugraph' of https://github.com/arangoml/cugraph-adapter into add-cugraph. [max]

* Attempt build without "black --color" [maxkernbach]

* Fix formatting. [max]

* Fix directory. [max]

* Add setup.py. [max]

* Add cugraph functionality. [max]

* Update .gitignore. [aMahanna]

* Update meta.yml. [aMahanna]

* #1. [aMahanna]

* #1: laying groundwork. [aMahanna]

* #1: conda build files. [aMahanna]

* Initial commit: #1. [aMahanna]


