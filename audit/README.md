# Audit reports

This folder holds five reports chosen from the project's review record. They show how the results were
checked before the paper was finished. The project produced many more review files; only these five are
published.

The reports are kept as written, with one change: three local folder paths were replaced with
`<reviewer scratch folder, not published>`. A few other things to know when reading them:

- They quote commit IDs from before this repository was published. `docs/COMMIT_MAP.md` lists the
  matching published IDs.
- Paths such as `repo/`, `repo-prereg2/` and `analysis_root/main/` name the author's local working
  folders. In this repository the code sits at the top level, and the raw run folders are not included.
- They use the short experiment names from the configs, such as M1 (`configs/main_M1.yaml`) and M3
  (`configs/main_M3.yaml`).

| File | What it is |
|---|---|
| `SKEPTIC_MAIN.md` | A second check of the main results, done by a reviewer told to look for mistakes. It recomputes the numbers from the raw run files with its own code, looks for bugs, and says which claims hold and which need a caveat. The reviewer named in the file is Claude. |
| `M3_DESIGN_CHECK.md` | A short check done after the eight-segment runs (M3) finished and before their results were read. It counts how many shards received new vectors in each time step, to confirm that the refresh rules had a real choice to make. The counting script it names is not included. |
| `SKEPTIC_M3.md` | The same kind of second check for the runs added by the second addendum to the plan (M3 and the online extra search). The reviewer named in the file is Claude. |
| `SKEPTIC_ADD3.md` | The same kind of second check for the exploratory Addendum 3 runs, where the data-change refresh rule pays for its score messages out of the byte budget. The reviewer named in the file is Claude. |
| `FINAL_AUDIT_R10.md` | A last read of the whole paper by a reviewer who had not seen the earlier reviews. It lists problems from most to least serious and checks numbers in the paper against the result tables. The paper was edited after this audit in response to its findings, so the text and line numbers it quotes may not match `paper/`. |
