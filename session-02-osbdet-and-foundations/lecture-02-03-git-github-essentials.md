# Modern Data Architectures for Big Data I
## Git & GitHub Essentials

*Clone · commit · push · branch · collaborate · October 2026*

---

## Agenda

1. Why version control? · 2. Core concepts · 3. The three areas · 4. Setup (once) · 5. Your first repository · 6. `.gitignore` · 7. GitHub: remotes, push, clone, pull · 8. Branches and merging · 9. Pull requests · 10. Undoing mistakes · 11. Merge conflicts · 12. Git in notebooks + cheat sheet

---

## 1. Why Version Control?

**Without Git:** `analysis.py`, `analysis_v2.py`, `analysis_v2_fixed.py`, `analysis_final.py`, `analysis_final_ANA.py`, `analysis_REALLY_final.py`… Who changed what? Which one works?

**With Git:** one file, full history:

- **History**: what changed, when, by whom, and why
- **Safety**: go back to any previous version
- **Collaboration**: many people, same project, same time
- **Distributed**: everyone has a full copy of the history

| | Git | GitHub |
|---|---|---|
| What | A **distributed version control system** on your computer | The largest **hosting service** for Git repositories (owned by Microsoft) |
| Origin | Created by Linus Torvalds in 2005 for the Linux kernel | Web UI, pull requests, issues, Actions, Pages |
| How you use it | `git …` in a terminal, works offline | In the browser; also your **professional portfolio** |

Alternatives to GitHub: GitLab, Bitbucket, Azure Repos.

---

## 2. Core Concepts

### A project is a chain of commits

```
24b9c5d ← be558ad ← 2064931   (main, HEAD)
Add README   Load      Print number
and script   movies    of movies
```

- A **snapshot** records the state of all tracked files at one moment
- A **commit** creates a snapshot: unique **hash**, **author**, **date**, **message**, link to its **parent**
- **HEAD** = where you are now; the first 7 characters of a hash are enough

### Repositories: local and remote

A **repository** = all files + their whole history (in a hidden `.git` folder).

| Operation | What it does |
|---|---|
| `git clone` | First full copy of a remote repository to your machine |
| `git push` | Upload your local commits to the remote |
| `git pull` | Download new commits from the remote |

The remote on GitHub is conventionally called **origin**.

---

## 3. The Three Areas

```
Working directory ──git add──► Staging area ──git commit──► Local repository ──git push──► GitHub
 (your files)                  (next commit)                (.git history)                (origin)
        ◄──────────────────────────── git pull ────────────────────────────────────────────
```

Why a staging area? You choose **which** changes go together in one commit. *Analogy: staging = putting things in the box; commit = sealing and labelling it; push = shipping it.*

---

## 4. Setup (Once)

- **macOS**: `xcode-select --install` or Homebrew · **Windows**: Git for Windows (Git Bash) · **Linux/OSBDET**: `sudo apt install git` if missing
- Create a **GitHub account** with a professional username

```bash
$ git --version
git version 2.43.0
$ git config --global user.name  "Ana García"
$ git config --global user.email "ana@student.ie.edu"   # your GitHub email
$ git config --global init.defaultBranch main            # GitHub's default branch name
$ git config --list --global
```

### Authenticating with GitHub

> **Your GitHub password doesn't work in the terminal.** Since 2021, GitHub requires a token or an SSH key for Git operations.

| Option | How |
|---|---|
| **A · GitHub CLI** (easiest) | Install `gh`, run `gh auth login`, follow the browser login |
| **B · Personal access token** (HTTPS) | GitHub → Settings → Developer settings → **Fine-grained tokens**; paste it when Git asks for a password. Set an expiry |
| **C · SSH key** | `ssh-keygen -t ed25519 -C "you@ie.edu"`, add `~/.ssh/id_ed25519.pub` in GitHub → Settings → SSH keys; use `git@github.com:…` URLs |

---

## 5. Your First Repository

```bash
$ mkdir movielens-analysis && cd movielens-analysis
$ git init
Initialized empty Git repository in …/movielens-analysis/.git/
$ echo "# MovieLens analysis" > README.md
$ echo "import pandas as pd" > analysis.py
$ git status
On branch main

No commits yet

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	README.md
	analysis.py
```

File states: **untracked** → (add) → **staged** → (commit) → **committed** → (edit) → **modified**.

```bash
$ git add README.md analysis.py      # or: git add .
$ git status
Changes to be committed:
	new file:   README.md
	new file:   analysis.py
$ git commit -m "Add README and analysis script"
[main (root-commit) 24b9c5d] Add README and analysis script
 2 files changed, 2 insertions(+)
```

**Good commit messages:** imperative, short (≤ 50 characters), say *what* and ideally *why*: ✓ `Add genre count to analysis` · ✓ `Fix CSV parsing for quoted titles` · ✗ `update` · ✗ `asdf` · ✗ `final changes!!!`

```bash
$ git status --short
 M analysis.py
$ git diff                          # unstaged changes (git diff --staged: staged ones)
@@ -1 +1,3 @@
 import pandas as pd
+movies = pd.read_csv("movies.csv")
+print(movies.head())
$ git commit -am "Load movies.csv and preview it"   # -a: stage tracked files
$ git log --oneline
be558ad Load movies.csv and preview it
24b9c5d Add README and analysis script
```

---

## 6. `.gitignore`

```gitignore
# Python
.venv/
__pycache__/
*.pyc
.ipynb_checkpoints/

# Data (too big, or private)
data/
*.csv
*.parquet

# Secrets!
.env
*.pem

# OS and editors
.DS_Store
.vscode/
```

- **Secrets:** once pushed, a password or key is public: rotate it immediately (it stays in history)
- **Data:** Git is for code; data lives in the data lake. GitHub rejects files over 100 MB
- Templates: github.com/github/gitignore; `uv init` also creates one
- Already tracked? `git rm --cached file`, then commit

---

## 7. GitHub

### Publish an existing repo: remote + push

1. On GitHub: **New repository** → `movielens-analysis` (don't add a README; you have one)
2. Copy its URL
3. Connect and push:

```bash
$ git remote add origin https://github.com/ana/movielens-analysis.git
$ git remote -v
origin  https://github.com/ana/movielens-analysis.git (fetch)
origin  https://github.com/ana/movielens-analysis.git (push)
$ git push -u origin main
 * [new branch]      main -> main
branch 'main' set up to track 'origin/main'.
$ git push        # from now on
```

### git clone

```bash
$ cd ~/notebooks
$ git clone https://github.com/raulmarinperez/osbdet.git
Cloning into 'osbdet'...
$ cd osbdet
$ git log --oneline -3      # the whole history came too
$ git remote -v             # origin is set for you
```

Find the URL with the green **Code** button. Public repos clone without login; pushing needs permission. **Clone, don't download ZIP**: the ZIP has no history and can't pull updates.

### git pull and fetch

```bash
$ git pull                  # fetch + merge into your branch
Updating be558ad..2064931
Fast-forward
$ git fetch                 # download only
$ git status
Your branch is behind 'origin/main' by 2 commits, and can be fast-forwarded.
```

**Daily routine:** `git pull` → edit, run, test → `git status` → `git add …` → `git commit -m "…"` → `git push`. Pull before you start and before you push.

---

## 8. Branches and Merging

```
main:            24b9c5d ── be558ad ── 2064931 ──────── 4609635 (merge)
                                 \                     /
feature/genres:                   └── f3e810e ────────┘
                                      Count movies by genre
```

```bash
$ git switch -c feature/genres     # create + move to it
$ git commit -am "Count movies by genre"
$ git branch
* feature/genres
  main
$ git switch main
$ git merge feature/genres
$ git log --oneline --graph --all
*   4609635 Merge feature/genres
|\
| * f3e810e Count movies by genre
* | 2064931 Print number of movies
|/
* be558ad Load movies.csv and preview it
$ git branch -d feature/genres
```

- A **branch** is a movable label pointing to a commit; **main** holds the stable version
- `git switch` (Git 2.23+) is the clearer replacement for `git checkout <branch>`
- Name branches by purpose: `feature/…`, `fix/…`, `exp/…`; push one with `git push -u origin feature/genres`

---

## 9. Pull Requests: The GitHub Flow

1. **Branch** — `git switch -c fix/csv`
2. **Commit** — small, clear commits
3. **Push** — `git push -u origin fix/csv`
4. **Pull request** — "Compare & pull request" on GitHub
5. **Review** — comments, changes, automatic checks (GitHub Actions)
6. **Merge** — into main, delete the branch

A **pull request (PR)** asks: "please merge my branch into main". No push access? **Fork** the repo (your own copy on GitHub) and open the PR from your fork.

---

## 10. Undoing Mistakes

| Situation | Command | Safe after push? |
|---|---|---|
| Discard my changes to a file (not staged) | `git restore analysis.py` | — |
| Unstage a file (keep the changes) | `git restore --staged analysis.py` | — |
| Fix the last commit's message or add a forgotten file | `git commit --amend` | no |
| Undo the last commit but keep the changes | `git reset --soft HEAD~1` | no |
| Undo a commit that's already on GitHub | `git revert <hash>` | yes |
| See an old version without changing anything | `git switch --detach <hash>` | yes |

> **Golden rule:** never rewrite history others already have (amend, reset, force push on shared branches). Use `git revert`: it adds a new commit that undoes the old one.

---

## 11. Merge Conflicts

```bash
$ git merge feature/genres
Auto-merging analysis.py
CONFLICT (content): Merge conflict in analysis.py
Automatic merge failed; fix conflicts and then commit the result.
$ cat analysis.py
import pandas as pd
movies = pd.read_csv("movies.csv")
print(movies.head())
<<<<<<< HEAD
print(len(movies), "movies")
=======
genres = movies["genres"].str.split("|").explode().value_counts()
>>>>>>> feature/genres
```

1. **Open the file**: between `<<<<<<<` and `=======` is your version (HEAD); below, the other branch
2. **Edit to the final version**: keep one, the other or both; delete the markers
3. **Mark as resolved and commit**: `git add analysis.py` · `git commit`

VS Code and GitHub offer *Accept current · Accept incoming · Accept both*. Changed your mind? `git merge --abort`.

---

## 12. Git in Notebooks

```python
!git clone https://github.com/ana/movielens-analysis.git
%cd movielens-analysis
!git status --short
!git add lab_7_1.ipynb
!git commit -m "Add lab 8.1 notebook"
!git push
```

- Same `!` trick as in the Linux lecture; use `%cd`, not `!cd`
- A Jupyter **terminal** is better for interactive prompts (passwords, editors)
- Notebooks are JSON with outputs inside: **clear outputs** before committing (`nbstripout` automates it)
- VS Code's **Source Control** panel does the same with buttons

---

## Cheat Sheet

| Task | Commands |
|---|---|
| Setup | `git config --global user.name / user.email` |
| Start | `git init` · `git clone URL` |
| Look | `git status` · `git diff` · `git log --oneline` |
| Save | `git add file` · `git add .` · `git commit -m "msg"` |
| Share | `git remote add origin URL` · `git push -u origin main` |
| Update | `git pull` · `git fetch` |
| Branch | `git switch -c name` · `git switch main` · `git branch` |
| Combine | `git merge name` · resolve · `git add` · `git commit` |
| Undo | `git restore f` · `git restore --staged f` · `git revert hash` |
| Ignore | `.gitignore` · `git rm --cached file` |
| Auth | `gh auth login` · token · `ssh-keygen -t ed25519` |
| Help | `git help cmd` · `git cmd -h` |

## Practice: Your Course Portfolio Repo

1. **Create** `mda-big-data-labs` on GitHub with a README
2. **Clone** it into `~/notebooks` from OSBDET's terminal
3. **Ignore** `*.csv`, `.ipynb_checkpoints/`, `.env` in a `.gitignore`
4. **Commit** a lab notebook with a clear message
5. **Branch** `git switch -c exp/spark`, change something, commit, push the branch
6. **PR**: open a pull request on GitHub, merge it, then `git switch main && git pull`

---

<details>
<summary><strong>Instructor notes</strong></summary>

- **New deck**, inspired by the Git & GitHub section of the Session I concepts guide (concepts, three stages, essential commands, `.gitignore`, typical workflow) and extended with setup and authentication, clone/pull/fetch, branches with a graph, pull requests and forks, undo commands, merge conflicts and Git in notebooks.
- **Real outputs:** every `git` output shown (init, status, commit, diff, log, graph, merge conflict, push) was produced by running the commands with Git 2.43.
- **Updated practices:** `main` as default branch; `git switch` / `git restore` instead of `checkout`; token, SSH or `gh auth login` (GitHub stopped accepting passwords for Git operations in 2021); GitHub's 100 MB file limit.
- **Logos** (Git, GitHub, Octocat) are not reproduced.

</details>
