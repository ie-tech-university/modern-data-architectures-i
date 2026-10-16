# Modern Data Architectures for Big Data I
## Lecture 2.2 — How to Be a Big Data Hacker: Linux Commands

*Updated edition, October 2026 · Practice notebook: `linux-in-jupyter-practice.ipynb`*

---

## Agenda

1. Why Linux? · 2. The terminal and the shell · 3. The file system · 4. Moving around · 5. Files and folders · 6. Looking inside files · 7. Pipes, search and redirection · 8. Permissions · 9. Processes, disk and environment · 10. Downloads and archives · 11. Linux inside Jupyter (`!`) · 12. From Linux to big data

---

## 1. Why Linux?

- Created by **Linus Torvalds** in 1991; free and open source
- Runs most **cloud servers**, every one of the **world's 500 fastest supercomputers**, and Android phones
- **Hadoop, Spark, Kafka, NiFi**… are built and deployed on Linux; **OSBDET** runs Debian 13
- Big data clusters have **no mouse and no windows**: you drive them by typing commands

**Distributions** = kernel + tools + package manager: Debian (OSBDET), Ubuntu (laptops, cloud), RHEL/Fedora (enterprises), Alpine (containers).

### How Linux is layered

```
You (type commands)
Shell (bash) — interprets them
Programs — ls, grep, python, hdfs, spark-submit…
Linux kernel — CPU, memory, disks, network
Hardware
```

- The **kernel** is Linux itself; the **shell** (usually **bash**; zsh on macOS) runs what you type; the **terminal** is the window
- **Where to practise:** OSBDET Jupyter → *File → New → Terminal*; macOS Terminal; Windows **WSL**

---

## 2. The Terminal and the Shell

### Reading the prompt

```
osbdet@osbdet:~/notebooks$
 user  machine  current folder   $ normal user (# = root)
```

In slides, `$` marks a command: don't type it.

### Anatomy of a command

```
ls   -l -h   /datalake
command  options  arguments
(what)   (how)    (on what)
```

- Options combine: `ls -l -h` = `ls -lh`; long options: `ls --all`
- Linux is **case-sensitive**: `Movies.csv` ≠ `movies.csv`
- Spaces separate parts: quote names with spaces (`cat "my data.csv"`), or better, avoid spaces

### Survival kit

```bash
$ man ls          # full manual (q to quit)
$ ls --help       # short summary of options
$ whatis grep     # one-line description
```

| Shortcut | Effect |
|---|---|
| `Tab` | autocomplete (twice: list options) |
| `↑` / `↓` | previous / next command |
| `Ctrl+C` | stop the running command |
| `Ctrl+R` | search command history |
| `Ctrl+L` | clear screen (or `clear`) |

---

## 3. The File System

```
/
├── bin     programs (ls, cp…)
├── etc     configuration
├── home
│   └── osbdet        (= ~)
│       └── notebooks (Jupyter's root in OSBDET)
├── opt     extra software (hadoop3, nifi…)
├── tmp     temporary files
└── var     logs, data (var/log)
```

- **One single tree** (no `C:\`); folders separated with `/`
- Your home: `/home/osbdet` = `~`

| Path type | Example |
|---|---|
| **Absolute** (starts at `/`, works anywhere) | `/home/osbdet/notebooks/landing/movies.csv` |
| **Relative** (from where you are) | `landing/movies.csv` |

| Symbol | Means |
|---|---|
| `/` | root |
| `~` | your home |
| `.` | current folder |
| `..` | parent folder |
| `-` | previous folder (`cd -`) |

---

## 4. Moving Around

```bash
$ pwd
/home/osbdet/notebooks/ml-latest-small
$ ls
README.txt  links.csv  movies.csv  ratings.csv  tags.csv
$ ls -lh
-rw-r--r-- 1 osbdet osbdet 483K Sep 26  2018 movies.csv
-rw-r--r-- 1 osbdet osbdet 2.4M Sep 26  2018 ratings.csv
…
```

`-rw-r--r--` permissions · `osbdet` owner · `483K` size · date · name. Options: `-a` hidden files, `-t` newest first, `-R` recursive, `-S` biggest first.

```bash
$ cd /opt/hadoop3      # absolute
$ cd etc/hadoop        # relative
$ cd ..                # up one level
$ cd ../..             # up two levels
$ cd ~                 # home (plain cd too)
$ cd -                 # previous folder
```

---

## 5. Files and Folders

```bash
$ mkdir landing                       # one folder
$ mkdir movies ratings tags           # several
$ mkdir -p landing/movielens/movies   # create parents too, no error if it exists
$ touch notes.txt                     # empty file

$ cp movies.csv backup.csv            # copy file
$ cp movies.csv ratings.csv landing/  # many → folder
$ cp -r landing landing_backup        # whole folder

$ mv notes.txt README.md              # rename
$ mv *.csv landing/                   # move

$ rm backup.csv                       # delete file
$ rm -i movies.csv                    # ask first
$ rm -r landing_backup                # delete folder and contents
$ rmdir empty_folder                  # only if empty
```

> **No recycle bin**, and `cp`/`mv` overwrite silently (use `-i` to be asked). Run `ls` with the same pattern before `rm`. Never run `rm -rf /` or `sudo rm -rf …` copied from the internet.

### Wildcards

| Pattern | Matches | Example |
|---|---|---|
| `*` | anything | `*.csv` |
| `?` | exactly one character | `part-0000?` |
| `[abc]` | one of these characters | `[mr]*.csv` → movies.csv, ratings.csv |
| `{a,b}` | each alternative | `mkdir -p landing/movielens/{movies,ratings,tags,links}` |

---

## 6. Looking Inside Files

```bash
$ head -n 3 movies.csv          # first 3 lines (default 10)
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
2,Jumanji (1995),Adventure|Children|Fantasy
$ tail -n 2 movies.csv          # last 2 lines
193587,Bungo Stray Dogs: Dead Apple (2018),Action|Animation
193609,Andrew Dice Clay: Dice Rules (1991),Comedy
$ wc -l movies.csv ratings.csv
   9743 movies.csv
 100837 ratings.csv
```

- **cat** prints a whole file (or joins files: `cat a.csv b.csv > all.csv`)
- **head** = first N lines; **tail** = last N lines; `tail -f app.log` follows a log live
- **less** pages through big files (`/word` search, `q` quit)
- **Big data tip:** never `cat` a 10 GB file; `head` it

---

## 7. Pipes, Search and Redirection

```bash
$ grep -i "toy story" movies.csv         # -i ignore case
1,Toy Story (1995),…
3114,Toy Story 2 (1999),…
78499,Toy Story 3 (2010),…
$ grep -c "Comedy" movies.csv            # count matching lines
3756
$ find ~/notebooks -name "*.csv"         # find files by name
$ find . -type f -size +1M               # files over 1 MB
```

### Pipes: `|`

```bash
$ tail -n +2 movies.csv | tr -d '\r' | awk -F, '{print $NF}' \
    | tr '|' '\n' | sort | uniq -c | sort -nr | head -5
   4361 Drama
   3756 Comedy
   1894 Thriller
   1828 Action
   1596 Romance
```

skip header → drop Windows line ends → keep last column → one genre per line → group → count → biggest first → top 5. This is **map → sort → reduce** on one machine, with the same numbers as the Hadoop and Spark labs.

### Redirection

```bash
$ grep "Comedy" movies.csv > comedies.csv   # create / overwrite
$ echo "run finished" >> log.txt            # append
$ sort < names.txt                          # read input from a file
$ python job.py 2> errors.txt               # errors only
$ python job.py > out.txt 2>&1              # output + errors
```

Every program has **stdin (0)**, **stdout (1)** and **stderr (2)**.

---

## 8. Permissions

```
-  rwx  r-x  r--
│  │    │    └ others: read            = 4
│  │    └ group: read + execute        = 5
│  └ owner: read + write + execute     = 7
└ type: - file, d folder
```

| Letter | File | Folder | Value |
|---|---|---|---|
| r | read | list contents | 4 |
| w | modify | create / delete inside | 2 |
| x | run as program | enter (cd) | 1 |

```bash
$ chmod +x mapper.py        # make executable
$ chmod 754 run.sh          # rwxr-xr--
$ chmod 600 secrets.env     # only me
$ sudo apt install htop     # run as administrator
```

---

## 9. Processes, Disk and Environment

```bash
$ ps aux | grep java        # running processes (NiFi, Kafka… are Java)
$ top                       # live view (q to quit)
$ kill 1234                 # ask process 1234 to stop; kill -9 forces it
$ python long_job.py &      # run in background
$ df -h /                   # free disk space
$ du -sh ~/notebooks/*      # size of each folder
$ free -h                   # memory
```

### Environment variables

```bash
$ echo $HOME
/home/osbdet
$ echo $HADOOP_HOME
/opt/hadoop3
$ which hdfs
/opt/hadoop3/bin/hdfs
$ export DATA=~/notebooks/ml-latest-small
$ ls $DATA
```

**PATH** lists the folders where the shell looks for commands. Make variables permanent in `~/.bashrc`.

---

## 10. Downloads and Archives

```bash
$ curl -O https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
$ unzip ml-latest-small.zip
$ curl -s https://api.tfl.gov.uk/BikePoint -o bikepoint.json   # an API
$ wget https://example.com/data.csv
$ tar -czf movielens.tar.gz ml-latest-small/                   # create archive
$ tar -xzf movielens.tar.gz                                    # extract
```

`ssh user@server` opens a terminal on a remote machine; `scp` copies files to it.

---

## 11. Linux Inside Jupyter

**`!` runs a shell command from a notebook cell.** The kernel hands the line to a new shell and prints its output under the cell.

```python
!pwd
!ls -lh ml-latest-small
!head -n 3 ml-latest-small/movies.csv
```

### Mixing Python and shell

```python
files = !ls ml-latest-small/*.csv      # shell output → Python list
len(files), files[0]                   # (4, 'ml-latest-small/links.csv')

dataset = "ratings"
!wc -l ml-latest-small/{dataset}.csv   # Python value → shell
# 100837 ml-latest-small/ratings.csv

n = !wc -l < ml-latest-small/movies.csv
print(int(n[0]) - 1, "movies")         # 9742 movies

!hdfs dfs -ls /datalake/raw/movielens  # big data commands too
```

### The `!cd` trap, `%cd` and `%%bash`

```python
!cd ml-latest-small
!pwd                   # /home/osbdet/notebooks  ← nothing changed!
%cd ml-latest-small    # /home/osbdet/notebooks/ml-latest-small
```

Each `!` runs in a **new shell that dies afterwards**, so `!cd` is forgotten; the magic **`%cd`** changes the notebook's folder.

```bash
%%bash
cd /tmp
mkdir -p demo && cd demo
echo "hello" > hi.txt
pwd; ls
```

`%%bash` on the **first line** runs the whole cell as one bash script. Databricks uses `%sh`; Google Colab supports `!` as here.

---

## 12. From Linux to Big Data

| Linux (your disk) | HDFS (the data lake) | What it does |
|---|---|---|
| `ls -l /data` | `hdfs dfs -ls /datalake` | list |
| `mkdir -p a/b` | `hdfs dfs -mkdir -p /a/b` | create folders |
| `cat f \| head` | `hdfs dfs -cat /f \| head` | read |
| `cp` (into the lake) | `hdfs dfs -put movies.csv /datalake/raw/` | upload |
| `cp` (out of the lake) | `hdfs dfs -get /jobs/movies-genre .` | download |
| `rm -r` | `hdfs dfs -rm -r /jobs/movies-genre` | delete |
| `du -sh` | `hdfs dfs -du -h /datalake` | space used |

Same idea for object storage: `aws s3 ls`, `aws s3 cp`, MinIO's `mc ls`, `mc cp`.

### Practice: build a landing zone by hand

```bash
$ cd ~/notebooks
$ curl -sO https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
$ unzip -q ml-latest-small.zip
$ mkdir -p landing/movielens/{movies,ratings,tags,links}
$ for d in movies ratings tags links; do
>   cp ml-latest-small/$d.csv landing/movielens/$d/
> done
$ find landing -type f
$ du -sh landing
3.2M  landing
```

That's the landing zone NiFi picks up in lab 8.1.

---

## Cheat Sheet

| Task | Commands |
|---|---|
| Move | `pwd` · `ls -lah` · `cd dir` · `cd ..` · `cd ~` |
| Create | `mkdir -p a/b` · `touch f` |
| Copy / move | `cp f g` · `cp -r d e` · `mv f g` |
| Delete | `rm f` · `rm -r d` · `rm -i f` |
| Read | `cat` · `head -n` · `tail -n` · `tail -f` · `less` · `wc -l` |
| Search | `grep -i word f` · `find . -name "*.csv"` |
| Combine | `a \| b` · `> file` · `>> file` · `2> err` |
| Permissions | `chmod +x f` · `chmod 644 f` · `sudo cmd` |
| System | `ps aux` · `top` · `kill PID` · `df -h` · `du -sh` · `free -h` |
| Data in | `curl -O url` · `wget url` · `unzip` · `tar -xzf` |
| Jupyter | `!cmd` · `x = !cmd` · `{var}` · `%cd` · `%%bash` |
| Help | `man cmd` · `cmd --help` · `Tab` · `Ctrl+C` · `Ctrl+R` |

---

<details>
<summary><strong>Instructor notes — what changed from the original deck</strong></summary>

- **Kept:** ls, mkdir, rm, cd, cp, mv, cat, head, tail, with their syntax and warnings.
- **Corrected:** the original said `head` prints the *last* N lines; it prints the **first** N (default 10).
- **Added:** why Linux, distributions, the layered architecture, prompt and command anatomy, help and shortcuts, the file system tree, absolute/relative paths, touch/rmdir, wildcards, wc/less/tail -f, grep/find, pipes and redirection (with a real MovieLens pipeline matching the MapReduce lab results), permissions, processes/disk/memory, environment variables, curl/wget/unzip/tar/ssh, **Linux in Jupyter** (`!`, capturing output, `{var}`, the `!cd` trap, `%cd`, `%%bash`), HDFS equivalents and a landing-zone exercise.
- **Practice notebook** `linux-in-jupyter-practice.ipynb`: executed end to end; all outputs shown are real.
- **Logos and the Tux mascot** are not reproduced.

</details>
