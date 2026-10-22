# Windows: install and open Scrub Data

These steps open the RDS app with the **two MoveTraits studies included in Git**.
Use **PowerShell**. Run each block in order; if it fails, stop and copy the error.

This guide uses the repository's [uv setup](ENVIRONMENT.md). You do not need to
install Python separately, activate a virtual environment or use Conda.

**Already installed and have a login? Go to step 4.**

## 1. Install Git and uv — once

Skip any tool you already installed.

```powershell
winget install --id Git.Git -e --source winget
winget install --id astral-sh.uv -e
```

Close PowerShell and open it again. Official instructions:
[Git](https://git-scm.com/install/windows),
[uv](https://docs.astral.sh/uv/getting-started/installation/#winget).

## 2. Get the app — once

```powershell
git clone --branch rds_input https://github.com/shir3bar/vibecleaning.git "$env:USERPROFILE\Vibecleaning"
cd "$env:USERPROFILE\Vibecleaning"
```

**Already cloned?** Open PowerShell in that folder, run `git switch rds_input`
and `git pull --ff-only`, then continue below. Use that folder's path in the
`cd` command in step 4 too.

## 3. Install dependencies and create your local login — once per data folder

Run from the app folder. In File Explorer, open that folder and type
`powershell` in the address bar to open a terminal there:

```powershell
$env:UV_PROJECT_ENVIRONMENT = "$env:LOCALAPPDATA\Vibecleaning\venvs\development"
uv sync --locked --no-dev
uv run --no-sync python -m app.auth_cli --data-root ".\data" bootstrap editor
```

The first line chooses the Python environment used by the Windows launcher.
`uv` installs Python and the app's dependencies. Enter your chosen password twice;
typing is invisible. Your username is **editor**, or replace it with your own
name in the command. If the registry is already initialized, use your existing
login and continue to step 4. Accounts belong to the selected data folder; a
shared team data folder has its own registry, managed as described in the IT guide.

New accounts use their normalized username (trimmed and lowercase) as the display
name. Optionally add `--display-name "Taylor Reviewer"` to `bootstrap` or `add`
to choose another label. Existing accounts keep their current names.

## 4. Start the app — every time

```powershell
cd "$env:USERPROFILE\Vibecleaning"
powershell -ExecutionPolicy Bypass -File .\scripts\start-vibecleaning.ps1 -Profile Rds -DataRoot ".\data"
```

Leave PowerShell open. Open **http://127.0.0.1:8422** in Chrome or Edge on this
computer and log in. The launcher finds the Python environment each time.

Choose **268904527** for the smaller study or **481458** for Bildstein, then select
an individual. The first load of the larger study can take several minutes;
later loads are faster.

**Export RDS:** files appear in the selected study's `scrubdata\cleaned_files\`
folder with `_cleaned.rds` names. The app shows the full folder path when ready.

**Stop:** save your work, then press **Ctrl+C** in PowerShell. **Reopen:** repeat
step 4. Reviews are saved in `data`; back up that whole folder, including hidden
folders.

**Update:** stop the app, run `git pull --ff-only` in the app folder, then repeat
step 4. The launcher installs any changed locked dependencies automatically.

**CSV review:** use `-Profile Csv` in step 4 and open `http://127.0.0.1:8421`.
CSV studies belong under `data\movement_raw\`; RDS studies under `data\movement_rds\`.

Shared folders and team accounts: [IT guide](IT_DEPLOYMENT_REQUIREMENTS.md).
App controls: [review manual](movement-review-manual.md).

**Upgrading an existing installation?** Stop all old app instances first. Saved
accounts and studies are converted automatically to `scrubdata/`, with the old
history retained as a backup. See [upgrade and rollback](SCRUBDATA_MIGRATION.md).
