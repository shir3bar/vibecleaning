# Mac: install and open Scrub Data

These steps open the RDS app with the **two MoveTraits studies included in Git**.
Use **Terminal**. Run each block in order; if it fails, stop and copy the error.

**Already installed and have a login? Go to step 4.**

## 1. Install Git and uv — once

If Git is not installed, run this and finish the installation window:

```sh
xcode-select --install
```

If uv is not installed, run:

```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Close Terminal and open it again. Official instructions:
[Git](https://git-scm.com/install/mac),
[uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer).

## 2. Get the app — once

```sh
git clone --branch rds_input https://github.com/shir3bar/vibecleaning.git "$HOME/Vibecleaning"
cd "$HOME/Vibecleaning"
```

**Already cloned?** Open Terminal in that folder, run `git switch rds_input`
and `git pull --ff-only`, then continue below. Use that folder's path in the
`cd` command in step 4 too.

## 3. Install dependencies and create your login — once

Run from the app folder:

```sh
uv sync --locked --no-dev
uv run --no-sync python -m app.auth_cli --data-root "./data" bootstrap editor --display-name "Reviewer"
```

`uv` installs Python and the app's dependencies. Enter your chosen password twice;
typing is invisible. Your username is **editor**. If it says the user registry is
already initialized, use your existing login and continue to step 4.

## 4. Start the app — every time

```sh
cd "$HOME/Vibecleaning"
bash scripts/start-vibecleaning.sh --profile Rds --data-root "./data"
```

Leave Terminal open. Open **http://127.0.0.1:8422** in Chrome or Edge on this
computer and log in. The launcher finds the Python environment each time.

Choose **268904527** for the smaller study or **481458** for Bildstein, then select
an individual. The first load of the larger study can take several minutes;
later loads are faster.

**Stop:** save your work, then press **Ctrl+C** in Terminal. **Reopen:** repeat
step 4. Reviews are saved in `data`; back up that whole folder, including hidden
folders.

**Update:** stop the app, run `git pull --ff-only` in the app folder, then repeat
step 4.

Shared folders and team accounts: [IT guide](IT_DEPLOYMENT_REQUIREMENTS.md).
App controls: [review manual](movement-review-manual.md).

**Upgrading an existing installation?** Stop all old app instances first. Saved
accounts and studies are converted automatically to `scrubdata/`, with the old
history retained as a backup. See [upgrade and rollback](SCRUBDATA_MIGRATION.md).
