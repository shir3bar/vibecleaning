# Start Vibecleaning on your Windows VM

This gets release `p0-634edd3afcbb` running with two example animals. You will
open the app in your browser, make a review decision, save it and reopen it.
Allow internet access for the first setup.

**Already set up? Go straight to step 4.**

## 1. Put the two ZIP files on the Windows VM

Save these files in the VM's **Downloads** folder:

- [The app: vibecleaning-p0-634edd3afcbb.zip](../dist/vibecleaning-p0-634edd3afcbb.zip)
- [The example data: vibecleaning-p0-634edd3afcbb-test-data.zip](../dist/vibecleaning-p0-634edd3afcbb-test-data.zip)

Open the Windows Start menu, type **PowerShell**, and open it. Copy and paste
these lines, then press Enter:

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\Vibecleaning" | Out-Null
Expand-Archive "$env:USERPROFILE\Downloads\vibecleaning-p0-634edd3afcbb.zip" "$env:USERPROFILE\Vibecleaning\app"
Expand-Archive "$env:USERPROFILE\Downloads\vibecleaning-p0-634edd3afcbb-test-data.zip" "$env:USERPROFILE\Vibecleaning\data"
```

You now have an **app** folder and a **data** folder inside **Vibecleaning** in
your Windows user folder. The example data is ready to use.

## 2. Install the tool that starts the app

In PowerShell, run:

```powershell
winget install --id=astral-sh.uv -e
```

This installs `uv`, which downloads the software the app needs. If `uv` is
already installed, skip this step. This is the command from the
[official uv installation instructions](https://docs.astral.sh/uv/getting-started/installation/#winget).

**Close PowerShell and open it again** before continuing.

## 3. Prepare the app and create your test login — once

Paste these lines into the new PowerShell window:

```powershell
cd "$env:USERPROFILE\Vibecleaning\app"
$env:UV_PROJECT_ENVIRONMENT = "$env:LOCALAPPDATA\Vibecleaning\venvs\p0-634edd3afcbb"
uv python install 3.11
uv sync --locked --no-dev --python 3.11
uv run --no-sync python -m app.auth_cli --data-root "$env:USERPROFILE\Vibecleaning\data" bootstrap editor --display-name "Test reviewer"
```

Wait for the downloads to finish. When asked, choose a password and enter it
twice. The password characters will not appear while you type. Your username
is **editor**.

If a command prints an error, stop there and copy the error message. If it says
the user registry is already initialized, a login already exists; use that
login instead of repeating account creation.

## 4. Start the app — each time you want to use it

Open PowerShell and paste:

```powershell
cd "$env:USERPROFILE\Vibecleaning\app"
powershell -ExecutionPolicy Bypass -File .\scripts\start-vibecleaning.ps1 -Profile Rds -DataRoot "$env:USERPROFILE\Vibecleaning\data"
```

**Leave this window open.** When it says the server is running, open Chrome or
Edge on the VM and enter this address:

**http://127.0.0.1:8422**

Log in as **editor** using the password you chose. The PowerShell window runs
the app; the browser is where you use it.

## 5. Try one review

1. Choose the study **acceptance**.
2. Select **animal-A** to display its track. An empty map before choosing an
   animal is expected.
3. Open **Review queue**, choose **OK**, and add a short note.
4. Click **Save decision** and wait for the saved message.
5. Click **Generate report**, choose **Per-individual profile report** and
   animal-A, then click **Create analysis**. Open the report link that appears
   and check that it includes your decision and note.
6. Click **Export reviewed RDS ZIP** to download the reviewed data.

The supplied study contains two animals and 24 fixes in total. Your work is
saved in the **data** folder. Keep that whole folder when moving or backing up
your work, including its hidden folders.

## 6. Stop and reopen

After the saved message appears, go to PowerShell and press **Ctrl+C**. Close
the browser tab. Next time, repeat **step 4**, log in and check your saved note.
You do not need to extract the ZIPs, reinstall or create the account again.

To try the CSV version, stop the RDS app, then run:

```powershell
cd "$env:USERPROFILE\Vibecleaning\app"
powershell -ExecutionPolicy Bypass -File .\scripts\start-vibecleaning.ps1 -Profile Csv -DataRoot "$env:USERPROFILE\Vibecleaning\data"
```

Open **http://127.0.0.1:8421**. Use the same login and choose **acceptance**.
CSV and RDS have separate example studies, so your RDS decisions will not
appear in the CSV study. Its export button says **Export reviewed CSV**.

## If you get stuck

- **A ZIP cannot be found:** check that both ZIPs are in the VM's Downloads
  folder and have the exact names in step 1.
- **`winget` is not recognized:** use the Windows installer in the
  [official uv instructions](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer),
  then reopen PowerShell.
- **The browser cannot open the page:** check that PowerShell is still running
  the app, and that the browser is on the VM. Use the address for the version
  you started: RDS ends in **8422**, CSV in **8421**.
- **Another error:** copy the error text and say which step failed. Keep the
  data folder so your saved work is available when we fix it.

For more about reviewing tracks, see the [review manual](movement-review-manual.md).
Shared-drive setup and deployment testing are covered separately in the
[IT reference](IT_DEPLOYMENT_REQUIREMENTS.md) and
[acceptance checklist](P0_ACCEPTANCE.md).
