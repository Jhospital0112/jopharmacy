# Narcotic System - Windows EXE Build

## What this repository contains
This project wraps your existing web frontend in Electron and builds two Windows outputs:
- Portable EXE
- Setup Installer EXE

## Files added
- `main.js`
- `package.json`
- `.github/workflows/build-exe.yml`
- `README_BUILD.md`

## How to get the EXE from GitHub
1. Upload all files in this folder to your GitHub repository.
2. Open the repository on GitHub.
3. Go to **Actions**.
4. Open **Build Windows EXE**.
5. Click **Run workflow** if needed.
6. Wait until the workflow finishes successfully.
7. Download the artifacts:
   - `Narcotic-System-Portable`
   - `Narcotic-System-Setup`

## Notes
- Your backend URL is read from `config.js`.
- The app loads `index.html` locally inside Electron.
- External links are opened in the default browser.
