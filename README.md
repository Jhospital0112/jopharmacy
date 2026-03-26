# CDMS — Google Sheets + Vercel Ready

This package keeps your current UI/design and switches the backend to **Google Sheets + Google Apps Script**.

## Project structure
- `index.html` — current UI
- `style.css` — current styling
- `app.js` — frontend logic using Apps Script API
- `config.js` — paste your Apps Script Web App URL here
- `config.example.js` — example only
- `vercel.json` — Vercel static deployment config
- `backend/Code.gs` — Google Apps Script backend

## 1) Create the Google Sheet backend
1. Create a new Google Sheet.
2. Open **Extensions → Apps Script**.
3. Delete the default file content.
4. Paste the code from `backend/Code.gs`.
5. Save the project.
6. Run `setupSheets()` one time.
7. Accept permissions.

## 2) Deploy Apps Script
1. Click **Deploy → New deployment**.
2. Type: **Web app**.
3. Execute as: **Me**.
4. Who has access: **Anyone**.
5. Deploy and copy the Web App URL.

## 3) Connect the frontend
1. Open `config.js`.
2. Replace the placeholder value with your Apps Script Web App URL.

## 4) Push to GitHub
1. Create a new GitHub repository.
2. Upload all files from this folder to the repo root.
3. Commit the files.

## 5) Deploy to Vercel
1. Import the GitHub repository into Vercel.
2. Framework preset: **Other**.
3. Root directory: **/**
4. Deploy.

## 6) First login
- Default password: `111111`
- Seed data is created automatically in the Google Sheet on first run.

## Notes
- Best suited for your current scale: around 5 users, ~100 prescriptions/day, 3 pharmacies.
- Frontend keeps polling lightweight to reduce Apps Script load.
- Design was preserved from your current files as much as possible.
