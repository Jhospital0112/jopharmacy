/**
 * Pharmacy admin tools for Google Sheets.
 *
 * What it does:
 * 1) Adds a custom menu: Pharmacy Tools
 * 2) Zeroes stock quantities in common inventory sheets
 * 3) Clears transaction/prescription/audit logs while keeping headers
 *
 * Before using:
 * - Open Extensions > Apps Script
 * - Paste this file
 * - Save, reload the sheet, then use the Pharmacy Tools menu
 *
 * You can customize the candidate sheet names below to match your workbook.
 */

const INVENTORY_SHEET_CANDIDATES = [
  'Inventory',
  'inventory',
  'Stock',
  'stock',
  'Main Stock',
  'المخزون',
  'Drug Stock'
];

const TRANSACTION_SHEET_CANDIDATES = [
  'Transactions',
  'transactions',
  'Transaction_Log',
  'transaction_log',
  'Prescriptions',
  'prescriptions',
  'Audit_Log',
  'audit_log',
  'Verification_Log',
  'verification_log',
  'الوصفات',
  'الحركات'
];

const STOCK_HEADER_KEYWORDS = [
  'boxes',
  'units',
  'pills',
  'capsules',
  'tablets',
  'injections',
  'patches',
  'oral drops',
  'suspension',
  'currentstock_boxes',
  'currentstock_pills',
  'available boxes',
  'available units',
  'totalunits',
  'stock boxes',
  'stock units',
  'quantity boxes',
  'quantity units',
  'العلب',
  'الحبات',
  'الوحدات'
];

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Pharmacy Tools')
    .addItem('Zero All Drug Stocks', 'zeroAllDrugStocks')
    .addItem('Clear All Transactions', 'clearAllTransactions')
    .addToUi();
}

function zeroAllDrugStocks() {
  const ui = SpreadsheetApp.getUi();
  const answer = ui.alert(
    'Zero All Drug Stocks',
    'This will set all stock quantity cells to 0 in the detected inventory sheets. Continue?',
    ui.ButtonSet.YES_NO
  );
  if (answer !== ui.Button.YES) return;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = getCandidateSheets_(ss, INVENTORY_SHEET_CANDIDATES);
  if (!sheets.length) {
    ui.alert('No inventory sheet was found. Update INVENTORY_SHEET_CANDIDATES in Apps Script to match your file.');
    return;
  }

  let affectedSheets = 0;
  let affectedCells = 0;

  sheets.forEach(sheet => {
    const stats = zeroStockInSheet_(sheet);
    if (stats.cells > 0) {
      affectedSheets += 1;
      affectedCells += stats.cells;
    }
  });

  ui.alert(`Done. Updated ${affectedSheets} inventory sheet(s) and zeroed ${affectedCells} stock cell(s).`);
}

function clearAllTransactions() {
  const ui = SpreadsheetApp.getUi();
  const answer = ui.alert(
    'Clear All Transactions',
    'This will delete all transaction / prescription / audit rows from the detected log sheets and keep only the header row. Continue?',
    ui.ButtonSet.YES_NO
  );
  if (answer !== ui.Button.YES) return;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = getCandidateSheets_(ss, TRANSACTION_SHEET_CANDIDATES);
  if (!sheets.length) {
    ui.alert('No transaction sheet was found. Update TRANSACTION_SHEET_CANDIDATES in Apps Script to match your file.');
    return;
  }

  let affectedSheets = 0;
  let deletedRows = 0;

  sheets.forEach(sheet => {
    const lastRow = sheet.getLastRow();
    if (lastRow > 1) {
      sheet.getRange(2, 1, lastRow - 1, Math.max(sheet.getLastColumn(), 1)).clearContent();
      affectedSheets += 1;
      deletedRows += (lastRow - 1);
    }
  });

  ui.alert(`Done. Cleared ${deletedRows} row(s) from ${affectedSheets} sheet(s).`);
}

function getCandidateSheets_(ss, names) {
  const map = new Map(ss.getSheets().map(s => [String(s.getName()).trim().toLowerCase(), s]));
  return names
    .map(name => map.get(String(name).trim().toLowerCase()))
    .filter(Boolean);
}

function zeroStockInSheet_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol < 1) return { cells: 0 };

  const header = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(v => normalize_(v));
  const stockCols = [];
  header.forEach((value, idx) => {
    if (STOCK_HEADER_KEYWORDS.some(k => value.includes(normalize_(k)))) {
      stockCols.push(idx + 1);
    }
  });

  let cells = 0;
  stockCols.forEach(col => {
    const numRows = lastRow - 1;
    sheet.getRange(2, col, numRows, 1).setValue(0);
    cells += numRows;
  });

  return { cells };
}

function normalize_(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[_\-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
