
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js/+esm";

const SUPABASE_URL =
  window.CDMS_CONFIG?.SUPABASE_URL ||
  window.APP_CONFIG?.SUPABASE_URL ||
  "";

const SUPABASE_ANON_KEY =
  window.CDMS_CONFIG?.SUPABASE_ANON_KEY ||
  window.APP_CONFIG?.SUPABASE_ANON_KEY ||
  "";

const ARCHIVE_WEBAPP_URL =
  window.CDMS_CONFIG?.ARCHIVE_WEBAPP_URL ||
  window.APP_CONFIG?.ARCHIVE_WEBAPP_URL ||
  "https://script.google.com/macros/s/AKfycbywKK4kH3PDT-TbddEw1Wtrd4NStyY5Xpk0BFWMrLOulOCQRHm10WB8TXbs6txeJYfPQw/exec";

const ARCHIVE_SECRET = "779911";

const ARCHIVE_MONTHS_THRESHOLD = 3;
const REALTIME_SYNC_FALLBACK_MS = 5000;
const AUTO_ARCHIVE_STATUS_DOC_ID = "archive_status";
const AUTO_ARCHIVE_LOCK_DOC_ID = "archive_lock";
const AUTO_ARCHIVE_LOCK_MINUTES = 15;


if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
  alert("Supabase config is missing. Open config.js and set SUPABASE_URL and SUPABASE_ANON_KEY first.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const db = { kind: "supabase" };
const TABLE_MAP = {
  settings: "app_settings",
  meta: "app_meta"
};

function normalizeTable(table) {
  return TABLE_MAP[table] || table;
}


function camelToSnake(str) {
  return String(str || "").replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}

function snakeToCamel(str) {
  return String(str || "").replace(/_([a-z])/g, (_, c) => c.toUpperCase());
}

function toDbObject(obj) {
  if (Array.isArray(obj)) return obj.map(toDbObject);
  if (obj && typeof obj === "object" && !(obj instanceof Date)) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[camelToSnake(k)] = toDbObject(v);
    return out;
  }
  return obj;
}

function fromDbObject(obj) {
  if (Array.isArray(obj)) return obj.map(fromDbObject);
  if (obj && typeof obj === "object" && !(obj instanceof Date)) {
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[snakeToCamel(k)] = fromDbObject(v);
    return out;
  }
  return obj;
}

function normalizeField(field) {
  return camelToSnake(field);
}

function cleanData(obj) {
  return Object.fromEntries(
    Object.entries(toDbObject(obj || {})).filter(([, v]) => v !== undefined)
  );
}

function makeDocSnapshot(row) {
  const mapped = row ? fromDbObject(row) : row;
  return {
    exists: () => !!mapped,
    data: () => mapped ? { ...mapped } : undefined
  };
}

function makeQuerySnapshot(rows) {
  return {
    docs: (rows || []).map(row => {
      const mapped = fromDbObject(row);
      return {
        id: mapped.id,
        data: () => ({ ...mapped })
      };
    })
  };
}

function collection(_db, table) {
  return { kind: "collection", table: normalizeTable(table) };
}

function where(field, op, value) {
  return { field: normalizeField(field), op, value };
}

function query(collectionRef, ...filters) {
  return { kind: "query", table: normalizeTable(collectionRef.table), filters };
}

function doc(a, b, c) {
  if (a && a.kind === "collection") {
    return { kind: "doc", table: normalizeTable(a.table), id: b || crypto.randomUUID() };
  }
  return { kind: "doc", table: normalizeTable(b), id: c };
}

function applyFilters(queryBuilder, filters = []) {
  let q = queryBuilder;
  for (const f of filters) {
    if (!f) continue;
    if (f.op === "==" || f.op === "eq") q = q.eq(f.field, f.value);
  }
  return q;
}

async function getDoc(docRef) {
  const { data, error } = await supabase
    .from(normalizeTable(docRef.table))
    .select("*")
    .eq("id", docRef.id)
    .maybeSingle();
  if (error) throw error;
  return makeDocSnapshot(data || null);
}

async function fetchRows(ref) {
  let q = supabase.from(normalizeTable(ref.table)).select("*");
  if (ref.kind === "query") q = applyFilters(q, ref.filters);
  const { data, error } = await q;
  if (error) throw error;
  return (data || []).map(fromDbObject);
}

function onSnapshot(ref, callback) {
  let active = true;
  let lastPayload = "";
  const table = normalizeTable(ref.table);
  const channelName = `cdms-${table}-${Math.random().toString(36).slice(2)}`;

  const emit = async () => {
    if (!active) return;
    try {
      if (ref.kind === "doc") {
        const snap = await getDoc(ref);
        const payload = JSON.stringify(snap.data() || null);
        if (payload !== lastPayload) {
          lastPayload = payload;
          callback(snap);
        }
        return;
      }

      const rows = await fetchRows(ref);
      const payload = JSON.stringify(rows);
      if (payload !== lastPayload) {
        lastPayload = payload;
        callback(makeQuerySnapshot(rows));
      }
    } catch (err) {
      console.error("Snapshot error:", err);
    }
  };

  const channel = supabase
    .channel(channelName)
    .on(
      "postgres_changes",
      { event: "*", schema: "public", table },
      () => {
        emit();
      }
    )
    .subscribe((status) => {
      if (status === "SUBSCRIBED") emit();
    });

  emit();

  return () => {
    active = false;
    supabase.removeChannel(channel);
  };
}

async function setDoc(docRef, payload, options = {}) {
  const table = normalizeTable(docRef.table);
  const row = cleanData({ id: docRef.id, ...payload });
  if (options && options.merge) {
    const currentSnap = await getDoc(docRef);
    const merged = { ...(currentSnap.exists() ? currentSnap.data() : {}), ...row, id: docRef.id };
    const { error } = await supabase.from(table).upsert(merged, { onConflict: 'id' });
    if (error) throw error;
    optimisticUpsertRow(table, merged);
    return;
  }
  const { error } = await supabase.from(table).upsert(row, { onConflict: 'id' });
  if (error) throw error;
  optimisticUpsertRow(table, row);
}

async function updateDoc(docRef, payload) {
  const table = normalizeTable(docRef.table);
  const cleaned = cleanData(payload);
  const { error } = await supabase.from(table).update(cleaned).eq("id", docRef.id);
  if (error) throw error;
  optimisticPatchRow(table, docRef.id, cleaned);
}

async function addDoc(collectionRef, payload) {
  const table = normalizeTable(collectionRef.table);
  const id = crypto.randomUUID();
  const row = cleanData({ id, ...payload });
  const { error } = await supabase
    .from(table)
    .insert(row);
  if (error) throw error;
  optimisticUpsertRow(table, row);
  return { id };
}

async function deleteDoc(docRef) {
  const table = normalizeTable(docRef.table);
  const { error } = await supabase.from(table).delete().eq("id", docRef.id);
  if (error) throw error;
  optimisticDeleteRow(table, docRef.id);
}

function serverTimestamp() {
  return jordanNowIso ? jordanNowIso() : new Date().toISOString();
}

function writeBatch() {
  const ops = [];
  return {
    set(ref, data) { ops.push({ type: "set", ref, data }); },
    update(ref, data) { ops.push({ type: "update", ref, data }); },
    delete(ref) { ops.push({ type: "delete", ref }); },
    async commit() {
      APP.optimisticRenderSuspended = true;
      try {
        for (const op of ops) {
          if (op.type === "set") await setDoc(op.ref, op.data);
          if (op.type === "update") await updateDoc(op.ref, op.data);
          if (op.type === "delete") await deleteDoc(op.ref);
        }
      } finally {
        APP.optimisticRenderSuspended = false;
        flushOptimisticRender();
      }
    }
  };
}

const DEFAULT_PASSWORD = "111111";
const MASTER_PASSWORD = "779911";
// Internal app password only. Database storage is handled through Google Sheets Apps Script.
const PHARMACIES = ["General Hospital Stock", "In-Patient Pharmacy", "Out-Patient Pharmacy", "Medical Center Pharmacy"];
const WORK_PHARMACIES = ["In-Patient Pharmacy", "Out-Patient Pharmacy", "Medical Center Pharmacy"];
const MONTHS = ["January","February","March","April","May","June","July","August","September","October","November","December"];
const USERS = {
  ADMIN: { displayName: "Admin", role: "ADMIN", pharmacyScope: WORK_PHARMACIES, canAudit: true },
  IN_PATIENT_USER: { displayName: "In-Patient Pharmacy", role: "IN_PATIENT_USER", pharmacyScope: ["In-Patient Pharmacy"], canAudit: false },
  OUT_PATIENT_USER: { displayName: "Out-Patient Pharmacy", role: "OUT_PATIENT_USER", pharmacyScope: ["Out-Patient Pharmacy"], canAudit: false },
  MEDICAL_CENTER_USER: { displayName: "Medical Center Pharmacy", role: "MEDICAL_CENTER_USER", pharmacyScope: ["Medical Center Pharmacy"], canAudit: false }
};

const APP = {
  currentTransactionDetail: null,
  currentRole: null,
  currentUser: null,
  auditTab: "new",
  selectedDrugId: null,
  editPrescriptionId: null,
  adjustStockDrugId: null,
  listeners: [],
  liveTimer: null,
  pendingQuickPayload: null,
  confirmAction: null,
  userEditId: null,
  currentPortal: null,
  currentPharmacyScope: null,
  currentUserDocId: null,
  loginMethod: null,
  drugCardsPage: 1,
  currentPatientHistory30Days: [],
  narcoticTab: 'dispensing',
  narcoticDepartmentEditId: null,
  narcoticOrdersBatchRows: [],
  narcoticOpenDepartmentId: null,
  narcoticOpenDepartmentDrugId: null,
  narcoticOpenDrugId: null,
  narcoticEditPrescriptionId: null,
  narcoticPendingOverflowRows: null,
  narcoticDetailTab: "stock",
  narcoticInternalAdjustDrugId: null,
  syncEnhancementsStarted: false,
  syncRefreshBusy: false,
  autoArchiveCheckStarted: false,
  archiveStatus: null,
  archiveView: { dashboard: "all", patients: "all", prescriptions: "all", transactions: "all", narcoticPrescriptions: "all", narcoticMovements: "all" },
  archiveCache: { prescriptions: { rows: [], loaded: false, loading: null }, transactions: { rows: [], loaded: false, loading: null }, narcoticPrescriptions: { rows: [], loaded: false, loading: null }, narcoticOrderMovements: { rows: [], loaded: false, loading: null } },
  optimisticRenderSuspended: false,
  optimisticDirtyTables: new Set(),
  cache: {
    drugs: [],
    inventory: [],
    prescriptions: [],
    transactions: [],
    prescriptionDoses: [],
    users: [],
    pharmacists: [],
    settings: {},
    narcoticDrugs: [],
    narcoticDepartments: [],
    narcoticDepartmentStock: [],
    narcoticPrescriptions: [],
    narcoticOrderMovements: [],
    narcoticInternalStock: []
  }
};


function canCurrentUserTransfer() {
  return APP.currentRole === "ADMIN" || !!APP.currentUser?.canTransfer;
}

function applyRowsToCache(table, rows) {
  const mappedRows = (rows || []).map(row => ({ ...row }));
  if (table === "drugs") APP.cache.drugs = mappedRows.sort((a, b) => String(a.tradeName || "").localeCompare(String(b.tradeName || "")));
  else if (table === "inventory") APP.cache.inventory = mappedRows;
  else if (table === "prescriptions") APP.cache.prescriptions = mappedRows.sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
  else if (table === "transactions") APP.cache.transactions = mappedRows.sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
  else if (table === "prescription_doses") APP.cache.prescriptionDoses = mappedRows.sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
  else if (table === "users") {
    APP.cache.users = mappedRows;
    APP.cache.pharmacists = activeUserRows().map(u => ({
      id: u.id,
      name: u.userName || u.fullName || u.displayName || "",
      jobNumber: u.employeeNumber || "",
      workplace: getUserAllowedPharmacies(u)[0] || "",
      pharmacies: getUserAllowedPharmacies(u),
      canAudit: !!u.canAudit,
      canManageNarcotic: !!u.canManageNarcotic,
      canTransfer: !!u.canTransfer,
      active: u.active !== false
    }));
  } else if (table === "app_settings") APP.cache.settings = mappedRows[0] || {};
  else if (table === "narcotic_drugs") APP.cache.narcoticDrugs = mappedRows.sort((a,b) => String(a.tradeName || "").localeCompare(String(b.tradeName || "")));
  else if (table === "narcotic_departments") APP.cache.narcoticDepartments = mappedRows.filter(r => r.active !== false).sort((a,b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0));
  else if (table === "narcotic_department_stock") APP.cache.narcoticDepartmentStock = mappedRows;
  else if (table === "narcotic_prescriptions") APP.cache.narcoticPrescriptions = mappedRows.sort((a,b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
  else if (table === "narcotic_order_movements") APP.cache.narcoticOrderMovements = mappedRows.sort((a,b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
  else if (table === "narcotic_internal_stock") APP.cache.narcoticInternalStock = mappedRows.sort((a,b) => String(a.drugName || "").localeCompare(String(b.drugName || "")));
}
function renderFromOptimisticCache() {
  if (!APP.currentUser) return;
  renderStaticOptions();
  renderAll();
}

function queueOptimisticRender(table) {
  if (!table) return;
  APP.optimisticDirtyTables.add(normalizeTable(table));
  if (APP.optimisticRenderSuspended) return;
  APP.optimisticDirtyTables.clear();
  renderFromOptimisticCache();
}

function flushOptimisticRender() {
  if (!APP.optimisticDirtyTables.size) return;
  APP.optimisticDirtyTables.clear();
  renderFromOptimisticCache();
}

function getCacheRowsForTable(table) {
  const normalized = normalizeTable(table);
  if (normalized === "app_settings") {
    return APP.cache.settings && Object.keys(APP.cache.settings).length ? [{ ...APP.cache.settings }] : [];
  }
  if (normalized === "drugs") return [...(APP.cache.drugs || [])];
  if (normalized === "inventory") return [...(APP.cache.inventory || [])];
  if (normalized === "prescriptions") return [...(APP.cache.prescriptions || [])];
  if (normalized === "transactions") return [...(APP.cache.transactions || [])];
  if (normalized === "prescription_doses") return [...(APP.cache.prescriptionDoses || [])];
  if (normalized === "users") return [...(APP.cache.users || [])];
  if (normalized === "narcotic_drugs") return [...(APP.cache.narcoticDrugs || [])];
  if (normalized === "narcotic_departments") return [...(APP.cache.narcoticDepartments || [])];
  if (normalized === "narcotic_department_stock") return [...(APP.cache.narcoticDepartmentStock || [])];
  if (normalized === "narcotic_prescriptions") return [...(APP.cache.narcoticPrescriptions || [])];
  if (normalized === "narcotic_order_movements") return [...(APP.cache.narcoticOrderMovements || [])];
  if (normalized === "narcotic_internal_stock") return [...(APP.cache.narcoticInternalStock || [])];
  return [];
}

function optimisticUpsertRow(table, row) {
  const normalized = normalizeTable(table);
  if (!row) return;
  const nextRows = getCacheRowsForTable(normalized);
  const rowId = row.id;
  if (normalized === "app_settings") {
    applyRowsToCache(normalized, [{ ...(nextRows[0] || {}), ...row }]);
    queueOptimisticRender(normalized);
    return;
  }
  const index = nextRows.findIndex(item => String(item?.id || "") === String(rowId || ""));
  if (index >= 0) nextRows[index] = { ...nextRows[index], ...row };
  else nextRows.push({ ...row });
  applyRowsToCache(normalized, nextRows);
  queueOptimisticRender(normalized);
}

function optimisticPatchRow(table, id, patch) {
  optimisticUpsertRow(table, { id, ...patch });
}

function optimisticDeleteRow(table, id) {
  const normalized = normalizeTable(table);
  if (normalized === "app_settings") {
    applyRowsToCache(normalized, []);
    queueOptimisticRender(normalized);
    return;
  }
  const nextRows = getCacheRowsForTable(normalized).filter(item => String(item?.id || "") !== String(id || ""));
  applyRowsToCache(normalized, nextRows);
  queueOptimisticRender(normalized);
}

async function refreshTablesImmediate(tables = []) {
  const uniqueTables = [...new Set((tables || []).map(normalizeTable).filter(Boolean))];
  for (const table of uniqueTables) {
    const rows = await fetchRows(collection(db, table));
    applyRowsToCache(table, rows);
  }
  renderStaticOptions();
  renderAll();
}

async function applyOperations(operations = []) {
  APP.optimisticRenderSuspended = true;
  try {
    for (const op of operations) {
      const refTable = normalizeTable(op.table);
      const ref = { kind: "doc", table: refTable, id: op.id };
      if (op.type === "set") await setDoc(ref, op.data);
      else if (op.type === "update") await updateDoc(ref, op.data);
      else if (op.type === "delete") await deleteDoc(ref);
    }
  } finally {
    APP.optimisticRenderSuspended = false;
    flushOptimisticRender();
  }
}

function monthsAgoDate(months = ARCHIVE_MONTHS_THRESHOLD) {
  const d = new Date();
  d.setMonth(d.getMonth() - Number(months || ARCHIVE_MONTHS_THRESHOLD));
  return d;
}

function isOlderThanMonths(dateTime, months = ARCHIVE_MONTHS_THRESHOLD) {
  if (!dateTime) return false;
  const d = new Date(dateTime);
  if (Number.isNaN(d.getTime())) return false;
  return d < monthsAgoDate(months);
}

async function postRowsToArchive(sheetName, rows) {
  if (!rows?.length) {
    return { success: true, inserted: 0, sheetName };
  }

  const res = await fetch(ARCHIVE_WEBAPP_URL, {
    method: "POST",
    headers: {
      "Content-Type": "text/plain;charset=utf-8"
    },
    body: JSON.stringify({
      secret: ARCHIVE_SECRET,
      sheetName,
      rows
    })
  });

  const text = await res.text();
  let data = {};

  try {
    data = JSON.parse(text || "{}");
  } catch (_) {
    data = {
      success: false,
      error: text || "Invalid response from archive server"
    };
  }

  if (!res.ok || data?.success !== true) {
    throw new Error(data?.error || `Archive request failed for ${sheetName}`);
  }

  return data;
}


async function archiveTableRows({
  sourceTable,
  archiveSheetName,
  months = ARCHIVE_MONTHS_THRESHOLD,
  dateField = "dateTime"
}) {
  const rows = await fetchRows(collection(db, sourceTable));
  const oldRows = rows.filter(row => isOlderThanMonths(row?.[dateField], months));

  if (!oldRows.length) {
    return {
      sourceTable,
      archiveSheetName,
      archived: 0,
      deleted: 0
    };
  }

  await postRowsToArchive(archiveSheetName, oldRows);

  APP.optimisticRenderSuspended = true;
  try {
    for (const row of oldRows) {
      if (!row?.id) continue;

      if (sourceTable === "prescriptions") {
        const linkedDoses = await fetchRows(
          query(collection(db, "prescription_doses"), where("prescriptionId", "==", row.id))
        );

        for (const dose of linkedDoses) {
          if (!dose?.id) continue;
          await deleteDoc(doc(db, "prescription_doses", dose.id));
        }
      }

      await deleteDoc(doc(db, sourceTable, row.id));
    }
  } finally {
    APP.optimisticRenderSuspended = false;
    flushOptimisticRender();
  }

  return {
    sourceTable,
    archiveSheetName,
    archived: oldRows.length,
    deleted: oldRows.length
  };
}


async function archiveOldDataNow(options = {}) {
  const silent = !!options.silent;
  const reason = options.reason || "manual";
  if (!silent) {
    showActionModal("Archive Data", "Please wait while old records are being archived to Google Sheets...");
  }

  try {
    const results = [];
    results.push(await archiveTableRows({
      sourceTable: "prescription_doses",
      archiveSheetName: "prescription_doses_archive",
      dateField: "createdAt"
    }));
    results.push(await archiveTableRows({
      sourceTable: "prescriptions",
      archiveSheetName: "prescriptions_archive"
    }));
    results.push(await archiveTableRows({
      sourceTable: "transactions",
      archiveSheetName: "transactions_archive"
    }));
    results.push(await archiveTableRows({
      sourceTable: "narcotic_prescriptions",
      archiveSheetName: "narcotic_prescriptions_archive"
    }));
    results.push(await archiveTableRows({
      sourceTable: "narcotic_order_movements",
      archiveSheetName: "narcotic_order_movements_archive"
    }));

    await refreshTablesImmediate([
      "prescription_doses",
      "prescriptions",
      "transactions",
      "narcotic_prescriptions",
      "narcotic_order_movements"
    ]);

    const totalArchived = results.reduce((sum, item) => sum + Number(item.archived || 0), 0);

    const archiveStartedAt = new Date().toISOString();
    const archiveFromDate = monthsAgoDate(ARCHIVE_MONTHS_THRESHOLD).toISOString().slice(0, 10);
    const archiveToDate = new Date().toISOString().slice(0, 10);

    const { data: insertedRun, error: archiveRunError } = await supabase
      .from("archive_runs")
      .insert({
        archive_key: `${reason || "manual"}_${archiveStartedAt}`,
        archived_from: archiveFromDate,
        archived_to: archiveToDate,
        target_sheet_name: "all_archive_sheets",
        row_count: totalArchived,
        status: "success",
        started_at: archiveStartedAt,
        completed_at: new Date().toISOString(),
        notes: `Archive run from app (${reason || "manual"})`,
        created_at: archiveStartedAt
      })
      .select("*")
      .maybeSingle();

    if (archiveRunError) {
      console.error("Archive run log insert failed:", archiveRunError);
    } else if (insertedRun) {
      APP.archiveStatus = insertedRun;
    }

    await loadArchiveStatus();

    if (!silent) {
      finishActionModal(
        true,
        totalArchived
          ? `Archive completed successfully. ${totalArchived} old row(s) moved to Google Sheets.`
          : "Archive completed successfully. No rows older than 3 months were found."
      );
    }
    console.log("Archive Results:", results);
    return results;
  } catch (error) {
    console.error("Archive Error:", error);
    try {
      const failedAt = new Date().toISOString();
      await supabase.from("archive_runs").insert({
        archive_key: `failed_${failedAt}`,
        archived_from: monthsAgoDate(ARCHIVE_MONTHS_THRESHOLD).toISOString().slice(0, 10),
        archived_to: new Date().toISOString().slice(0, 10),
        target_sheet_name: "all_archive_sheets",
        row_count: 0,
        status: "failed",
        started_at: failedAt,
        completed_at: new Date().toISOString(),
        notes: String(error?.message || "Archive failed"),
        created_at: failedAt
      });
      await loadArchiveStatus();
    } catch (archiveRunError) {
      console.error("Archive run failure log insert failed:", archiveRunError);
    }
    if (!silent) {
      finishActionModal(false, error?.message || "Archive failed.");
    }
    throw error;
  }
}

function isArchiveDue(lastSuccessfulRunAt, months = ARCHIVE_MONTHS_THRESHOLD) {
  if (!lastSuccessfulRunAt) return true;
  const d = new Date(lastSuccessfulRunAt);
  if (Number.isNaN(d.getTime())) return true;
  const nextRun = new Date(d);
  nextRun.setMonth(nextRun.getMonth() + Number(months || ARCHIVE_MONTHS_THRESHOLD));
  return new Date() >= nextRun;
}

function getArchiveRunCreatedAt(run) {
  return run?.createdAt || run?.created_at || run?.dateTime || run?.date_time || "";
}

function formatArchiveStatusLabel(run) {
  if (!run) return "No archive has been recorded yet.";
  const createdAt = getArchiveRunCreatedAt(run);
  const status = String(run?.status || "unknown").trim() || "unknown";
  const when = createdAt ? formatJordanDateTime(createdAt, true) : "-";
  return `Last archive: ${when} · Status: ${status}`;
}

async function fetchLatestArchiveRun() {
  const { data, error } = await supabase
    .from("archive_runs")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(1);
  if (error) throw error;
  return data?.[0] || null;
}

async function loadArchiveStatus() {
  if (APP.currentRole !== "ADMIN") return null;
  try {
    const latestRun = await fetchLatestArchiveRun();
    APP.archiveStatus = latestRun || null;
    renderDashboard();
    renderSettings();
    return latestRun;
  } catch (error) {
    console.error("Archive status load failed:", error);
    return null;
  }
}

async function runAutomaticArchiveIfDue() {
  if (APP.currentRole !== "ADMIN") return false;

  try {
    const { data: runs, error } = await supabase
      .from("archive_runs")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(1);

    if (error) throw error;

    const lastRun = runs?.[0]?.created_at
      ? new Date(runs[0].created_at)
      : null;

    const now = new Date();

    let shouldRun = false;

    if (!lastRun) {
      shouldRun = true;
    } else {
      const nextRun = new Date(lastRun);
      nextRun.setMonth(nextRun.getMonth() + ARCHIVE_MONTHS_THRESHOLD);
      shouldRun = now >= nextRun;
    }

    if (!shouldRun) {
      await loadArchiveStatus();
      return false;
    }

    await archiveOldDataNow({ silent: true, reason: "auto_admin_login" });
    await loadArchiveStatus();

    return true;

  } catch (error) {
    console.error("Automatic archive check failed:", error);
    return false;
  }
}

async function refreshCrossUserSyncNow() {
  if (!APP.currentUser || APP.syncRefreshBusy) return;
  APP.syncRefreshBusy = true;
  try {
    const tables = [
      "drugs",
      "inventory",
      "prescriptions",
      "transactions",
      "users",
      "app_settings"
    ];
    if (APP.currentRole === "ADMIN") {
      tables.push("archive_runs");
    }
    if (APP.currentRole === "ADMIN" || APP.currentPortal === "IN_PATIENT_USER") {
      tables.push(
        "narcotic_drugs",
        "narcotic_departments",
        "narcotic_department_stock",
        "narcotic_prescriptions",
        "narcotic_order_movements",
        "narcotic_internal_stock"
      );
    }
    await refreshTablesImmediate(tables);
    if (APP.currentRole === "ADMIN") await loadArchiveStatus();
  } catch (error) {
    console.error("Cross-user sync refresh failed:", error);
  } finally {
    APP.syncRefreshBusy = false;
  }
}

function startRealtimeSyncEnhancements() {
  if (APP.syncEnhancementsStarted) return;
  APP.syncEnhancementsStarted = true;
  window.addEventListener("focus", () => {
    refreshCrossUserSyncNow();
  });
  window.addEventListener("online", () => {
    refreshCrossUserSyncNow();
  });
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refreshCrossUserSyncNow();
  });
}

function queueAutomaticArchiveCheck() {
  if (APP.autoArchiveCheckStarted || APP.currentRole !== "ADMIN") return;
  APP.autoArchiveCheckStarted = true;
  setTimeout(async () => {
    try {
      await runAutomaticArchiveIfDue();
    } catch (error) {
      console.error("Automatic archive check failed:", error);
    } finally {
      APP.autoArchiveCheckStarted = false;
    }
  }, 1500);
}



const ARCHIVE_VIEW_MODES = { CURRENT: "current", ARCHIVED: "archived", ALL: "all" };
const ARCHIVE_BUCKET_CONFIG = {
  prescriptions: { sheetName: "prescriptions_archive", cacheKey: "prescriptions" },
  transactions: { sheetName: "transactions_archive", cacheKey: "transactions" },
  narcoticPrescriptions: { sheetName: "narcotic_prescriptions_archive", cacheKey: "narcoticPrescriptions" },
  narcoticOrderMovements: { sheetName: "narcotic_order_movements_archive", cacheKey: "narcoticOrderMovements" }
};

function archiveRowId(prefix, row) {
  const base = `${row?.id || ''}_${row?.dateTime || row?.createdAt || row?.started_at || ''}_${row?.fileNumber || row?.invoiceNumber || row?.type || ''}`;
  return `${prefix}_${String(base).replace(/[^a-zA-Z0-9_-]+/g, '_')}`;
}

function normalizeArchivePrescriptionRow(row = {}) {
  return {
    ...row,
    id: row.id || archiveRowId('arch_rx', row),
    qtyBoxes: Number(row.qtyBoxes || 0),
    qtyUnits: Number(row.qtyUnits || 0),
    __archived: true,
    __archiveSheet: row.__archiveSheet || 'prescriptions_archive'
  };
}

function normalizeArchiveTransactionRow(row = {}) {
  return {
    ...row,
    id: row.id || archiveRowId('arch_tx', row),
    qtyBoxes: Number(row.qtyBoxes || 0),
    qtyUnits: Number(row.qtyUnits || 0),
    __archived: true,
    __archiveSheet: row.__archiveSheet || 'transactions_archive'
  };
}

function normalizeArchiveNarcoticPrescriptionRow(row = {}) {
  return {
    ...row,
    id: row.id || archiveRowId('arch_nrx', row),
    dispensedUnits: Number(row.dispensedUnits || row.quantitySent || 0),
    __archived: true,
    __archiveSheet: row.__archiveSheet || 'narcotic_prescriptions_archive'
  };
}

function normalizeArchiveNarcoticMovementRow(row = {}) {
  return {
    ...row,
    id: row.id || archiveRowId('arch_nom', row),
    quantitySent: Number(row.quantitySent || 0),
    quantityReceived: Number(row.quantityReceived || 0),
    dispensedUnits: Number(row.dispensedUnits || 0),
    emptyAmpoulesReceived: Number(row.emptyAmpoulesReceived || 0),
    __archived: true,
    __archiveSheet: row.__archiveSheet || 'narcotic_order_movements_archive'
  };
}

function normalizeArchiveBucketRows(bucket, rows) {
  if (bucket === 'prescriptions') return (rows || []).map(normalizeArchivePrescriptionRow);
  if (bucket === 'transactions') return (rows || []).map(normalizeArchiveTransactionRow);
  if (bucket === 'narcoticPrescriptions') return (rows || []).map(normalizeArchiveNarcoticPrescriptionRow);
  if (bucket === 'narcoticOrderMovements') return (rows || []).map(normalizeArchiveNarcoticMovementRow);
  return rows || [];
}

function buildArchiveScriptUrl(sheetName, params = {}) {
  const url = new URL(ARCHIVE_WEBAPP_URL);
  url.searchParams.set('action', 'search_archive');
  url.searchParams.set('secret', ARCHIVE_SECRET);
  url.searchParams.set('sheetName', sheetName);
  Object.entries(params || {}).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value) !== '') url.searchParams.set(key, String(value));
  });
  return url.toString();
}

function loadArchiveJsonp(sheetName, params = {}) {
  return new Promise((resolve, reject) => {
    const callbackName = `cdmsArchiveCallback_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const script = document.createElement('script');
    const timeout = window.setTimeout(() => {
      cleanup();
      reject(new Error(`Archive request timed out for ${sheetName}`));
    }, 15000);

    function cleanup() {
      window.clearTimeout(timeout);
      try { delete window[callbackName]; } catch (_) { window[callbackName] = undefined; }
      if (script.parentNode) script.parentNode.removeChild(script);
    }

    window[callbackName] = (payload) => {
      cleanup();
      resolve(payload || {});
    };

    script.async = true;
    script.src = buildArchiveScriptUrl(sheetName, { ...params, callback: callbackName });
    script.onerror = () => {
      cleanup();
      reject(new Error(`Archive script load failed for ${sheetName}`));
    };
    document.head.appendChild(script);
  });
}

async function loadArchiveBucket(bucket, { force = false } = {}) {
  const config = ARCHIVE_BUCKET_CONFIG[bucket];
  if (!config) return [];
  const cache = APP.archiveCache?.[config.cacheKey];
  if (!cache) return [];
  if (cache.loading) return cache.loading;
  if (cache.loaded && !force) return cache.rows || [];

  const pharmacyScope = APP.currentRole === 'ADMIN' ? '' : currentScopePharmacy();
  cache.loading = (async () => {
    try {
      const result = await loadArchiveJsonp(config.sheetName, { pharmacyScope });
      if (result?.success !== true) throw new Error(result?.error || `Archive request failed for ${config.sheetName}`);
      cache.rows = normalizeArchiveBucketRows(bucket, result.rows || []);
      cache.loaded = true;
      return cache.rows;
    } catch (error) {
      console.error(`Archive bucket load failed: ${bucket}`, error);
      cache.rows = [];
      cache.loaded = false;
      return [];
    } finally {
      cache.loading = null;
    }
  })();
  return cache.loading;
}

function archiveRowsFor(bucket) {
  const config = ARCHIVE_BUCKET_CONFIG[bucket];
  return config ? (APP.archiveCache?.[config.cacheKey]?.rows || []) : [];
}

function getArchiveMode(section) {
  return APP.archiveView?.[section] || ARCHIVE_VIEW_MODES.ALL;
}

function setArchiveMode(section, mode) {
  if (!APP.archiveView) APP.archiveView = {};
  APP.archiveView[section] = mode;
  if (section === 'dashboard') { renderDashboard(); renderDrugRows(); renderRecentPrescriptionsModal(); }
  else if (section === 'patients') renderPatientsPage();
  else if (section === 'prescriptions') renderPrescriptions();
  else if (section === 'transactions') renderTransactions();
  else if (section === 'narcoticPrescriptions') { renderNarcoticRecent(); renderNarcoticRecentRows(); }
  else if (section === 'narcoticMovements') renderNarcoticTransactionsTable();
}

function archiveToggleHtml(section) {
  const mode = getArchiveMode(section);
  const item = (value, label) => `<button type="button" class="soft-btn mini-btn ${mode === value ? 'active' : ''}" data-archive-mode="${section}:${value}">${label}</button>`;
  return `<div class="archive-toggle-wrap" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:8px 0 12px"><span class="subline" style="font-weight:700">Data View</span>${item('current','Current Data')}${item('archived','Archived Data')}${item('all','All')}</div>`;
}

function mountArchiveToggle(hostEl, section, options = {}) {
  if (!hostEl) return;
  const panelId = options.panelId || `${section}ArchiveToggle`;
  let panel = document.getElementById(panelId);
  if (!panel) {
    panel = document.createElement('div');
    panel.id = panelId;
    if (options.prepend) hostEl.prepend(panel);
    else if (options.beforeEl && options.beforeEl.parentNode === hostEl) hostEl.insertBefore(panel, options.beforeEl);
    else hostEl.appendChild(panel);
  }
  panel.innerHTML = archiveToggleHtml(section);
  panel.querySelectorAll('[data-archive-mode]').forEach(btn => {
    btn.onclick = async () => {
      const [sec, mode] = String(btn.dataset.archiveMode || '').split(':');
      if (mode !== ARCHIVE_VIEW_MODES.CURRENT) {
        if (sec === 'dashboard' || sec === 'patients' || sec === 'prescriptions') await loadArchiveBucket('prescriptions');
        if (sec === 'transactions') await loadArchiveBucket('transactions');
        if (sec === 'narcoticPrescriptions') await loadArchiveBucket('narcoticPrescriptions');
        if (sec === 'narcoticMovements') await loadArchiveBucket('narcoticOrderMovements');
      }
      setArchiveMode(sec, mode);
    };
  });
}

function isArchivedRow(row) {
  return !!row?.__archived;
}

function archivedLabelHtml(row) {
  return isArchivedRow(row) ? ' <span class="badge pending">Archived</span>' : '';
}

function mergeArchiveModeRows(currentRows, archivedRows, mode) {
  if (mode === ARCHIVE_VIEW_MODES.CURRENT) return [...currentRows];
  if (mode === ARCHIVE_VIEW_MODES.ARCHIVED) return [...archivedRows];
  return [...currentRows, ...archivedRows];
}

function getScopedPrescriptionRows(mode = ARCHIVE_VIEW_MODES.ALL) {
  const scope = currentScopePharmacy();
  const currentRows = APP.currentRole === 'ADMIN' ? [...(APP.cache.prescriptions || [])] : (APP.cache.prescriptions || []).filter(row => String(row.pharmacy || '') === String(scope));
  const archivedRows = APP.currentRole === 'ADMIN' ? archiveRowsFor('prescriptions') : archiveRowsFor('prescriptions').filter(row => String(row.pharmacy || '') === String(scope));
  return mergeArchiveModeRows(currentRows, archivedRows, mode).sort((a, b) => String(b.dateTime || '').localeCompare(String(a.dateTime || '')));
}

function getMergedTransactionRows(mode = ARCHIVE_VIEW_MODES.ALL) {
  const scope = currentScopePharmacy();
  const currentRows = APP.currentRole === 'ADMIN' ? [...(APP.cache.transactions || [])] : (APP.cache.transactions || []).filter(row => row.pharmacy === scope || String(row.pharmacy || '').includes(scope));
  const archivedRows = APP.currentRole === 'ADMIN' ? archiveRowsFor('transactions') : archiveRowsFor('transactions').filter(row => row.pharmacy === scope || String(row.pharmacy || '').includes(scope) || String(row.fromPharmacy || '') === String(scope) || String(row.toPharmacy || '') === String(scope));
  return mergeArchiveModeRows(currentRows, archivedRows, mode).sort((a, b) => String(b.dateTime || '').localeCompare(String(a.dateTime || '')));
}

function getMergedNarcoticPrescriptionRows(mode = ARCHIVE_VIEW_MODES.ALL) {
  return mergeArchiveModeRows(APP.cache.narcoticPrescriptions || [], archiveRowsFor('narcoticPrescriptions'), mode).sort((a, b) => String(b.dateTime || '').localeCompare(String(a.dateTime || '')));
}

function getMergedNarcoticMovementRows(mode = ARCHIVE_VIEW_MODES.ALL) {
  return mergeArchiveModeRows(APP.cache.narcoticOrderMovements || [], archiveRowsFor('narcoticOrderMovements'), mode).sort((a, b) => String(b.dateTime || '').localeCompare(String(a.dateTime || '')));
}

async function ensureArchiveCacheForSection(section) {
  const mode = getArchiveMode(section);
  if (mode === ARCHIVE_VIEW_MODES.CURRENT) return;
  if (section === 'dashboard' || section === 'patients' || section === 'prescriptions') await loadArchiveBucket('prescriptions');
  if (section === 'transactions') await loadArchiveBucket('transactions');
  if (section === 'narcoticPrescriptions') await loadArchiveBucket('narcoticPrescriptions');
  if (section === 'narcoticMovements') await loadArchiveBucket('narcoticOrderMovements');
}

const q = id => document.getElementById(id);
const esc = value => String(value ?? "").replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch]));
const todayKey = () => { const p = jordanDateParts(); return `${p.year}-${p.month}-${p.day}`; };
const monthKey = dateText => (dateText || "").slice(0, 7);
const selectedMonthKey = () => {
  const year = Number(APP.cache.settings.year || new Date().getFullYear());
  const monthIndex = Math.max(0, MONTHS.indexOf(APP.cache.settings.month || MONTHS[new Date().getMonth()]));
  return `${year}-${String(monthIndex + 1).padStart(2, "0")}`;
};

const JORDAN_TZ = "Asia/Amman";

function jordanDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: JORDAN_TZ,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false
  }).formatToParts(date);
  const obj = {};
  for (const part of parts) if (part.type !== "literal") obj[part.type] = part.value;
  return obj;
}

function jordanNowIso() {
  return new Date().toISOString();
}

function jordanDateKey() {
  const p = jordanDateParts();
  return `${p.year}-${p.month}-${p.day}`;
}

function formatJordanDateTime(value, withSeconds = false) {
  if (!value) return "-";
  const s = String(value).trim();
  const date = new Date(s);
  if (Number.isNaN(date.getTime())) {
    return s.replace("T", " ").slice(0, withSeconds ? 19 : 16);
  }
  const p = jordanDateParts(date);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}${withSeconds ? `:${p.second}` : ""}`;
}

function toSmartTitleCase(value) {
  return String(value || "").replace(/\S+/g, word => {
    return word
      .split(/([-'])/)
      .map(part => /[-']/.test(part) ? part : (part.charAt(0).toUpperCase() + part.slice(1).toLowerCase()))
      .join('');
  });
}

function applyTitleCaseToInput(input) {
  if (!input) return;
  const next = toSmartTitleCase(input.value);
  if (input.value !== next) {
    const atEnd = input.selectionStart === input.value.length;
    input.value = next;
    if (atEnd && typeof input.setSelectionRange === 'function') input.setSelectionRange(next.length, next.length);
  }
}

function bindAutoTitleCaseInput(ids = []) {
  ids.forEach(id => {
    const input = q(id);
    if (!input || input.dataset.titleCaseBound === 'true') return;
    const handler = () => applyTitleCaseToInput(input);
    input.addEventListener('input', handler);
    input.addEventListener('change', handler);
    input.addEventListener('blur', handler);
    input.dataset.titleCaseBound = 'true';
  });
}

function parseJordanDateTime(value) {
  if (!value) return null;
  const s = String(value).trim();
  const d = new Date(s);
  if (!Number.isNaN(d.getTime())) return d;
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [,y,mo,dv,h,mi,se='00'] = m;
    return new Date(`${y}-${mo}-${dv}T${h}:${mi}:${se}+03:00`);
  }
  return null;
}

function canEditPrescription(rx) {
  return !!rx && rx.status !== "Returned";
}

function userWorksInScope(user, scope) {
  const pharmacies = Array.isArray(user?.allowedPharmacies) && user.allowedPharmacies.length
    ? user.allowedPharmacies
    : (Array.isArray(user?.pharmacies) && user.pharmacies.length ? user.pharmacies : [user?.workplace].filter(Boolean));
  return pharmacies.includes(scope);
}

function activeUserRows() {
  return (APP.cache.users || []).filter(row => row && row.active !== false && String(row.active).toUpperCase() !== "FALSE");
}

function getScopePharmacists(scope, opts = {}) {
  return activeUserRows().filter(user => userWorksInScope(user, scope) && (!opts.canAuditOnly || user.canAudit));
}

function renderLiveClocks() {
  const scope = currentScopePharmacy();
  const nowText = formatJordanDateTime(jordanNowIso());
  if (q("quickLiveTime")) q("quickLiveTime").textContent = nowText;
  if (q("dashboardScopeChip")) q("dashboardScopeChip").textContent = `${scope} · ${nowText}`;
  if (q("drugModalLiveTime")) q("drugModalLiveTime").textContent = nowText;

  document.querySelectorAll('[data-edit-btn]').forEach(btn => {
    const rx = APP.cache.prescriptions.find(row => row.id === btn.dataset.id);
    btn.classList.toggle('hidden', !canEditPrescription(rx));
  });

  document.querySelectorAll('[data-edit-countdown]').forEach(el => {
    el.textContent = '';
    el.classList.add('hidden');
  });
}

async function sha256(text) {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
}

function applyTheme(theme) {
  const allowed = ["light","dark","blue","green"];
  const nextTheme = allowed.includes(theme) ? theme : "light";
  document.body.classList.remove("theme-light","theme-dark","theme-blue","theme-green","dark");
  document.body.classList.add(`theme-${nextTheme}`);
  if (nextTheme === "dark") document.body.classList.add("dark");
  localStorage.setItem("cdms_theme", nextTheme);
}

function themeInit() {
  applyTheme(localStorage.getItem("cdms_theme") || "light");
}

function toggleTheme() {
  const current = localStorage.getItem("cdms_theme") || "light";
  const order = ["light","dark","blue","green"];
  const next = order[(order.indexOf(current) + 1) % order.length];
  applyTheme(next);
}

function openModal(id) {
  APP.modalZCounter = (APP.modalZCounter || 60) + 2;
  q("overlay").classList.remove("hidden");
  q("overlay").style.zIndex = Math.max(55, APP.modalZCounter - 1);
  const modal = q(id);
  modal.style.zIndex = APP.modalZCounter;
  modal.classList.remove("hidden");
}

function closeModal(id) {
  const modal = q(id);
  modal.classList.add("hidden");
  modal.style.zIndex = "";
  if (id === "actionModal") document.body.classList.remove("action-modal-open");
  const visible = [...document.querySelectorAll(".modal:not(.hidden)")];
  if (!visible.length) {
    q("overlay").classList.add("hidden");
    q("overlay").style.zIndex = "";
    return;
  }
  const topModal = visible.reduce((best, el) => {
    const z = Number(el.style.zIndex || 0);
    return !best || z > Number(best.style.zIndex || 0) ? el : best;
  }, null);
  q("overlay").style.zIndex = String(Math.max(55, Number(topModal?.style.zIndex || 60) - 1));
}

function showActionModal(title, body, waiting = true) {
  q("actionTitle").textContent = title;
  q("actionBody").innerHTML = waiting ? `<span class="spinner"></span>${body}` : body;
  q("actionOkBtn").classList.toggle("hidden", waiting);
  document.body.classList.add("action-modal-open");
  openModal("actionModal");
}

function finishActionModal(ok, msg) {
  q("actionBody").innerHTML = `<div style="font-weight:800;color:${ok ? 'var(--success)' : 'var(--danger)'}">${esc(msg)}</div>`;
  q("actionOkBtn").classList.remove("hidden");
}

function portalToScope(role) {
  return role === "IN_PATIENT_USER" ? "In-Patient Pharmacy" :
    role === "OUT_PATIENT_USER" ? "Out-Patient Pharmacy" :
    role === "MEDICAL_CENTER_USER" ? "Medical Center Pharmacy" : "In-Patient Pharmacy";
}

function getUserAllowedPharmacies(user) {
  const raw = user?.allowedPharmacies;
  if (Array.isArray(raw)) return raw.filter(Boolean);
  return String(raw || "").split("|").map(v => String(v || "").trim()).filter(Boolean);
}

function currentActorName() {
  return APP.currentUser?.userName || APP.currentUser?.fullName || APP.currentUser?.displayName || APP.currentUser?.name || "";
}

function currentActorEmployeeNumber() {
  return String(APP.currentUser?.employeeNumber || APP.currentUser?.jobNumber || "").trim();
}

function actorDisplayName() {
  return currentActorName() || APP.currentRole || "";
}
function canCurrentUserAudit() {
  return APP.currentRole === "ADMIN" || !!APP.currentUser?.canAudit;
}
function canDeletePrescriptionRow(row) {
  if (!row) return false;
  if (APP.currentRole === "ADMIN") return true;
  return canCurrentUserAudit() && String(row.pharmacy || "") === String(currentScopePharmacy() || "");
}
function canViewPharmacy(pharmacy) {
  return APP.currentRole === "ADMIN" || String(pharmacy || "") === String(currentScopePharmacy() || "");
}

function applyCurrentUserReadonlyFields() {
  const name = currentActorName();
  [
    "quickPharmacist",
    "narcoticPharmacist",
    "narcoticInternalReceivePharmacist",
    "narcoticOrderPharmacist",
    "narcoticEditPharmacist",
    "confirmActionPharmacist",
    "auditAuditor"
  ].forEach(id => {
    const el = q(id);
    if (!el) return;
    if (el.tagName === "SELECT") {
      el.innerHTML = `<option value="${esc(name)}">${esc(name || "-")}</option>`;
      el.value = name;
      el.disabled = true;
    } else {
      el.value = name;
      el.readOnly = true;
    }
  });
}

function currentScopePharmacy() {
  if (APP.currentRole === "ADMIN") return APP.currentPharmacyScope || APP.cache.settings.pharmacyType || "In-Patient Pharmacy";
  return APP.currentPharmacyScope || APP.currentUser?.pharmacyScope?.[0] || "In-Patient Pharmacy";
}


function currentAuditPharmacy() {
  if (APP.currentRole !== "ADMIN") return currentScopePharmacy();
  return q("auditPharmacy")?.value || currentScopePharmacy();
}

function currentReportPharmacy() {
  if (APP.currentRole !== "ADMIN") return currentScopePharmacy();
  return q("reportPharmacy")?.value || currentScopePharmacy();
}

function scopedPrescriptionRowsByPharmacy(pharmacy) {
  if (pharmacy === "ALL_WORK_PHARMACIES") return APP.cache.prescriptions.filter(row => WORK_PHARMACIES.includes(row.pharmacy));
  return APP.cache.prescriptions.filter(row => row.pharmacy === pharmacy);
}

function sortDrugsAlphabetically(drugs) {
  return [...drugs].sort((a, b) => `${a.tradeName || ""} ${a.strength || ""}`.localeCompare(`${b.tradeName || ""} ${b.strength || ""}`));
}

function prescriptionScopeRows() {
  return getScopedPrescriptionRows(getArchiveMode('dashboard'));
}

function transactionScopeRows() {
  return getMergedTransactionRows(getArchiveMode('transactions'));
}

function unitLabel(drug) {
  const form = (drug?.dosageForm || "").toLowerCase();
  if (form.includes("tablet")) return "tablets";
  if (form.includes("capsule")) return "capsules";
  if (form.includes("patch")) return "patches";
  if (form.includes("inject")) return "ampoules";
  if (form.includes("drop")) return "drops";
  if (form.includes("susp")) return "ml";
  return "units";
}

function invRow(drugId, pharmacy) {
  return APP.cache.inventory.find(item => item.drugId === drugId && item.pharmacy === pharmacy);
}

function normalizeInventory(boxes, units, unitsPerBox) {
  const perBox = Math.max(1, Number(unitsPerBox || 1));
  let totalUnits = Number(boxes || 0) * perBox + Number(units || 0);
  if (totalUnits < 0) totalUnits = 0;
  return {
    boxes: Math.floor(totalUnits / perBox),
    units: totalUnits % perBox,
    totalUnits
  };
}

function formatStock(boxes, units, drug) {
  return `${Number(boxes || 0)} box(es) + ${Number(units || 0)} ${unitLabel(drug)}`;
}

function statusBadge(status) {
  const key = String(status || "Registered").toLowerCase();
  const cls = key === "verified" ? "verified" : key === "pending" ? "pending" : key === "returned" ? "returned" : "";
  return `<span class="badge ${cls}">${esc(status || "Registered")}</span>`;
}


function setSelectOptions(selectId, items, preferredValue, disabled = false) {
  const el = q(selectId);
  if (!el) return;
  const normalized = (items || []).map(item => typeof item === "string" ? { value: item, label: item } : item).filter(Boolean);
  el.innerHTML = normalized.map(item => `<option value="${esc(item.value)}">${esc(item.label)}</option>`).join("");
  const fallback = normalized[0]?.value || "";
  const nextValue = normalized.some(item => item.value === preferredValue) ? preferredValue : fallback;
  el.value = nextValue;
  el.disabled = disabled;
}

function syncTransferToOptions(preferredValue) {
  const from = q("transferFrom")?.value || currentScopePharmacy();
  const scope = currentScopePharmacy();
  const isAdmin = APP.currentRole === "ADMIN";
  const baseOptions = isAdmin ? PHARMACIES : PHARMACIES.filter(name => name !== scope ? true : true);
  const filtered = baseOptions.filter(name => name !== from);
  const safeOptions = filtered.length ? filtered : PHARMACIES.filter(name => name !== from);
  setSelectOptions("transferTo", safeOptions, preferredValue || q("transferTo")?.value || safeOptions[0] || "", false);
}

function refreshScopedSelectors() {
  const scope = currentScopePharmacy();
  const isAdmin = APP.currentRole === "ADMIN";
  [q("dashboardScopeChip"), q("controlPharmacy")].forEach(el => { if (el) el.textContent = scope; });

  const inventoryOptions = isAdmin ? PHARMACIES : [scope];
  setSelectOptions("inventoryLocationFilter", inventoryOptions, q("inventoryLocationFilter")?.value || scope, !isAdmin);

  const reportOptions = isAdmin
    ? [{ value: "ALL_WORK_PHARMACIES", label: "All Pharmacies" }, ...WORK_PHARMACIES.map(name => ({ value: name, label: name }))]
    : [{ value: scope, label: scope }];
  setSelectOptions("reportPharmacy", reportOptions, q("reportPharmacy")?.value || (isAdmin ? "ALL_WORK_PHARMACIES" : scope), false);

  const auditOptions = isAdmin ? WORK_PHARMACIES : [scope];
  setSelectOptions("auditPharmacy", auditOptions, q("auditPharmacy")?.value || scope, !isAdmin);

  const shipmentOptions = isAdmin ? PHARMACIES : [scope];
  setSelectOptions("shipmentLocation", shipmentOptions, q("shipmentLocation")?.value || scope, !isAdmin && shipmentOptions.length === 1);

  const transferAllowed = isAdmin || canCurrentUserTransfer();
  const transferFromOptions = transferAllowed ? (isAdmin ? PHARMACIES : [scope]) : [scope];
  setSelectOptions("transferFrom", transferFromOptions, q("transferFrom")?.value || scope, !isAdmin && transferFromOptions.length === 1);
  syncTransferToOptions(q("transferTo")?.value || (isAdmin ? PHARMACIES.find(name => name !== (q("transferFrom")?.value || scope)) : PHARMACIES.find(name => name !== scope)));
}


async function bootstrapIfNeeded() {
  const marker = await getDoc(doc(db, "app_meta", "bootstrap"));
  if (marker.exists()) return;

  showActionModal("First Setup", "Preparing Google Sheets data...");
  const defaultHash = await sha256(DEFAULT_PASSWORD);

  const seedUsers = [
    { id: "admin_main", userName: "Admin", employeeNumber: "1000", passwordHash: defaultHash, mustChangePassword: true, active: true, userRoles: ["ADMIN"], role: "ADMIN", allowedPharmacies: WORK_PHARMACIES, canAudit: true, canManageNarcotic: true },
    { id: "user_inpatient", userName: "In-Patient User", employeeNumber: "2001", passwordHash: defaultHash, mustChangePassword: true, active: true, userRoles: ["IN_PATIENT_USER"], role: "IN_PATIENT_USER", allowedPharmacies: ["In-Patient Pharmacy"], canAudit: false, canManageNarcotic: false },
    { id: "user_outpatient", userName: "Out-Patient User", employeeNumber: "2002", passwordHash: defaultHash, mustChangePassword: true, active: true, userRoles: ["OUT_PATIENT_USER"], role: "OUT_PATIENT_USER", allowedPharmacies: ["Out-Patient Pharmacy"], canAudit: false, canManageNarcotic: false },
    { id: "user_medical", userName: "Medical Center User", employeeNumber: "2003", passwordHash: defaultHash, mustChangePassword: true, active: true, userRoles: ["MEDICAL_CENTER_USER"], role: "MEDICAL_CENTER_USER", allowedPharmacies: ["Medical Center Pharmacy"], canAudit: false, canManageNarcotic: false }
  ];

  for (const row of seedUsers) {
    await setDoc(doc(db, "users", row.id), {
      ...row,
      createdAt: serverTimestamp(),
      assigned: true,
      updatedAt: serverTimestamp()
    });
  }

  await setDoc(doc(db, "app_settings", "main"), {
    pharmacyType: "In-Patient Pharmacy",
    month: MONTHS[new Date().getMonth()],
    year: new Date().getFullYear(),
    updatedAt: serverTimestamp()
  });


  const drugs = [
    {id:"d1", scientificName:"Gabapentin", tradeName:"Gabatex", category:"Gabapentinoids", strength:"100mg", dosageForm:"Tablet", unitsPerBox:30, reorderLevelUnits:30, active:true},
    {id:"d2", scientificName:"Gabapentin", tradeName:"Gabanet", category:"Gabapentinoids", strength:"300mg", dosageForm:"Capsule", unitsPerBox:30, reorderLevelUnits:30, active:true},
    {id:"d3", scientificName:"Pregabalin", tradeName:"Galica", category:"Gabapentinoids", strength:"75mg", dosageForm:"Capsule", unitsPerBox:28, reorderLevelUnits:28, active:true},
    {id:"d4", scientificName:"Tramadol", tradeName:"Tramal", category:"Controlled", strength:"100mg", dosageForm:"Tablet", unitsPerBox:30, reorderLevelUnits:30, active:true}
  ];

  for (const drug of drugs) {
    await setDoc(doc(db, "drugs", drug.id), { ...drug, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });
    for (const pharmacy of PHARMACIES) {
      const seedBoxes = pharmacy === "General Hospital Stock" ? 4 : pharmacy === "In-Patient Pharmacy" ? 3 : 1;
      const stock = normalizeInventory(seedBoxes, 0, drug.unitsPerBox);
      await setDoc(doc(db, "inventory", `${drug.id}__${pharmacy.replace(/\s+/g, "_")}`), {
        id: `${drug.id}__${pharmacy.replace(/\s+/g, "_")}`,
        drugId: drug.id,
        pharmacy,
        ...stock,
        updatedAt: serverTimestamp()
      });
    }
  }


  const narcoticSeedMarker = await getDoc(doc(db, "app_meta", "narcotic_bootstrap"));
  if (!narcoticSeedMarker.exists()) {
    const narcoticDrugs = [
      { id: "nd1", scientificName: "Morphine", tradeName: "Morphine", strength: "10mg/ml", dosageForm: "Injection", unitsPerBox: 1, active: true },
      { id: "nd2", scientificName: "Fentanyl", tradeName: "Fentanyl", strength: "50mcg/ml", dosageForm: "Injection", unitsPerBox: 1, active: true }
    ];
    const departments = [
      { id: "icu", name: "ICU", sortOrder: 1, active: true, notes: "" },
      { id: "er", name: "ER", sortOrder: 2, active: true, notes: "" },
      { id: "ward_a", name: "Ward A", sortOrder: 3, active: true, notes: "" }
    ];
    for (const item of narcoticDrugs) {
      await setDoc(doc(db, "narcotic_drugs", item.id), { ...item, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });
      await setDoc(doc(db, "narcotic_internal_stock", item.id), {
        id: item.id,
        drugId: item.id,
        drugName: item.tradeName,
        availableStockUnits: item.id === "nd1" ? 120 : 80,
        reorderLevelUnits: item.id === "nd1" ? 20 : 15,
        pharmacy: "In-Patient Pharmacy",
        updatedAt: serverTimestamp()
      });
    }
    for (const dept of departments) {
      await setDoc(doc(db, "narcotic_departments", dept.id), { departmentName: dept.name, sortOrder: dept.sortOrder, active: dept.active, notes: dept.notes, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });
      for (const item of narcoticDrugs) {
        await setDoc(doc(db, "narcotic_department_stock", `${dept.id}__${item.id}`), {
          departmentId: dept.id,
          departmentName: dept.name,
          drugId: item.id,
          drugName: item.tradeName,
          fixedStockUnits: dept.id === "icu" && item.id === "nd1" ? 20 : 10,
          availableStockUnits: dept.id === "icu" && item.id === "nd1" ? 20 : 10,
          updatedAt: serverTimestamp()
        });
      }
    }
    await setDoc(doc(db, "app_meta", "narcotic_bootstrap"), { createdAt: serverTimestamp() });
  }


  await setDoc(doc(db, "app_meta", "bootstrap"), { createdAt: serverTimestamp() });
  finishActionModal(true, "Google Sheets data prepared.");
}

function bindListeners() {
  APP.listeners.forEach(unsub => unsub && unsub());
  APP.listeners = [];

  APP.listeners.push(onSnapshot(query(collection(db, "drugs"), where("active", "==", true)), snap => {
    APP.cache.drugs = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a, b) => String(a.tradeName || "").localeCompare(String(b.tradeName || "")));
    renderStaticOptions();
    renderAll();
  }));

  APP.listeners.push(onSnapshot(collection(db, "inventory"), snap => {
    APP.cache.inventory = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    renderAll();
  }));

  APP.listeners.push(onSnapshot(collection(db, "prescriptions"), snap => {
    APP.cache.prescriptions = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
    renderStaticOptions();
    renderAll();
  }));

  APP.listeners.push(onSnapshot(collection(db, "transactions"), snap => {
    APP.cache.transactions = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
    renderAll();
  }));

  APP.listeners.push(onSnapshot(collection(db, "prescription_doses"), snap => {
    APP.cache.prescriptionDoses = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
    renderAll();
  }));

  APP.listeners.push(onSnapshot(collection(db, "users"), snap => {
    APP.cache.users = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    APP.cache.pharmacists = activeUserRows().map(u => ({
      id: u.id,
      name: u.userName || u.fullName || u.displayName || "",
      jobNumber: u.employeeNumber || "",
      workplace: getUserAllowedPharmacies(u)[0] || "",
      pharmacies: getUserAllowedPharmacies(u),
      canAudit: !!u.canAudit,
      canManageNarcotic: !!u.canManageNarcotic,
      canTransfer: !!u.canTransfer,
      active: u.active !== false
    }));
    renderStaticOptions();
    renderAll();
  }));


  APP.listeners.push(onSnapshot(query(collection(db, "narcotic_drugs"), where("active", "==", true)), snap => {
    APP.cache.narcoticDrugs = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a,b) => String(a.tradeName || "").localeCompare(String(b.tradeName || "")));
    renderNarcoticStaticOptions();
    renderNarcoticPage();
  }));

  APP.listeners.push(onSnapshot(collection(db, "narcotic_departments"), snap => {
    APP.cache.narcoticDepartments = snap.docs.map(d => ({ id: d.id, ...d.data() })).filter(r => r.active !== false).sort((a,b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0));
    renderNarcoticStaticOptions();
    renderNarcoticPage();
  }));

  APP.listeners.push(onSnapshot(collection(db, "narcotic_department_stock"), snap => {
    APP.cache.narcoticDepartmentStock = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    renderNarcoticPage();
  }));

  APP.listeners.push(onSnapshot(collection(db, "narcotic_prescriptions"), snap => {
    APP.cache.narcoticPrescriptions = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a,b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
    renderNarcoticPage();
  }));

  APP.listeners.push(onSnapshot(collection(db, "narcotic_order_movements"), snap => {
    APP.cache.narcoticOrderMovements = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a,b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
    renderNarcoticPage();
  }));

  APP.listeners.push(onSnapshot(collection(db, "narcotic_internal_stock"), snap => {
    APP.cache.narcoticInternalStock = snap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a,b) => String(a.drugName || "").localeCompare(String(b.drugName || "")));
    renderNarcoticPage();
  }));

  if (APP.currentRole === "ADMIN") {
    APP.listeners.push(onSnapshot(collection(db, "archive_runs"), snap => {
      const runs = snap.docs.map(d => ({ id: d.id, ...d.data() }))
        .sort((a, b) => String(getArchiveRunCreatedAt(b) || "").localeCompare(String(getArchiveRunCreatedAt(a) || "")));
      APP.archiveStatus = runs[0] || null;
      renderDashboard();
      renderSettings();
    }));
  }

  APP.listeners.push(onSnapshot(doc(db, "app_settings", "main"), snap => {
    APP.cache.settings = snap.exists() ? snap.data() : {};
    renderStaticOptions();
    renderAll();
  }));
}

function applyRoleUI() {
  const isAdmin = APP.currentRole === "ADMIN";
  const canAudit = canCurrentUserAudit();
  const canTransfer = canCurrentUserTransfer();
  const canSeeNarcotic = isAdmin || APP.currentPortal === "IN_PATIENT_USER";
  document.querySelectorAll(".admin-only").forEach(el => {
    if (el.dataset.page === "audit") return;
    el.classList.toggle("hidden", !isAdmin);
  });
  document.querySelectorAll(".admin-only-block").forEach(el => {
    if (el.id === "page-shipments") {
      el.classList.toggle("hidden", !(isAdmin || canTransfer));
    } else {
      el.classList.toggle("hidden", !isAdmin);
    }
  });
  document.querySelectorAll(".narcotic-nav-link").forEach(el => el.classList.toggle("hidden", !canSeeNarcotic));
  document.querySelectorAll(".narcotic-admin-tab").forEach(el => el.classList.toggle("hidden", !isAdmin));
  document.querySelectorAll(".narcotic-admin-tab-content").forEach(el => el.classList.toggle("hidden", !isAdmin));
  document.querySelectorAll('[data-page="audit"]').forEach(el => el.classList.toggle("hidden", !canAudit));
  document.querySelectorAll(".audit-only").forEach(el => el.classList.toggle("hidden", !canAudit));
  if (q("page-audit")) q("page-audit").classList.toggle("hidden", q("page-audit").classList.contains("hidden") ? true : !canAudit && !q("page-audit").classList.contains("hidden"));
  if (!isAdmin && APP.narcoticTab !== "dispensing") APP.narcoticTab = "dispensing";
  [q("openTransferModalBtn"), q("openTransferModalBtn2")].forEach(el => { if (el) el.classList.toggle("hidden", !(isAdmin || canTransfer)); });
  [q("openShipmentModalBtn"), q("openShipmentModalBtn2"), q("openAddDrugModalBtn"), q("openAddDrugModalBtn2")].forEach(el => { if (el) el.classList.toggle("hidden", !isAdmin); });
  document.querySelectorAll('[data-page="shipments"]').forEach(el => el.classList.toggle("hidden", !(isAdmin || canTransfer)));
  document.body.classList.toggle("allow-narcotic", canSeeNarcotic);
}


async function tryRestoreSession() {
  const userId = sessionStorage.getItem("cdms_session_user_id");
  const portal = sessionStorage.getItem("cdms_session_portal");
  const scope = sessionStorage.getItem("cdms_session_scope");
  if (!userId || !portal) return;
  const snap = await getDoc(doc(db, "users", userId));
  if (!snap.exists()) return;
  const user = snap.data();
  APP.currentUserDocId = userId;
  APP.currentUser = { ...user, pharmacyScope: getUserAllowedPharmacies(user) };
  APP.currentRole = portal === "ADMIN" ? "ADMIN" : portal;
  APP.currentPortal = portal;
  APP.currentPharmacyScope = scope || (APP.currentRole === "ADMIN" ? (APP.cache.settings.pharmacyType || "In-Patient Pharmacy") : (portalToScope(portal)));
  APP.loginMethod = sessionStorage.getItem("cdms_session_login_method") || "NORMAL";
  bindListeners();
  applyRoleUI();
  q("loginScreen").classList.add("hidden");
  q("appShell").classList.remove("hidden");
  updateLayoutMode();
  showPage("dashboard");
  startRealtimeSyncEnhancements();
  if (APP.currentRole === "ADMIN") loadArchiveStatus();
  queueAutomaticArchiveCheck();
}

function renderStaticOptions() {
  const drugOptions = APP.cache.drugs.map(d => `<option value="${esc(d.id)}">${esc(d.tradeName)} ${esc(d.strength)}</option>`).join("");
  ["quickDrug","shipmentDrug","transferDrug","reportDrug"].forEach(id => {
    if (q(id)) q(id).innerHTML = `<option value="">Select Drug</option>${drugOptions}`;
  });
  filterQuickDrugOptions();

  const scope = currentScopePharmacy();
  applyCurrentUserReadonlyFields();
  q("doctorList").innerHTML = [...new Set(APP.cache.prescriptions.map(p => p.doctorName).filter(Boolean))].sort().map(name => `<option value="${esc(name)}"></option>`).join("");

  const pharmacyOptions = PHARMACIES.map(name => `<option>${esc(name)}</option>`).join("");
  ["shipmentLocation","transferFrom","transferTo","inventoryLocationFilter","reportPharmacy"].forEach(id => {
    if (q(id)) q(id).innerHTML = pharmacyOptions;
  });

  if (q("settingsPharmacy")) q("settingsPharmacy").innerHTML = WORK_PHARMACIES.map(name => `<option>${esc(name)}</option>`).join("");
  const pharmacyCheckboxesMarkup = WORK_PHARMACIES.map(name => `<label class="checkbox-item"><input type="checkbox" class="user-pharmacy-checkbox" value="${esc(name)}"> <span>${esc(name)}</span></label>`).join("");
  if (q("userPharmacyCheckboxes")) q("userPharmacyCheckboxes").innerHTML = pharmacyCheckboxesMarkup;
  if (q("userPharmacyCheckboxesModal")) q("userPharmacyCheckboxesModal").innerHTML = pharmacyCheckboxesMarkup;
  const rolesMarkup = [
    { value: "ADMIN", label: "Admin" },
    { value: "IN_PATIENT_USER", label: "In-Patient Portal" },
    { value: "OUT_PATIENT_USER", label: "Out-Patient Portal" },
    { value: "MEDICAL_CENTER_USER", label: "Medical Center Portal" }
  ].map(item => `<label class="checkbox-item"><input type="checkbox" class="user-role-checkbox" value="${esc(item.value)}"> <span>${esc(item.label)}</span></label>`).join("");
  if (q("userRoleCheckboxes")) q("userRoleCheckboxes").innerHTML = rolesMarkup;
  if (q("userRoleCheckboxesModal")) q("userRoleCheckboxesModal").innerHTML = rolesMarkup;
  if (q("settingsMonth")) q("settingsMonth").innerHTML = MONTHS.map(name => `<option>${esc(name)}</option>`).join("");

  const patientDrugOptions = APP.cache.drugs.map(d => `<option value="${esc(d.id)}">${esc(d.tradeName)} ${esc(d.strength)}</option>`).join("");
  if (q("patientsDrugFilter")) q("patientsDrugFilter").innerHTML = `<option value="">All Drugs</option>${patientDrugOptions}`;
  if (q("patientsPharmacyFilter")) q("patientsPharmacyFilter").innerHTML = `<option value="">All Pharmacies</option>${WORK_PHARMACIES.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("")}`;
  if (q("prescriptionsDrugFilter")) q("prescriptionsDrugFilter").innerHTML = `<option value="">All Drugs</option>${patientDrugOptions}`;
  populatePrescriptionLabelModalOptions();
  if (q("prescriptionsPharmacyFilter")) {
    const pharmacyOptions = APP.currentRole === "ADMIN" ? WORK_PHARMACIES : [currentScopePharmacy()];
    q("prescriptionsPharmacyFilter").innerHTML = `<option value="">${APP.currentRole === "ADMIN" ? "All Pharmacies" : "Current Pharmacy"}</option>${pharmacyOptions.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("")}`;
  }

  refreshScopedSelectors();
  updateQuickAvailableStock();
  renderNarcoticStaticOptions();
  renderLiveClocks();
  applyCurrentUserReadonlyFields();
}

function renderAll() {
  if (!APP.currentUser) return;
  refreshScopedSelectors();
  renderControlPanel();
  renderDashboard();
  renderInventory();
  renderTransactions();
  renderAudit();
  renderSettings();
  renderDrugRows();
  renderPatientsPage();
  renderPrescriptions();
  renderNarcoticPage();
  updateQuickAvailableStock();
  updateNarcoticAvailableStock();
  renderNarcoticStaticOptions();
  renderLiveClocks();
}


function updateLayoutMode() {
  document.body.classList.toggle("app-authenticated", !q("appShell")?.classList.contains("hidden"));
}

function updateSidebarIdentity() {
  const btn = q("userMenuBtn");
  if (!btn) return;
  const name = currentActorName() || "Account";
  const role = (APP.currentPortal || APP.currentRole || "").replaceAll("_", " ") || "Open menu";
  btn.innerHTML = `
    <span class="sidebar-user-avatar">👤</span>
    <span class="sidebar-user-meta">
      <strong>${esc(name)}</strong>
      <small>${esc(role)}</small>
    </span>
    <span class="sidebar-user-caret">▾</span>`;
}

function renderControlPanel() {
  q("controlUser").textContent = currentActorName() || "-";
  q("controlRole").textContent = (APP.currentPortal || APP.currentRole || "").replaceAll("_", " ");
  q("controlPharmacy").textContent = currentScopePharmacy();
  updateSidebarIdentity();
}


function filterQuickDrugOptions() {
  if (!q("quickDrug")) return;
  const term = (q("quickDrugSearch")?.value || "").toLowerCase().trim();
  const options = [...q("quickDrug").options];
  let firstMatch = "";
  options.forEach((opt, index) => {
    if (index === 0) {
      opt.hidden = false;
      return;
    }
    const show = !term || opt.text.toLowerCase().includes(term);
    opt.hidden = !show;
    if (show && !firstMatch) firstMatch = opt.value;
  });
  if (term && firstMatch && (!q("quickDrug").value || q("quickDrug").selectedOptions[0]?.hidden)) {
    q("quickDrug").value = firstMatch;
    updateQuickAvailableStock();
  }
}

function getPagedDrugCards(drugs) {
  const pageSize = 8;
  const totalPages = Math.max(1, Math.ceil(drugs.length / pageSize));
  APP.drugCardsPage = Math.min(totalPages, Math.max(1, APP.drugCardsPage || 1));
  const start = (APP.drugCardsPage - 1) * pageSize;
  return {
    totalPages,
    pageItems: drugs.slice(start, start + pageSize)
  };
}

function buildBarChart(containerId, items, monthly = false) {
  const container = q(containerId);
  if (!container) return;
  container.classList.toggle("monthly", monthly);
  const maxValue = Math.max(1, ...items.map(item => Number(item.value || 0)));
  container.innerHTML = items.map(item => {
    const height = Math.max(8, Math.round((Number(item.value || 0) / maxValue) * 160));
    return `
      <div class="chart-bar-col">
        <div class="chart-value">${Number(item.value || 0)}</div>
        <div class="chart-bar" style="height:${height}px"></div>
        <div class="chart-label">${esc(item.label)}</div>
      </div>`;
  }).join("");
}

function renderDashboardCharts(scopedPrescriptions) {
  const now = new Date();
  const daily = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const dayKey = d.toLocaleDateString("en-CA", { timeZone: JORDAN_TZ });
    daily.push({
      label: d.toLocaleDateString("en-US", { timeZone: JORDAN_TZ, month: "short", day: "numeric" }),
      value: scopedPrescriptions.filter(row => formatJordanDateTime(row.dateTime).slice(0, 10) === dayKey).length
    });
  }
  buildBarChart("dailyDispenseChart", daily, false);

  const monthly = [];
  for (let i = 11; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    monthly.push({
      label: d.toLocaleDateString("en-US", { month: "short" }),
      value: scopedPrescriptions.filter(row => formatJordanDateTime(row.dateTime).slice(0, 7) === key).length
    });
  }
  buildBarChart("monthlyDispenseChart", monthly, true);
}

function renderRecentPrescriptionsModal() {
  const term = (q("recentPrescriptionsSearch")?.value || "").toLowerCase().trim();
  const from = q("recentPrescriptionsFrom")?.value || "";
  const to = q("recentPrescriptionsTo")?.value || "";
  const effectiveFrom = from || (!to ? jordanDateKey() : "");
  const effectiveTo = to || (!from ? jordanDateKey() : "");
  const rows = getScopedPrescriptionRows(getArchiveMode('dashboard'))
    .filter(row => {
      const hay = `${row.patientName} ${row.fileNumber} ${row.doctorName} ${row.pharmacistName}`.toLowerCase();
      const drugHay = `${APP.cache.drugs.find(d => d.id === row.drugId)?.tradeName || ""} ${APP.cache.drugs.find(d => d.id === row.drugId)?.strength || ""}`.toLowerCase();
      const day = formatJordanDateTime(row.dateTime).slice(0, 10);
      if (term && !hay.includes(term) && !drugHay.includes(term)) return false;
      if (effectiveFrom && day < effectiveFrom) return false;
      if (effectiveTo && day > effectiveTo) return false;
      return true;
    })
    .slice(0, 50);

  q("recentPrescriptionsTbody").innerHTML = rows.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    return `
      <tr>
        <td>${esc(formatJordanDateTime(row.dateTime))}</td>
        <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td>
        <td>${esc(row.patientName || "")}</td>
        <td>${esc(row.fileNumber || "")}</td>
        <td>${esc(row.prescriptionType || "")}</td>
        <td>${esc(row.pharmacy || "")}</td>
        <td>${Number(row.qtyBoxes || 0)}</td>
        <td>${Number(row.qtyUnits || 0)}</td>
        <td>${statusBadge(row.status || "Registered")}</td>
        <td class="rx-actions-cell">${isArchivedRow(row) ? '<span class="muted">Archived</span>' : buildPrescriptionActionsDropdown(row, { prefix: "recent" })}</td>
      </tr>`;
  }).join("") || `<tr><td colspan="10" class="empty-state">No recent prescriptions found.</td></tr>`;
}

function openRecentPrescriptionsModal() {
  renderRecentPrescriptionsModal();
  openModal("recentPrescriptionsModal");
}

function getFilteredTransactionsRows() {
  const term = (q("transactionsSearch")?.value || "").toLowerCase().trim();
  const type = q("transactionsTypeFilter")?.value || "";
  const from = q("transactionsFromDate")?.value || "";
  const to = q("transactionsToDate")?.value || "";
  const effectiveFrom = from || (!to ? jordanDateKey() : "");
  const effectiveTo = to || (!from ? jordanDateKey() : "");

  return transactionScopeRows().filter(row => {
    const haystack = `${row.type} ${row.tradeName} ${row.pharmacy} ${row.performedBy} ${row.note} ${row.receiverPharmacist || ""} ${row.receivedBy || ""} ${row.invoiceNumber || ""} ${row.invoiceDate || ""} ${row.patientName || ""} ${row.fileNumber || ""} ${row.doctorName || ""}`.toLowerCase();
    const day = formatJordanDateTime(row.dateTime).slice(0, 10);
    const normalizedType = String(row.type || "");
    const matchesRegisterType = !type || (type === "Register" ? ["Register", "Dispense"].includes(normalizedType) : normalizedType === type);
    if (term && !haystack.includes(term)) return false;
    if (!matchesRegisterType) return false;
    if (effectiveFrom && day < effectiveFrom) return false;
    if (effectiveTo && day > effectiveTo) return false;
    return true;
  });
}


function txQtyText(boxes, units, unitWord = "unit(s)") {
  return `${Number(boxes || 0)} box(es) + ${Number(units || 0)} ${unitWord}`;
}

function txSignatureBlock(leftLabel, leftName, rightLabel, rightName) {
  return `
    <div class="tx-signature-row">
      <div class="tx-signature-box"><span>${esc(leftLabel || "Signature")}</span><strong>${esc(leftName || "-")}</strong><div class="tx-sign-line"></div></div>
      <div class="tx-signature-box"><span>${esc(rightLabel || "Signature")}</span><strong>${esc(rightName || "-")}</strong><div class="tx-sign-line"></div></div>
    </div>`;
}

function txTableSection(title, columns, rowsHtml, emptyText, signatures = "") {
  return `
    <div class="tx-print-section">
      <div class="section-title">${title}</div>
      <div class="section">
        <table>
          <thead><tr>${columns.map(col => `<th>${col}</th>`).join("")}</tr></thead>
          <tbody>${rowsHtml || `<tr><td colspan="${columns.length}">${emptyText}</td></tr>`}</tbody>
        </table>
        ${signatures}
      </div>
    </div>`;
}

function buildNormalTransactionPrintSection(type, rows) {
  const safeRows = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if (type === "register") {
    return txTableSection(
      "Register / Dispense",
      ["Patient Name", "File Number", "Pharmacy", "Doctor", "Dispensed Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.patientName || "-")}</td>
          <td>${esc(row.fileNumber || "-")}</td>
          <td>${esc(row.pharmacy || "-")}</td>
          <td>${esc(row.doctorName || "-")}</td>
          <td>${txQtyText(row.qtyBoxes, row.qtyUnits)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Pharmacist:</strong> ${esc(row.registeredBy || row.pharmacistName || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.registeredDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No dispensing transactions found."
    );
  }
  if (type === "edit") {
    return txTableSection(
      "Edit Prescription",
      ["Stage", "Patient Name", "File Number", "Pharmacy", "Doctor", "Quantity"],
      safeRows.map(row => {
        const oldValues = row.oldValues || {};
        const newValues = row.newValues || {};
        const pharmacy = row.pharmacy || newValues.pharmacy || oldValues.pharmacy || "-";
        return `
          <tr>
            <td><strong>Before Edit</strong></td>
            <td>${esc(oldValues.patientName || row.patientName || "-")}</td>
            <td>${esc(oldValues.fileNumber || row.fileNumber || "-")}</td>
            <td>${esc(pharmacy)}</td>
            <td>${esc(oldValues.doctorName || row.doctorName || "-")}</td>
            <td>${txQtyText(oldValues.qtyBoxes, oldValues.qtyUnits)}</td>
          </tr>
          <tr>
            <td><strong>After Edit</strong></td>
            <td>${esc(newValues.patientName || row.patientName || "-")}</td>
            <td>${esc(newValues.fileNumber || row.fileNumber || "-")}</td>
            <td>${esc(pharmacy)}</td>
            <td>${esc(newValues.doctorName || row.doctorName || "-")}</td>
            <td>${txQtyText(newValues.qtyBoxes, newValues.qtyUnits)}</td>
          </tr>
          <tr class="tx-meta-row">
            <td colspan="6"><strong>Edited by:</strong> ${esc(row.editedBy || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.editedDateTime || row.dateTime || ""))}</td>
          </tr>
        `;
      }).join(""),
      "No edited prescription transactions found."
    );
  }
  if (type == "delete") {
    return txTableSection(
      "Delete Prescription",
      ["Patient Name", "File Number", "Pharmacy", "Doctor", "Deleted Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.patientName || "-")}</td>
          <td>${esc(row.fileNumber || "-")}</td>
          <td>${esc(row.pharmacy || "-")}</td>
          <td>${esc(row.doctorName || "-")}</td>
          <td>${txQtyText(row.qtyBoxes, row.qtyUnits)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Deleted by:</strong> ${esc(row.deletedBy || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.deletedDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No deleted prescription transactions found."
    );
  }
  if (type == "return") {
    return txTableSection(
      "Return",
      ["Patient Name", "File Number", "Pharmacy", "Doctor", "Returned Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.patientName || "-")}</td>
          <td>${esc(row.fileNumber || "-")}</td>
          <td>${esc(row.pharmacy || "-")}</td>
          <td>${esc(row.doctorName || "-")}</td>
          <td>${txQtyText(row.qtyBoxes, row.qtyUnits)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Returned by:</strong> ${esc(row.returnBy || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.returnDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No returned prescription transactions found."
    );
  }
  if (type == "shipment") {
    return txTableSection(
      "Receive Shipment",
      ["Pharmacy", "Drug", "Invoice Number", "Invoice Date", "Received Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.pharmacy || "-")}</td>
          <td>${esc(drugDisplayLabel(row.drugId, row.tradeName || "-"))}</td>
          <td>${esc(row.invoiceNumber || "-")}</td>
          <td>${esc(row.invoiceDate || "-")}</td>
          <td>${txQtyText(row.qtyBoxes, row.qtyUnits)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Received by:</strong> ${esc(row.receivedBy || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.receivedDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No shipment receipt transactions found."
    );
  }
  if (type == "transfer") {
    return txTableSection(
      "Transfer",
      ["From Pharmacy", "To Pharmacy", "Drug", "Transferred Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.fromPharmacy || String(row.pharmacy || "").split("→")[0]?.trim() || "-")}</td>
          <td>${esc(row.toPharmacy || String(row.pharmacy || "").split("→")[1]?.trim() || "-")}</td>
          <td>${esc(drugDisplayLabel(row.drugId, row.tradeName || "-"))}</td>
          <td>${txQtyText(row.qtyBoxes, row.qtyUnits)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="4"><strong>Transferred by:</strong> ${esc(row.performedBy || "-")} &nbsp; | &nbsp; <strong>Received by:</strong> ${esc(row.receiverPharmacist || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.transferredDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No stock transfer transactions found.",
      txSignatureBlock("Transferred By Signature", safeRows[0]?.performedBy || "-", "Received By Signature", safeRows[0]?.receiverPharmacist || "-")
    );
  }
  return "";
}

function buildNarcoticTransactionPrintSection(type, rows) {
  const safeRows = Array.isArray(rows) ? rows.filter(Boolean) : [];
  if (type === "register") {
    return txTableSection(
      "Narcotic Register / Dispense",
      ["Patient Name", "File Number", "Department", "Drug", "Dispensed Units"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.patientName || "-")}</td>
          <td>${esc(row.fileNumber || "-")}</td>
          <td>${esc(row.departmentName || "-")}</td>
          <td>${esc(row.drugName || "-")}</td>
          <td>${Number(row.dispensedUnits || row.quantitySent || 0)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Pharmacist:</strong> ${esc(row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No narcotic dispensing records found."
    );
  }
  if (type === "delete") {
    return txTableSection(
      "Narcotic Delete Prescription",
      ["Patient Name", "File Number", "Department", "Drug", "Deleted Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.patientName || "-")}</td>
          <td>${esc(row.fileNumber || "-")}</td>
          <td>${esc(row.departmentName || "-")}</td>
          <td>${esc(row.drugName || "-")}</td>
          <td>${Number(row.quantitySent || row.dispensedUnits || 0)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Deleted by:</strong> ${esc(row.deletedBy || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.deletedDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No deleted narcotic prescriptions found."
    );
  }
  if (type === "return") {
    return txTableSection(
      "Narcotic Return",
      ["Patient Name", "File Number", "Department", "Drug", "Returned Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.patientName || "-")}</td>
          <td>${esc(row.fileNumber || "-")}</td>
          <td>${esc(row.departmentName || "-")}</td>
          <td>${esc(row.drugName || "-")}</td>
          <td>${Number(row.quantitySent || row.dispensedUnits || 0)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="5"><strong>Returned by:</strong> ${esc(row.returnBy || row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.returnDateTime || row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No returned narcotic prescriptions found."
    );
  }
  if (type === "receipt") {
    return txTableSection(
      "Narcotic Receive Shipment",
      ["Drug", "Invoice Number", "Invoice Date", "Received Quantity"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.drugName || "-")}</td>
          <td>${esc(row.invoiceNumber || "-")}</td>
          <td>${esc(row.invoiceDate || "-")}</td>
          <td>${Number(row.quantityReceived || 0)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="4"><strong>Received by:</strong> ${esc(row.performedBy || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No internal narcotic receipts found."
    );
  }
  if (type === "transfer") {
    return txTableSection(
      "Narcotic Transfer / Department Order",
      ["Department", "Drug", "Quantity Sent", "Empty Ampoules"],
      safeRows.map(row => `
        <tr>
          <td>${esc(row.departmentName || "-")}</td>
          <td>${esc(row.drugName || "-")}</td>
          <td>${Number(row.quantitySent || 0)}</td>
          <td>${Number(row.emptyAmpoulesReceived || 0)}</td>
        </tr>
        <tr class="tx-meta-row">
          <td colspan="4"><strong>Pharmacist:</strong> ${esc(row.performedBy || "-")} &nbsp; | &nbsp; <strong>Nurse:</strong> ${esc(row.nurseName || "-")} &nbsp; | &nbsp; <strong>Date & Time:</strong> ${esc(formatJordanDateTime(row.dateTime || ""))}</td>
        </tr>
      `).join(""),
      "No department order movements found.",
      txSignatureBlock("Pharmacist Signature", safeRows[0]?.performedBy || "-", "Nurse Signature", safeRows[0]?.nurseName || "-")
    );
  }
  return "";
}

function printTransactionsPage() {
  if (APP.currentRole !== "ADMIN") {
    showActionModal("Print Transactions", "Only Admin can print transaction reports.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  const rows = getFilteredTransactionsRows().slice().sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
  const registerRows = rows.filter(row => ["Register", "Dispense"].includes(String(row.type || "")));
  const editRows = rows.filter(row => row.type === "Edit Prescription");
  const deleteRows = rows.filter(row => row.type === "Delete Prescription");
  const returnRows = rows.filter(row => row.type === "Return");
  const shipmentRows = rows.filter(row => row.type === "Receive Shipment");
  const transferRows = rows.filter(row => row.type === "Transfer");

  const body = `
    <div class="section-title">Transactions Report</div>
    <div class="sub"><strong>Date Filter:</strong> ${esc((q("transactionsFromDate")?.value || jordanDateKey()))} to ${esc((q("transactionsToDate")?.value || jordanDateKey()))} &nbsp; | &nbsp; <strong>Total Rows:</strong> ${rows.length}</div>
    ${buildNormalTransactionPrintSection("register", registerRows)}
    ${buildNormalTransactionPrintSection("edit", editRows)}
    ${buildNormalTransactionPrintSection("delete", deleteRows)}
    ${buildNormalTransactionPrintSection("return", returnRows)}
    ${buildNormalTransactionPrintSection("shipment", shipmentRows)}
    ${buildNormalTransactionPrintSection("transfer", transferRows)}
  `;

  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Transactions Report", currentScopePharmacy(), body));
  w.document.close();
}

function printInventoryPage() {
  const term = (q("inventorySearch")?.value || "").toLowerCase();
  const location = q("inventoryLocationFilter").value || currentScopePharmacy();
  const rows = APP.cache.drugs.filter(drug => `${drug.scientificName} ${drug.tradeName} ${drug.category} ${drug.strength} ${drug.dosageForm}`.toLowerCase().includes(term));
  const body = `
    <div class="section-title">Inventory</div>
    <div class="sub"><strong>Location:</strong> ${esc(location)}</div>
    <div class="section">
      <table>
        <thead><tr><th>Scientific Name</th><th>Trade Name</th><th>Category</th><th>Strength</th><th>Dosage Form</th><th>Units / Box</th><th>Available</th></tr></thead>
        <tbody>
          ${rows.map(drug => {
            const inv = invRow(drug.id, location) || { boxes: 0, units: 0, totalUnits: 0 };
            return `<tr>
              <td>${esc(drug.scientificName || "")}</td>
              <td>${esc(drug.tradeName || "")}</td>
              <td>${esc(drug.category || "")}</td>
              <td>${esc(drug.strength || "")}</td>
              <td>${esc(drug.dosageForm || "")}</td>
              <td>${Number(drug.unitsPerBox || 0)}</td>
              <td>${esc(formatStock(inv.boxes, inv.units, drug))}</td>
            </tr>`;
          }).join("")}
        </tbody>
      </table>
    </div>`;
  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Inventory Report", location, body));
  w.document.close();
}

function openInventoryAuditModal() {
  if (!canCurrentUserAudit()) return;
  const isAdmin = APP.currentRole === "ADMIN";
  const manualWrap = q("inventoryAuditPharmacyManualWrap");
  const selectWrap = q("inventoryAuditPharmacySelectWrap");
  if (selectWrap) selectWrap.classList.toggle("hidden", !isAdmin);
  if (manualWrap) manualWrap.classList.toggle("hidden", isAdmin);
  if (q("inventoryAuditPharmacy")) {
    q("inventoryAuditPharmacy").innerHTML = PHARMACIES.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
    q("inventoryAuditPharmacy").value = q("inventoryLocationFilter")?.value || currentScopePharmacy();
  }
  if (q("inventoryAuditPharmacyManual")) q("inventoryAuditPharmacyManual").value = currentScopePharmacy();
  if (q("inventoryAuditDateInput")) q("inventoryAuditDateInput").value = jordanDateKey();
  openModal("inventoryAuditModal");
}

function getInventoryAuditSelectedPharmacy() {
  if (APP.currentRole === "ADMIN") return (q("inventoryAuditPharmacy")?.value || currentScopePharmacy()).trim();
  return (q("inventoryAuditPharmacyManual")?.value || currentScopePharmacy()).trim();
}

function buildInventoryAuditMainPage(pharmacyLabel, auditDate) {
  const drugs = sortDrugsAlphabetically(APP.cache.drugs).filter(drug => invRow(drug.id, pharmacyLabel));
  const rows = drugs.length ? drugs : sortDrugsAlphabetically(APP.cache.drugs);
  const tbody = rows.map(drug => {
    const inv = invRow(drug.id, pharmacyLabel) || { boxes: 0, units: 0, totalUnits: 0 };
    return `<tr>
      <td>${esc(`${drug.tradeName || ""} ${drug.strength || ""}`.trim())}</td>
      <td>${Number(drug.unitsPerBox || 0)}</td>
      <td>${esc(formatStock(inv.boxes, inv.units, drug))}</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
    </tr>`;
  }).join("");

  return `
    <div class="section-title">Inventory Audit</div>
    <div class="sub"><strong>Pharmacy:</strong> ${esc(pharmacyLabel)} &nbsp; | &nbsp; <strong>Audit Date:</strong> ${esc(auditDate || "________________")}</div>
    <div class="section">
      <table>
        <thead>
          <tr>
            <th>Drug</th>
            <th>Units / Box</th>
            <th>Current Stock</th>
            <th>Physical Count</th>
            <th>Difference</th>
            <th>Registered Rx</th>
            <th>Returned Rx</th>
            <th>Unrecorded Rx</th>
          </tr>
        </thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>
    <div class="section">
      <table>
        <thead>
          <tr>
            <th>Total Prescriptions</th>
            <th>Registered Prescriptions</th>
            <th>Unrecorded Prescriptions</th>
            <th>Returned Prescriptions</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td style="height:42px"></td>
            <td></td>
            <td></td>
            <td></td>
          </tr>
        </tbody>
      </table>
    </div>
  `;
}

function buildInventoryAuditNotesPage(pharmacyLabel, auditDate) {
  return `
    <div class="head page-break">
      <div>
        <div class="title">Prescriptions Note</div>
        <div class="sub">Jordan Hospital Pharmacy · Controlled Drugs Management</div>
        <div class="sub">${esc(pharmacyLabel)}</div>
      </div>
      <div class="pill">Audit Date: ${esc(auditDate || "________________")}</div>
    </div>
    <div class="section">
      <table>
        <thead>
          <tr>
            <th>Patient Name</th>
            <th>File Number</th>
            <th>Drug Name</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          ${Array.from({ length: 18 }).map(() => `<tr><td style="height:34px"></td><td></td><td></td><td></td></tr>`).join("")}
        </tbody>
      </table>
    </div>
    <div class="section">
      <table>
        <thead>
          <tr>
            <th>Audited Pharmacist Name</th>
            <th>Signature</th>
            <th>Date</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td style="height:52px"></td>
            <td></td>
            <td>${esc(auditDate || "")}</td>
          </tr>
        </tbody>
      </table>
    </div>
  `;
}

function printInventoryAuditReport() {
  if (!canCurrentUserAudit()) return;
  const pharmacyLabel = getInventoryAuditSelectedPharmacy();
  const auditDate = (q("inventoryAuditDateInput")?.value || "").trim();
  if (!pharmacyLabel) {
    showActionModal("Inventory Audit Report", "Please enter or choose the pharmacy first.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  closeModal("inventoryAuditModal");
  const body = `${buildInventoryAuditMainPage(pharmacyLabel, auditDate)}${buildInventoryAuditNotesPage(pharmacyLabel, auditDate)}`;
  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Inventory Audit Report", pharmacyLabel, body, {
    headerPill: `Audit Date: ${auditDate || "________________"}`,
    landscape: true
  }));
  w.document.close();
}


function renderDashboard() {
  ensureArchiveCacheForSection('dashboard');
  const scope = currentScopePharmacy();
  const scopedPrescriptions = prescriptionScopeRows();
  const cardSearch = (q("drugCardsSearch").value || "").toLowerCase();

  const today = jordanDateKey();
  q("metricRegistered").textContent = scopedPrescriptions.filter(p => formatJordanDateTime(p.dateTime).slice(0,10) === today).length;
  q("metricPending").textContent = scopedPrescriptions.filter(p => (p.status || "") === "Pending" && formatJordanDateTime(p.dateTime).slice(0,10) === today).length;
  q("metricReturned").textContent = scopedPrescriptions.filter(p => (p.status || "") === "Returned" && formatJordanDateTime(p.dateTime).slice(0,10) === today).length;

  const dashboardArchiveHost = q("recentList")?.parentElement;
  if (dashboardArchiveHost) mountArchiveToggle(dashboardArchiveHost, 'dashboard', { prepend: true, panelId: 'dashboardArchiveTogglePanel' });

  q("recentList").innerHTML = scopedPrescriptions.slice(0, 7).map(p => {
    const drug = APP.cache.drugs.find(d => d.id === p.drugId);
    const canEdit = p.status !== "Returned" && canEditPrescription(p);
    return `
      <div class="recent-row-layout">
        <div>
          <div class="recent-name">${esc(p.patientName)}</div>
          <div class="subline">${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))} • ${esc(p.fileNumber || "")}</div>
        </div>
        <div class="subline">${esc(formatJordanDateTime(p.dateTime))}</div>
        <div class="subline">${Number(p.qtyBoxes || 0)} box(es) + ${Number(p.qtyUnits || 0)} ${esc(unitLabel(drug))}</div>
        <div class="recent-row-actions">
          ${isArchivedRow(p) ? '<span class="badge pending">Archived</span>' : (p.status === "Returned" ? statusBadge("Returned") : buildPrescriptionActionsDropdown(p, { prefix: "latest" }))}
        </div>
      </div>`;
  }).join("") || `<div class="empty-state">No prescriptions found for ${esc(scope)}.</div>`;

  const filteredDrugs = APP.cache.drugs
    .filter(drug => `${drug.scientificName} ${drug.tradeName} ${drug.category} ${drug.strength} ${drug.dosageForm}`.toLowerCase().includes(cardSearch));
  const { totalPages, pageItems } = getPagedDrugCards(filteredDrugs);
  q("drugCardsPageInfo").textContent = `Page ${APP.drugCardsPage} / ${totalPages}`;
  q("drugCardsPrevBtn").disabled = APP.drugCardsPage <= 1;
  q("drugCardsNextBtn").disabled = APP.drugCardsPage >= totalPages;

  q("drugCards").innerHTML = pageItems.map(drug => {
      const inv = invRow(drug.id, scope) || {};
      return `
        <div class="drug-card" data-drugid="${esc(drug.id)}">
          <div class="drug-title">${esc(drug.tradeName)} ${esc(drug.strength)}</div>
          <div class="drug-meta">${esc(drug.scientificName || "")}</div>
          <div class="drug-meta">${esc(drug.category || "")} • ${esc(drug.dosageForm || "")}</div>
          <div class="drug-stock">
            <div>Available<strong>${formatStock(inv.boxes, inv.units, drug)}</strong></div>
            <div>Reorder<strong>${Number(drug.reorderLevelUnits || 0)} ${esc(unitLabel(drug))}</strong></div>
          </div>
        </div>`;
    }).join("") || `<div class="empty-state">No medication cards match your search.</div>`;

  const dashboardArchiveHost = q("recentList")?.parentElement;
  if (dashboardArchiveHost) {
    let archivePanel = q("dashboardArchiveStatusPanel");
    if (!archivePanel) {
      archivePanel = document.createElement("div");
      archivePanel.id = "dashboardArchiveStatusPanel";
      archivePanel.className = "glass-card";
      archivePanel.style.marginBottom = "12px";
      dashboardArchiveHost.insertBefore(archivePanel, q("recentList"));
    }
    archivePanel.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
        <div>
          <div style="font-weight:800;font-size:14px">Archive Status</div>
          <div class="subline">${esc(formatArchiveStatusLabel(APP.archiveStatus))}</div>
        </div>
        ${APP.currentRole === "ADMIN" ? '<button id="dashboardArchiveNowBtn" class="soft-btn" type="button">Archive Now</button>' : ''}
      </div>`;
    const dashboardArchiveNowBtn = q("dashboardArchiveNowBtn");
    if (dashboardArchiveNowBtn) dashboardArchiveNowBtn.onclick = () => archiveOldDataNow({ reason: "dashboard_manual" });
  }

  renderDashboardCharts(scopedPrescriptions);
}

function renderInventory() {
  const term = (q("inventorySearch").value || "").toLowerCase();
  const location = q("inventoryLocationFilter").value || currentScopePharmacy();

  q("inventoryTbody").innerHTML = APP.cache.drugs.filter(drug => `${drug.scientificName} ${drug.tradeName} ${drug.category} ${drug.strength} ${drug.dosageForm}`.toLowerCase().includes(term)).map(drug => {
    const inv = invRow(drug.id, location) || { boxes: 0, units: 0, totalUnits: 0 };
    const low = Number(inv.totalUnits || 0) <= Number(drug.reorderLevelUnits || 0);
    return `
      <tr>
        <td>
          <div class="inventory-actions">
            <button class="soft-btn open-drug-btn" data-drugid="${esc(drug.id)}">Open</button>
            ${APP.currentRole === "ADMIN" ? `<button class="primary-btn stock-adjust-btn" data-adjust-stock="${esc(drug.id)}">Adjust Stock</button>` : ""}
          </div>
        </td>
        <td>${esc(drug.scientificName || "")}</td>
        <td>${esc(drug.tradeName || "")}</td>
        <td>${esc(drug.category || "")}</td>
        <td>${esc(drug.strength || "")}</td>
        <td>${esc(drug.dosageForm || "")}</td>
        <td>${Number(drug.unitsPerBox || 0)}</td>
        <td>${formatStock(inv.boxes, inv.units, drug)}</td>
        <td>${low ? '<span class="badge pending">LOW</span>' : '<span class="badge verified">OK</span>'}</td>
      </tr>`;
  }).join("");
}

function drugDisplayLabel(drugId, fallbackTradeName = "") {
  const drug = APP.cache.drugs.find(item => String(item.id) === String(drugId));
  return `${drug?.tradeName || fallbackTradeName || ''} ${drug?.strength || ''}`.trim() || fallbackTradeName || '-';
}

function formatDoseSummaryForAudit(row) {
  const dose = getActiveDoseForPrescription(row?.id);
  if (dose) {
    return `${Number(dose.unitsPerDose || 0)} unit(s) per dose · ${dose.frequency || '-'} · ${Number(dose.durationValue || 0)} ${dose.durationUnit || ''}(s) · Start ${dose.startDate || '-'}`;
  }
  return row?.dose || '-';
}

function renderTransactions() {
  ensureArchiveCacheForSection('transactions');
  const shell = q("transactionsTbody")?.closest('.table-shell') || q("transactionsTbody")?.parentElement;
  if (shell) mountArchiveToggle(shell, 'transactions', { prepend: true, panelId: 'transactionsArchiveTogglePanel' });
  const rows = getFilteredTransactionsRows();
  q("transactionsTbody").innerHTML = rows.map(row => {
    const typeClass = String(row.type || '').toLowerCase().replace(/[^a-z0-9]+/g, '-');
    const by = row.performedBy || row.receivedBy || row.receiverPharmacist || row.deletedBy || row.returnBy || '-';
    const note = row.note || row.invoiceNumber || row.patientName || '-';
    return `
      <tr>
        <td>${esc(formatJordanDateTime(row.dateTime))}</td>
        <td><span class="tx-type-pill tx-type-${esc(typeClass)}">${esc(row.type || "")}</span></td>
        <td>${esc(drugDisplayLabel(row.drugId, row.tradeName || ""))}</td>
        <td>${Number(row.qtyBoxes || 0)}</td>
        <td>${Number(row.qtyUnits || 0)}</td>
        <td>${esc(by)}</td>
        <td>${esc(note)}</td>
        <td><button class="soft-btn mini-btn transaction-details-btn" data-scope="normal" data-id="${esc(row.id)}">Details${isArchivedRow(row) ? ' · Archived' : ''}</button></td>
      </tr>`;
  }).join("") || `<tr><td colspan="8" class="empty-state">No transactions found.</td></tr>`;
}

function renderReports() {
  refreshScopedSelectors();
}

function getFilteredPrescriptionsRows() {
  const term = (q("prescriptionsSearch")?.value || "").toLowerCase().trim();
  const from = q("prescriptionsFromDate")?.value || "";
  const to = q("prescriptionsToDate")?.value || "";
  const effectiveFrom = from || (!to ? jordanDateKey() : "");
  const effectiveTo = to || (!from ? jordanDateKey() : "");
  const drugId = q("prescriptionsDrugFilter")?.value || "";
  const pharmacyFilter = q("prescriptionsPharmacyFilter")?.value || "";
  const baseRows = getScopedPrescriptionRows(getArchiveMode('prescriptions'));
  return baseRows.filter(row => {
    if (!canViewPharmacy(row.pharmacy)) return false;
    const day = formatJordanDateTime(row.dateTime).slice(0,10);
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    const hay = `${row.patientName||""} ${row.fileNumber||""} ${row.doctorName||""} ${row.pharmacistName||""} ${drug?.tradeName||""} ${drug?.strength||""}`.toLowerCase();
    if (term && !hay.includes(term)) return false;
    if (effectiveFrom && day < effectiveFrom) return false;
    if (effectiveTo && day > effectiveTo) return false;
    if (drugId && String(row.drugId)!==String(drugId)) return false;
    if (pharmacyFilter && String(row.pharmacy)!==String(pharmacyFilter)) return false;
    return true;
  }).sort((a,b)=>String(b.dateTime||"").localeCompare(String(a.dateTime||"")));
}

function renderPrescriptions() {
  if (!q("prescriptionsTbody")) return;
  ensureArchiveCacheForSection('prescriptions');
  const shell = q("prescriptionsTbody")?.closest('.table-shell') || q("prescriptionsTbody")?.parentElement;
  if (shell) mountArchiveToggle(shell, 'prescriptions', { prepend: true, panelId: 'prescriptionsArchiveTogglePanel' });
  const rows = getFilteredPrescriptionsRows();
  q("prescriptionsTbody").innerHTML = rows.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    return `<tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(row.fileNumber || "")}</td>
      <td>${esc(row.pharmacy || "")}</td>
      <td>${Number(row.qtyBoxes || 0)}</td>
      <td>${Number(row.qtyUnits || 0)}</td>
      <td>${statusBadge(row.status || "Registered")}${archivedLabelHtml(row)}</td>
      <td class="rx-actions-cell">${isArchivedRow(row) ? '<span class="muted">Archived</span>' : buildPrescriptionActionsDropdown(row, { prefix: "prescriptions" })}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="9" class="empty-state">No prescriptions found.</td></tr>`;
}

function renderAudit() {
  if (!canCurrentUserAudit()) return;
  const term = (q("auditSearch").value || "").toLowerCase();
  const scope = currentAuditPharmacy();
  const from = q("auditFromDate")?.value || "";
  const to = q("auditToDate")?.value || "";
  const effectiveFrom = from || (!to ? jordanDateKey() : "");
  const effectiveTo = to || (!from ? jordanDateKey() : "");
  const auditTableShell = q("auditTbody").closest(".table-shell");
  if (auditTableShell) auditTableShell.classList.add("audit-shell");

  const rows = scopedPrescriptionRowsByPharmacy(scope).filter(row => {
    if (APP.auditTab === "new") return (row.status || "New") === "New";
    if (APP.auditTab === "pending") return row.status === "Pending";
    return row.status === "Verified";
  }).filter(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    const dose = getActiveDoseForPrescription(row.id);
    const day = formatJordanDateTime(row.dateTime).slice(0, 10);
    const haystack = `${row.patientName} ${row.fileNumber} ${row.doctorName} ${row.pharmacistName || ''} ${drug?.tradeName || ""} ${drug?.strength || ""} ${dose?.frequency || ''} ${dose?.unitsPerDose || ''}`.toLowerCase();
    if (term && !haystack.includes(term)) return false;
    if (effectiveFrom && day < effectiveFrom) return false;
    if (effectiveTo && day > effectiveTo) return false;
    return true;
  }).sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));

  q("auditTbody").innerHTML = rows.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    const noteId = `audit_note_${row.id}`;
    const detailId = `audit_detail_${row.id}`;
    const dose = getActiveDoseForPrescription(row.id);
    const unitPerBox = Number(drug?.unitsPerBox || 1);
    const totalUnits = Number(row.qtyBoxes || 0) * unitPerBox + Number(row.qtyUnits || 0);
    const doseSummary = dose
      ? `${Number(dose.unitsPerDose || 0)} unit(s) per dose · ${dose.frequency || '-'} · ${Number(dose.durationValue || 0)} ${dose.durationUnit || ''}(s) · Start ${dose.startDate || '-'}`
      : (row.dose || '-');

    const menuItems = [];
    if (APP.auditTab === "new") {
      menuItems.push(`<button class="audit-menu-item audit-btn" data-id="${row.id}" data-status="Verified" data-note="${noteId}">Verify</button>`);
      menuItems.push(`<button class="audit-menu-item audit-btn" data-id="${row.id}" data-status="Pending" data-note="${noteId}">Pending</button>`);
    } else if (APP.auditTab === "pending") {
      menuItems.push(`<button class="audit-menu-item audit-btn" data-id="${row.id}" data-status="Verified" data-note="${noteId}">Verify</button>`);
    }
    menuItems.push(`<button class="audit-menu-item edit-rx-btn" data-id="${row.id}">Edit</button>`);
    menuItems.push(`<button class="audit-menu-item danger delete-rx-btn" data-id="${row.id}">Delete</button>`);

    const actionCell = `
      <div class="audit-action-cell">
        <div class="audit-menu-wrap">
          <button class="audit-menu-btn" data-audit-menu="${row.id}">Actions</button>
          <div class="audit-menu hidden" id="audit_menu_${row.id}">
            ${menuItems.join("")}
          </div>
        </div>
      </div>`;

    return `
      <tr class="audit-summary-row ${String(APP.openAuditDetailId || '') === String(row.id) ? 'is-open' : ''}">
        <td>
          <button class="audit-expand-btn" data-audit-expand="${row.id}" aria-expanded="${String(APP.openAuditDetailId || '') === String(row.id)}">
            ${String(APP.openAuditDetailId || '') === String(row.id) ? '−' : '+'}
          </button>
        </td>
        <td>${esc(formatJordanDateTime(row.dateTime))}</td>
        <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td>
        <td>${esc(row.patientName || "")}</td>
        <td>${esc(row.fileNumber || "")}</td>
        <td>${Number(row.qtyBoxes || 0)}</td>
        <td>${Number(row.qtyUnits || 0)}</td>
        <td>${esc(row.doctorName || "")}</td>
        <td><input id="${noteId}" class="audit-note" value="${esc(row.auditNote || "")}"></td>
        <td>${actionCell}</td>
      </tr>
      <tr id="${detailId}" class="audit-detail-row ${String(APP.openAuditDetailId || '') === String(row.id) ? '' : 'hidden'}">
        <td colspan="10">
          <div class="audit-detail-grid">
            <div class="audit-detail-card"><span>Patient Name</span><strong>${esc(row.patientName || '-')}</strong></div>
            <div class="audit-detail-card"><span>File Number</span><strong>${esc(row.fileNumber || '-')}</strong></div>
            <div class="audit-detail-card"><span>Pharmacist</span><strong>${esc(row.pharmacistName || '-')}</strong></div>
            <div class="audit-detail-card"><span>Doctor</span><strong>${esc(row.doctorName || '-')}</strong></div>
            <div class="audit-detail-card"><span>Boxes</span><strong>${Number(row.qtyBoxes || 0)}</strong></div>
            <div class="audit-detail-card"><span>Units</span><strong>${Number(row.qtyUnits || 0)}</strong></div>
            <div class="audit-detail-card"><span>Total Dispensed Units</span><strong>${totalUnits}</strong></div>
            <div class="audit-detail-card"><span>Prescription Type</span><strong>${esc(row.prescriptionType || '-')}</strong></div>
            <div class="audit-detail-card full"><span>Dose</span><strong>${esc(doseSummary)}</strong></div>
            <div class="audit-detail-card full"><span>Audit Trail</span><strong>${esc(row.status || 'New')} ${row.auditBy ? `· ${row.auditBy}` : ''} ${row.auditDateTime ? `· ${formatJordanDateTime(row.auditDateTime)}` : ''}</strong></div>
          </div>
        </td>
      </tr>`;
  }).join("") || `<tr><td colspan="10" class="empty-state">No prescriptions found in this audit tab.</td></tr>`;
}



function openAdjustStockModal(drugId) {
  if (APP.currentRole !== "ADMIN") return;
  APP.adjustStockDrugId = drugId;
  const pharmacy = q("inventoryLocationFilter").value || currentScopePharmacy();
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const inv = invRow(drugId, pharmacy) || { boxes: 0, units: 0 };
  q("adjustStockDrugDisplay").value = `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim();
  q("adjustStockPharmacyDisplay").value = pharmacy;
  q("adjustStockBoxes").value = Number(inv.boxes || 0);
  q("adjustStockUnits").value = Number(inv.units || 0);
  openModal("adjustStockModal");
}

async function saveAdjustedStock() {
  if (APP.currentRole !== "ADMIN" || !APP.adjustStockDrugId) return;
  const drug = APP.cache.drugs.find(d => d.id === APP.adjustStockDrugId);
  const pharmacy = q("adjustStockPharmacyDisplay").value || currentScopePharmacy();
  const boxes = Number(q("adjustStockBoxes").value || 0);
  const units = Number(q("adjustStockUnits").value || 0);
  if (boxes < 0 || units < 0) {
    showActionModal("Validation", "Boxes and units cannot be negative.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const normalized = normalizeInventory(boxes, units, drug?.unitsPerBox || 1);
  const currentInv = invRow(APP.adjustStockDrugId, pharmacy);
  showActionModal("Adjust Stock", "Please wait while stock is being updated...");
  if (currentInv?.id) {
    await updateDoc(doc(db, "inventory", currentInv.id), {
      boxes: normalized.boxes,
      units: normalized.units,
      totalUnits: normalized.totalUnits,
      updatedAt: serverTimestamp()
    });
  } else {
    await addDoc(collection(db, "inventory"), {
      drugId: APP.adjustStockDrugId,
      pharmacy,
      boxes: normalized.boxes,
      units: normalized.units,
      totalUnits: normalized.totalUnits,
      updatedAt: jordanNowIso()
    });
  }
  await addDoc(collection(db, "transactions"), {
    type: "Adjust Stock",
    drugId: APP.adjustStockDrugId,
    tradeName: drug?.tradeName || "",
    pharmacy,
    qtyBoxes: normalized.boxes,
    qtyUnits: normalized.units,
    performedBy: APP.currentRole,
    note: "Manual stock adjustment by admin",
    dateTime: jordanNowIso()
  });
  await refreshTablesImmediate(["inventory", "transactions"]);
  closeModal("adjustStockModal");
  finishActionModal(true, "Available stock updated successfully.");
}

function openConfirmActionModal(type, id) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx || (type === "delete" ? !canDeletePrescriptionRow(rx) : !canViewPharmacy(rx.pharmacy))) return;
  APP.confirmAction = { type, id };
  q("confirmActionTitle").textContent = type === "delete" ? "Delete Prescription" : "Return Prescription";
  if (type === "delete") {
    q("confirmActionText").textContent = rx.status === "Returned"
      ? "Please confirm deleting this returned prescription. Stock will not be restored because it was already returned."
      : "Please confirm deleting this prescription. Stock will be restored.";
  } else {
    q("confirmActionText").textContent = "Please confirm returning this prescription. Stock will be restored.";
  }
  applyCurrentUserReadonlyFields();
  if (q("confirmActionPharmacist")) q("confirmActionPharmacist").value = currentActorName();
  if (q("confirmActionPharmacist").parentElement) q("confirmActionPharmacist").parentElement.classList.remove("hidden");
  openModal("confirmActionModal");
}

async function submitConfirmedAction() {
  if (!APP.confirmAction) return closeModal("confirmActionModal");
  const action = APP.confirmAction;
  const pharmacistName = q("confirmActionPharmacist")?.value || currentActorName() || "";
  if (!pharmacistName) {
    showActionModal("Validation", "Please select the pharmacist who performed this action.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  if (action.scope === "narcotic") {
    closeModal("confirmActionModal");
    APP.confirmAction = null;
    if (action.type === "return") return performReturnNarcoticPrescription(action.id, pharmacistName);
    if (action.type === "delete") return performDeleteNarcoticPrescription(action.id, pharmacistName);
    return;
  }
  const { type, id } = action;
  closeModal("confirmActionModal");
  APP.confirmAction = null;
  if (type === "return") return performReturnPrescription(id, pharmacistName);
  if (type === "delete") return performDeletePrescription(id, pharmacistName);
}

async function performReturnNarcoticPrescription(id, pharmacistName) {
  const row = narcoticPrescriptionById(id);
  if (!row) return;
  const stockRow = narcoticDeptStockRow(row.departmentId, row.drugId);
  const batch = writeBatch(db);
  batch.update(doc(db, "narcotic_prescriptions", id), { status: "Returned", returnPharmacist: pharmacistName, updatedAt: serverTimestamp(), updatedBy: pharmacistName });
  batch.set(doc(collection(db, "narcotic_order_movements")), {
    dateTime: jordanNowIso(),
    type: "Return",
    departmentId: row.departmentId,
    departmentName: row.departmentName || narcoticDeptById(row.departmentId)?.departmentName || "",
    drugId: row.drugId,
    drugName: row.drugName || narcoticDrugById(row.drugId)?.tradeName || "",
    quantitySent: Number(row.dispensedUnits || 0),
    performedBy: pharmacistName,
    patientName: row.patientName || "",
    fileNumber: row.fileNumber || "",
    returnBy: pharmacistName,
    returnDateTime: jordanNowIso(),
    notes: `Prescription returned for ${row.patientName || ""}`
  });
  if (stockRow?.id) {
    batch.update(doc(db, "narcotic_department_stock", stockRow.id), {
      availableStockUnits: Number(stockRow.availableStockUnits || 0) + Number(row.dispensedUnits || 0),
      updatedAt: serverTimestamp()
    });
  }
  showActionModal("Return Narcotic Prescription", "Please wait while the prescription is being returned...");
  await batch.commit();
  await refreshTablesImmediate(["prescriptions", "inventory", "transactions"]);
  finishActionModal(true, "Prescription returned successfully.");
}

async function performDeleteNarcoticPrescription(id, pharmacistName) {
  const row = narcoticPrescriptionById(id);
  if (!row) return;
  const stockRow = narcoticDeptStockRow(row.departmentId, row.drugId);
  const batch = writeBatch(db);
  const shouldRestoreStock = row.status !== "Returned";
  batch.set(doc(collection(db, "narcotic_order_movements")), {
    dateTime: jordanNowIso(),
    type: "Delete Prescription",
    departmentId: row.departmentId,
    departmentName: row.departmentName || narcoticDeptById(row.departmentId)?.departmentName || "",
    drugId: row.drugId,
    drugName: row.drugName || narcoticDrugById(row.drugId)?.tradeName || "",
    quantitySent: Number(row.dispensedUnits || 0),
    performedBy: pharmacistName,
    patientName: row.patientName || "",
    fileNumber: row.fileNumber || "",
    deletedBy: pharmacistName,
    deletedDateTime: jordanNowIso(),
    notes: `Prescription deleted for ${row.patientName || ""}`
  });
  batch.delete(doc(db, "narcotic_prescriptions", id));
  if (shouldRestoreStock && stockRow?.id) {
    batch.update(doc(db, "narcotic_department_stock", stockRow.id), {
      availableStockUnits: Number(stockRow.availableStockUnits || 0) + Number(row.dispensedUnits || 0),
      updatedAt: serverTimestamp()
    });
  }
  showActionModal("Delete Narcotic Prescription", "Please wait while the prescription is being deleted...");
  await batch.commit();
  await refreshTablesImmediate(["prescriptions", "inventory", "transactions"]);
  finishActionModal(true, shouldRestoreStock ? "Prescription deleted successfully and stock was restored." : "Returned prescription deleted successfully without restoring stock.");
}

async function performDeletePrescription(id, pharmacistName) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx || !canDeletePrescriptionRow(rx)) return;
  const drug = APP.cache.drugs.find(d => d.id === rx.drugId);
  const inv = invRow(rx.drugId, rx.pharmacy);
  const unitsPerBox = Number(drug?.unitsPerBox || 1);
  const rxUnits = Number(rx.qtyBoxes || 0) * unitsPerBox + Number(rx.qtyUnits || 0);

  showActionModal("Delete Prescription", "Please wait while the prescription is being deleted...");
  const batch = writeBatch(db);

  if (rx.status !== "Returned") {
    const restored = normalizeInventory(Number(inv?.boxes || 0), Number(inv?.units || 0) + rxUnits, unitsPerBox);
    if (inv?.id) {
      batch.update(doc(db, "inventory", inv.id), { boxes: restored.boxes, units: restored.units, totalUnits: restored.totalUnits, updatedAt: serverTimestamp() });
    }
  }

  batch.delete(doc(db, "prescriptions", id));
  batch.set(doc(collection(db, "transactions")), {
    type: "Delete Prescription", drugId: rx.drugId, tradeName: drug?.tradeName || "", pharmacy: rx.pharmacy, qtyBoxes: rx.qtyBoxes || 0, qtyUnits: rx.qtyUnits || 0, performedBy: pharmacistName, patientName: rx.patientName || "", fileNumber: rx.fileNumber || "", doctorName: rx.doctorName || "", pharmacistName: rx.pharmacistName || pharmacistName, deletedBy: pharmacistName, deletedDateTime: jordanNowIso(), note: `Prescription deleted for ${rx.patientName || ""}`, dateTime: jordanNowIso()
  });

  await batch.commit();
  finishActionModal(true, rx.status === "Returned" ? "Prescription deleted successfully." : "Prescription deleted successfully and stock was restored.");
}

async function performReturnPrescription(id, pharmacistName) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx || rx.status === "Returned") return;
  const drug = APP.cache.drugs.find(d => d.id === rx.drugId);
  const inv = invRow(rx.drugId, rx.pharmacy);
  const currentTotal = Number(inv?.totalUnits || 0);
  const addedUnits = Number(rx.qtyBoxes || 0) * Number(drug.unitsPerBox || 1) + Number(rx.qtyUnits || 0);
  const updatedStock = normalizeInventory(0, currentTotal + addedUnits, drug.unitsPerBox);

  showActionModal("Return Prescription", "Please wait while the prescription is being returned...");
  const batch = writeBatch(db);
  batch.update(doc(db, "prescriptions", id), { status: "Returned", returnBy: pharmacistName, returnDateTime: jordanNowIso(), updatedBy: pharmacistName });
  batch.update(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
  batch.set(doc(collection(db, "transactions")), { type: "Return", drugId: rx.drugId, tradeName: drug.tradeName, pharmacy: rx.pharmacy, qtyBoxes: Number(rx.qtyBoxes || 0), qtyUnits: Number(rx.qtyUnits || 0), performedBy: pharmacistName, patientName: rx.patientName || "", fileNumber: rx.fileNumber || "", doctorName: rx.doctorName || "", pharmacistName: rx.pharmacistName || pharmacistName, returnBy: pharmacistName, returnDateTime: jordanNowIso(), note: `Returned prescription for ${rx.patientName}`, dateTime: jordanNowIso() });
  await batch.commit();
  await refreshTablesImmediate(["narcotic_prescriptions", "narcotic_department_stock", "narcotic_order_movements"]);
  finishActionModal(true, "Prescription returned successfully.");
}

function userRoleLabel(role) {
  return String(role || "").replaceAll("_", " ");
}

function userField(id) {
  return q(`${id}Modal`) || q(id);
}

function userCheckboxNodes(baseId) {
  const modal = q(`${baseId}Modal`);
  if (modal) return modal;
  return q(baseId);
}

function selectedUserPharmacies() {
  return [...document.querySelectorAll('#userPharmacyCheckboxesModal .user-pharmacy-checkbox, #userPharmacyCheckboxes .user-pharmacy-checkbox')]
    .filter(el => el.checked)
    .map(el => el.value)
    .filter((value, index, arr) => arr.indexOf(value) === index);
}

function selectedUserRoles() {
  return [...document.querySelectorAll('#userRoleCheckboxesModal .user-role-checkbox, #userRoleCheckboxes .user-role-checkbox')]
    .filter(el => el.checked)
    .map(el => el.value)
    .filter((value, index, arr) => arr.indexOf(value) === index);
}

function syncUserModalToHiddenFields() {
  const pairs = [
    ['userName','value'],
    ['userJobNumber','value'],
    ['userPassword','value'],
    ['userCanAudit','value'],
    ['userCanNarcotic','value'],
    ['userFormMode','textContent'],
    ['saveUserBtn','textContent'],
    ['cancelUserEditBtn','textContent'],
    ['toggleUserActiveBtn','textContent']
  ];
  pairs.forEach(([id, prop]) => {
    const base = q(id);
    const modal = q(`${id}Modal`);
    if (base && modal) base[prop] = modal[prop];
  });
  const toggleBase = q('toggleUserActiveBtn');
  const toggleModal = q('toggleUserActiveBtnModal');
  if (toggleBase && toggleModal) {
    toggleBase.dataset.id = toggleModal.dataset.id || '';
    toggleBase.classList.toggle('hidden', toggleModal.classList.contains('hidden'));
  }
  const cancelBase = q('cancelUserEditBtn');
  const cancelModal = q('cancelUserEditBtnModal');
  if (cancelBase && cancelModal) {
    cancelBase.classList.toggle('hidden', cancelModal.classList.contains('hidden'));
    cancelBase.textContent = cancelModal.textContent || 'Cancel';
  }

  const syncChecks = (baseId) => {
    const baseWrap = q(baseId);
    const modalWrap = q(`${baseId}Modal`);
    if (!baseWrap || !modalWrap) return;
    const modalChecked = new Set([...modalWrap.querySelectorAll('input:checked')].map(el => el.value));
    baseWrap.querySelectorAll('input').forEach(el => { el.checked = modalChecked.has(el.value); });
  };
  syncChecks('userPharmacyCheckboxes');
  syncChecks('userRoleCheckboxes');
}

function populateEditPharmacistOptions(pharmacy, selectedValue) {
  const el = q('editPharmacist');
  if (!el) return;
  const users = getScopePharmacists(pharmacy || currentScopePharmacy());
  const names = users.map(user => user.userName || user.fullName || user.displayName || user.name).filter(Boolean);
  const uniqueNames = [...new Set(names)];
  if (selectedValue && !uniqueNames.includes(selectedValue)) uniqueNames.unshift(selectedValue);
  el.innerHTML = uniqueNames.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join('') || `<option value="">No users found</option>`;
  el.value = uniqueNames.includes(selectedValue) ? selectedValue : (uniqueNames[0] || '');
}


function renderSettings() {
  if (APP.currentRole !== "ADMIN") return;
  if (q("settingsPharmacy")) q("settingsPharmacy").value = APP.cache.settings.pharmacyType || "In-Patient Pharmacy";
  if (q("settingsMonth")) q("settingsMonth").value = APP.cache.settings.month || MONTHS[new Date().getMonth()];
  if (q("settingsYear")) q("settingsYear").value = APP.cache.settings.year || new Date().getFullYear();
  if (userField("saveUserBtn")) userField("saveUserBtn").textContent = APP.userEditId ? "Save Changes" : "Add User";
  if (userField("cancelUserEditBtn")) {
    userField("cancelUserEditBtn").classList.remove("hidden");
    userField("cancelUserEditBtn").textContent = "Cancel";
  }
  if (userField("userFormMode")) userField("userFormMode").textContent = APP.userEditId ? "Edit user" : "Add new user";
  syncUserModalToHiddenFields();
  const users = (APP.cache.users || []).slice().sort((a, b) => String(a.userName || "").localeCompare(String(b.userName || "")));
  q("usersCards").innerHTML = users.map(user => {
    const pharmacies = getUserAllowedPharmacies(user).join(", ") || "-";
    const roles = (user.userRoles || [user.role].filter(Boolean)).map(userRoleLabel).join(", ") || "-";
    return `
      <button class="user-card-item ${APP.userEditId === user.id ? 'selected' : ''}" data-edit-user="${user.id}" type="button">
        <div class="user-card-top">
          <div class="user-card-name">${esc(user.userName || '-')}</div>
          <span class="status-chip ${user.active !== false ? 'active' : 'inactive'}">${user.active !== false ? 'Active' : 'Inactive'}</span>
        </div>
        <div class="user-card-meta">Job No. ${esc(user.employeeNumber || '-')}</div>
        <div class="user-card-meta">Workplaces: ${esc(pharmacies)}</div>
        <div class="user-card-meta">Roles: ${esc(roles)}</div>
      </button>`;
  }).join("") || `<div class="empty-state">No users found.</div>`;

  const usersCardsHost = q("usersCards")?.parentElement;
  if (usersCardsHost) {
    let archiveSettingsPanel = q("settingsArchivePanel");
    if (!archiveSettingsPanel) {
      archiveSettingsPanel = document.createElement("div");
      archiveSettingsPanel.id = "settingsArchivePanel";
      archiveSettingsPanel.className = "glass-card";
      archiveSettingsPanel.style.marginBottom = "16px";
      usersCardsHost.insertBefore(archiveSettingsPanel, q("usersCards"));
    }
    archiveSettingsPanel.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
        <div>
          <div style="font-weight:800;font-size:14px">Archive Controls</div>
          <div class="subline">${esc(formatArchiveStatusLabel(APP.archiveStatus))}</div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button id="settingsArchiveRefreshBtn" class="soft-btn" type="button">Refresh Archive Status</button>
          <button id="settingsArchiveNowBtn" class="primary-btn" type="button">Archive Now</button>
        </div>
      </div>`;
    const settingsArchiveRefreshBtn = q("settingsArchiveRefreshBtn");
    const settingsArchiveNowBtn = q("settingsArchiveNowBtn");
    if (settingsArchiveRefreshBtn) settingsArchiveRefreshBtn.onclick = () => loadArchiveStatus();
    if (settingsArchiveNowBtn) settingsArchiveNowBtn.onclick = () => archiveOldDataNow({ reason: "settings_manual" });
  }
}


function getDrugById(drugId) {
  return (APP.cache.drugs || []).find(d => String(d.id) === String(drugId));
}

function getPrescriptionQtyText(rx, drug) {
  return `${Number(rx?.qtyBoxes || 0)} box(es) + ${Number(rx?.qtyUnits || 0)} ${esc(unitLabel(drug))}`;
}

function dosageUnitSingular(drug) {
  const form = String(drug?.dosageForm || "").toLowerCase();
  if (form.includes("capsule")) return "capsule";
  if (form.includes("tablet")) return "tablet";
  if (form.includes("patch")) return "patch";
  if (form.includes("inject")) return "ampoule";
  if (form.includes("drop")) return "drop";
  return "unit";
}

function frequencyPerDay(freq) {
  const map = {
    "Once Daily": 1,
    "Every 12 hr": 2,
    "Every 8 hr": 3,
    "Every 6 hr": 4,
    "Every 4 hr": 6
  };
  return map[String(freq || "").trim()] || 1;
}

function durationToDays(value, unit) {
  const num = Math.max(0, Number(value || 0));
  if (String(unit || "").toLowerCase() === "week") return num * 7;
  if (String(unit || "").toLowerCase() === "month") return num * 30;
  return num;
}

function formatDoseText(dose, drug) {
  if (!dose) return "No dose added";
  const unitName = dosageUnitSingular(drug);
  return `${Number(dose.unitsPerDose || 0)} ${unitName}${Number(dose.unitsPerDose || 0) === 1 ? "" : "s"} · ${dose.frequency || "-"} · for ${Number(dose.durationValue || 0)} ${dose.durationUnit || ""}`;
}

function getPrescriptionTotalUnits(rx, drug) {
  return Number(rx?.qtyBoxes || 0) * Number(drug?.unitsPerBox || 1) + Number(rx?.qtyUnits || 0);
}

function getDoseHistoryForPrescription(prescriptionId) {
  return (APP.cache.prescriptionDoses || [])
    .filter(row => String(row.prescriptionId) === String(prescriptionId))
    .sort((a, b) => String(b.createdAt || b.dateTime || "").localeCompare(String(a.createdAt || a.dateTime || "")));
}

function getActiveDoseForPrescription(prescriptionId) {
  return getDoseHistoryForPrescription(prescriptionId).find(row => row.active !== false && String(row.active).toUpperCase() !== "FALSE") || null;
}

function calculatePrescriptionSupply(rx, doseOverride = null) {
  if (!rx) return null;
  const drug = getDrugById(rx.drugId);
  if (!drug) return null;
  const dose = doseOverride || getActiveDoseForPrescription(rx.id);
  const totalUnits = getPrescriptionTotalUnits(rx, drug);
  const scientificName = String(drug.scientificName || "").trim();
  if (!dose) {
    return {
      prescription: rx, drug, dose: null, scientificName, totalUnits,
      dailyUsage: 0, totalRequired: 0, coverageDays: 0, elapsedDays: 0, consumedUnits: 0,
      remainingUnits: totalUnits, estimatedFinishDate: "", status: rx.status === "Returned" ? "returned" : "no-dose", matchType: "exact"
    };
  }
  const dailyUsage = Number(dose.unitsPerDose || 0) * frequencyPerDay(dose.frequency);
  const totalRequired = dailyUsage * durationToDays(dose.durationValue, dose.durationUnit);
  const coverageDays = dailyUsage > 0 ? totalUnits / dailyUsage : 0;
  const startDateText = String(dose.startDate || rx.dateTime || "").slice(0, 10);
  const startDate = parseJordanDateTime(startDateText ? `${startDateText} 00:00:00` : rx.dateTime);
  const now = parseJordanDateTime(jordanNowIso());
  const elapsedDays = startDate && now ? Math.max(0, Math.floor((now.getTime() - startDate.getTime()) / (24 * 60 * 60 * 1000))) : 0;
  const consumedUnits = Math.max(0, Math.floor(elapsedDays * dailyUsage));
  const remainingUnits = Math.max(0, totalUnits - consumedUnits);
  let estimatedFinishDate = "";
  if (startDate && coverageDays > 0) {
    const finish = new Date(startDate.getTime() + Math.ceil(coverageDays) * 24 * 60 * 60 * 1000);
    estimatedFinishDate = finish.toISOString().slice(0, 10);
  }
  let status = "active";
  if (rx.status === "Returned") status = "returned";
  else if (!dose) status = "no-dose";
  else if (remainingUnits <= 0) status = "finished";
  else if (coverageDays > 0 && remainingUnits / dailyUsage <= 3) status = "finishing-soon";
  return {
    prescription: rx, drug, dose, scientificName, totalUnits,
    dailyUsage, totalRequired, coverageDays, elapsedDays, consumedUnits,
    remainingUnits, estimatedFinishDate, status, matchType: "exact"
  };
}

function formatRemainingText(calc) {
  if (!calc) return "-";
  const unitName = dosageUnitSingular(calc.drug);
  const daysLeft = calc.dailyUsage > 0 ? Math.ceil(calc.remainingUnits / calc.dailyUsage) : 0;
  if (!calc.dose) return `${calc.remainingUnits} ${unitName}${calc.remainingUnits === 1 ? "" : "s"} · no dose`;
  return `${calc.remainingUnits} ${unitName}${calc.remainingUnits === 1 ? "" : "s"} · ${daysLeft} day(s)`;
}

function findScientificNameSupply(fileNumber, drugId) {
  const targetDrug = getDrugById(drugId);
  const scientificName = String(targetDrug?.scientificName || "").trim().toLowerCase();
  const targetStrength = String(targetDrug?.strength || "").trim().toLowerCase();
  if (!scientificName) return [];
  return getScopedPrescriptionRows(getArchiveMode('patients'))
    .filter(rx => normalizeFileNumber(rx.fileNumber) === normalizeFileNumber(fileNumber))
    .filter(rx => String(rx.status || "").toLowerCase() !== "returned")
    .map(rx => calculatePrescriptionSupply(rx))
    .filter(Boolean)
    .filter(calc => String(calc.scientificName || "").trim().toLowerCase() === scientificName)
    .filter(calc => calc.remainingUnits > 0)
    .map(calc => ({
      ...calc,
      matchType: String(calc.drug?.strength || "").trim().toLowerCase() === targetStrength ? "exact" : "scientific"
    }))
    .sort((a, b) => String(b.prescription?.dateTime || "").localeCompare(String(a.prescription?.dateTime || "")));
}

function setPatientsFiltersForProfile(fileNumber) {
  if (q("patientsFileFilter")) q("patientsFileFilter").value = String(fileNumber || "");
  showPage("patients");
  renderPatientsPage();
}

function getFilteredPatientRows() {
  const fileNumber = normalizeFileNumber(q("patientsFileFilter")?.value || "");
  const drugId = q("patientsDrugFilter")?.value || "";
  const pharmacy = q("patientsPharmacyFilter")?.value || "";
  const status = q("patientsStatusFilter")?.value || "";
  const fromDate = q("patientsFromFilter")?.value || "";
  const toDate = q("patientsToFilter")?.value || "";
  const search = String(q("patientsSearchFilter")?.value || "").trim().toLowerCase();

  return getScopedPrescriptionRows(getArchiveMode('patients'))
    .filter(rx => WORK_PHARMACIES.includes(rx.pharmacy))
    .map(rx => ({ rx, calc: calculatePrescriptionSupply(rx) }))
    .filter(({ rx, calc }) => {
      const drug = calc?.drug || getDrugById(rx.drugId);
      const day = formatJordanDateTime(rx.dateTime).slice(0, 10);
      const hay = `${rx.patientName || ""} ${rx.fileNumber || ""} ${rx.doctorName || ""} ${rx.pharmacistName || ""} ${drug?.tradeName || ""} ${drug?.scientificName || ""} ${drug?.strength || ""}`.toLowerCase();
      if (fileNumber && normalizeFileNumber(rx.fileNumber) !== fileNumber) return false;
      if (drugId && String(rx.drugId) !== String(drugId)) return false;
      if (pharmacy && String(rx.pharmacy) !== pharmacy) return false;
      if (status && String(calc?.status || "") !== status) return false;
      if (fromDate && day < fromDate) return false;
      if (toDate && day > toDate) return false;
      if (search && !hay.includes(search)) return false;
      return true;
    })
    .sort((a, b) => String(b.rx.dateTime || "").localeCompare(String(a.rx.dateTime || "")));
}

function buildPatientDetailCard(rx, calc) {
  const drug = calc?.drug || getDrugById(rx.drugId);
  const dose = calc?.dose;
  const history = getDoseHistoryForPrescription(rx.id);
  const scientificMatches = findScientificNameSupply(rx.fileNumber, rx.drugId).filter(item => String(item.prescription.id) !== String(rx.id));
  return `<div class="patient-detail-card">
    <div class="patient-detail-grid">
      <div class="patient-detail-item"><span>Scientific Name</span><strong>${esc(drug?.scientificName || "-")}</strong></div>
      <div class="patient-detail-item"><span>Trade Name</span><strong>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</strong></div>
      <div class="patient-detail-item"><span>Dispensed Quantity</span><strong>${esc(getPrescriptionQtyText(rx, drug))}</strong></div>
      <div class="patient-detail-item"><span>Pharmacy</span><strong>${esc(rx.pharmacy || "-")}</strong></div>
      <div class="patient-detail-item"><span>Current Dose</span><strong>${esc(formatDoseText(dose, drug))}</strong></div>
      <div class="patient-detail-item"><span>Daily Usage</span><strong>${dose ? `${Number(calc.dailyUsage || 0)} ${esc(dosageUnitSingular(drug))}(s)` : "-"}</strong></div>
      <div class="patient-detail-item"><span>Coverage Days</span><strong>${dose ? `${Math.ceil(Number(calc.coverageDays || 0))} day(s)` : "-"}</strong></div>
      <div class="patient-detail-item"><span>Estimated Finish</span><strong>${esc(calc?.estimatedFinishDate || "-")}</strong></div>
      <div class="patient-detail-item"><span>Remaining Supply</span><strong>${esc(formatRemainingText(calc))}</strong></div>
      <div class="patient-detail-item"><span>Status</span><strong>${esc((calc?.status || "").replaceAll("-", " "))}</strong></div>
      <div class="patient-detail-item"><span>Dose History</span><strong>${history.length} entr${history.length === 1 ? "y" : "ies"}</strong></div>
      <div class="patient-detail-item"><span>Scientific Name Matches</span><strong>${scientificMatches.length} related prescription(s)</strong></div>
    </div>
    <div class="patient-detail-actions">
      <button class="soft-btn add-dose-btn" data-add-dose="${esc(rx.id)}">Add Dose</button>
    </div>
  </div>`;
}

function renderPatientsPage() {
  if (!q("patientsTbody")) return;
  ensureArchiveCacheForSection('patients');
  const shell = q("patientsTbody")?.closest('.table-shell') || q("patientsTbody")?.parentElement;
  if (shell) mountArchiveToggle(shell, 'patients', { prepend: true, panelId: 'patientsArchiveTogglePanel' });
  const fileFilter = normalizeFileNumber(q("patientsFileFilter")?.value || "");
  if (!fileFilter) {
    ["patientsTotalRx","patientsActiveMedications","patientsRemainingUnits","patientsClinicalAlerts"].forEach(id => { if (q(id)) q(id).textContent = "0"; });
    q("patientsTbody").innerHTML = `<tr><td colspan="10" class="empty-state">Enter patient file number to view prescriptions.</td></tr>`;
    return;
  }
  const rows = getFilteredPatientRows();
  let activeCount = 0;
  let remainingUnits = 0;
  let alerts = 0;
  rows.forEach(({ calc }) => {
    if (calc?.status === "active" || calc?.status === "finishing-soon") activeCount += 1;
    remainingUnits += Number(calc?.remainingUnits || 0);
    if ((findScientificNameSupply(calc?.prescription?.fileNumber, calc?.prescription?.drugId) || []).filter(item => item.remainingUnits > 0).length > 1) alerts += 1;
  });
  q("patientsTotalRx").textContent = String(rows.length);
  q("patientsActiveMedications").textContent = String(activeCount);
  q("patientsRemainingUnits").textContent = String(remainingUnits);
  q("patientsClinicalAlerts").textContent = String(alerts);

  q("patientsTbody").innerHTML = rows.length ? rows.map(({ rx, calc }) => {
    const drug = calc?.drug || getDrugById(rx.drugId);
    const isOpen = APP.openPatientPrescriptionId && String(APP.openPatientPrescriptionId) === String(rx.id);
    const qtyText = getPrescriptionQtyText(rx, drug);
    const statusCls = calc?.status || "no-dose";
    const statusLabel = statusCls.replaceAll("-", " ");
    return `<tr>
      <td><button class="patient-row-toggle" data-patient-toggle="${esc(rx.id)}">${isOpen ? "−" : "+"}</button></td>
      <td>${esc(formatJordanDateTime(rx.dateTime))}</td>
      <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}<div class="subline">${esc(drug?.scientificName || "")}</div></td>
      <td>${esc(rx.patientName || "")}</td>
      <td>${esc(rx.fileNumber || "")}</td>
      <td>${esc(rx.pharmacy || "")}</td>
      <td>${esc(qtyText)}</td>
      <td><span class="patient-status-chip ${esc(statusCls)}">${esc(statusLabel)}</span>${archivedLabelHtml(rx)}</td>
      <td>${esc(formatRemainingText(calc))}</td>
      <td>${isArchivedRow(rx) ? '<span class="muted">Archived</span>' : `<button class="soft-btn add-dose-btn" data-add-dose="${esc(rx.id)}">Add Dose</button>`}</td>
    </tr>${isOpen ? `<tr class="patient-detail-row"><td colspan="10">${buildPatientDetailCard(rx, calc)}</td></tr>` : ""}`;
  }).join("") : `<tr><td colspan="10" class="empty-state">No patient prescriptions found for the selected filters.</td></tr>`;
}

function openAddDoseModal(prescriptionId) {
  const rx = getScopedPrescriptionRows(getArchiveMode('patients')).find(item => String(item.id) === String(prescriptionId));
  if (!rx || !canViewPharmacy(rx.pharmacy) || isArchivedRow(rx)) return;
  APP.currentDosePrescriptionId = rx.id;
  const drug = getDrugById(rx.drugId);
  const currentDose = getActiveDoseForPrescription(rx.id);
  const rxDate = formatJordanDateTime(rx.dateTime).slice(0, 10);
  q("dosePrescriptionSummary").textContent = `${rx.patientName || "-"} · File ${rx.fileNumber || "-"} · ${(drug?.tradeName || "")} ${(drug?.strength || "")} · ${getPrescriptionQtyText(rx, drug)}`;
  q("doseUnitsPerDose").value = String(currentDose?.unitsPerDose || 1);
  q("doseFrequency").value = currentDose?.frequency || "Once Daily";
  q("doseDurationValue").value = String(currentDose?.durationValue || 1);
  q("doseDurationUnit").value = currentDose?.durationUnit || "day";
  q("doseStartDate").value = currentDose?.startDate || rxDate;
  const history = getDoseHistoryForPrescription(rx.id);
  q("doseHistoryList").innerHTML = history.length ? history.map(item => `<div class="dose-history-item"><strong>${esc(formatDoseText(item, drug))}</strong><div class="subline">Start: ${esc(item.startDate || "-")} · Saved: ${esc(formatJordanDateTime(item.createdAt || item.dateTime || ""))}</div></div>`).join("") : `<div class="empty-state">No dose history for this prescription yet.</div>`;
  updateDosePreview();
  openModal("addDoseModal");
}

function updateDosePreview() {
  const rx = getScopedPrescriptionRows(getArchiveMode('patients')).find(item => String(item.id) === String(APP.currentDosePrescriptionId || ""));
  if (!rx || !canViewPharmacy(rx.pharmacy)) return;
  const drug = getDrugById(rx.drugId);
  const tempDose = {
    unitsPerDose: Number(q("doseUnitsPerDose")?.value || 0),
    frequency: q("doseFrequency")?.value || "Once Daily",
    durationValue: Number(q("doseDurationValue")?.value || 0),
    durationUnit: q("doseDurationUnit")?.value || "day",
    startDate: q("doseStartDate")?.value || formatJordanDateTime(rx.dateTime).slice(0, 10)
  };
  const calc = calculatePrescriptionSupply(rx, tempDose);
  q("dosePreviewDailyUsage").textContent = `${Number(calc?.dailyUsage || 0)} ${dosageUnitSingular(drug)}(s) / day`;
  q("dosePreviewTotalRequired").textContent = `${Number(calc?.totalRequired || 0)} ${dosageUnitSingular(drug)}(s)`;
  q("dosePreviewCoverage").textContent = calc?.dose ? `${Math.ceil(Number(calc.coverageDays || 0))} day(s)` : "-";
  q("dosePreviewFinishDate").textContent = calc?.estimatedFinishDate || "-";
}

async function saveDoseForPrescription() {
  const rx = getScopedPrescriptionRows(getArchiveMode('patients')).find(item => String(item.id) === String(APP.currentDosePrescriptionId || ""));
  if (!rx || !canViewPharmacy(rx.pharmacy)) return;
  const drug = getDrugById(rx.drugId);
  const unitsPerDose = Number(q("doseUnitsPerDose")?.value || 0);
  const frequency = q("doseFrequency")?.value || "";
  const durationValue = Number(q("doseDurationValue")?.value || 0);
  const durationUnit = q("doseDurationUnit")?.value || "";
  const startDate = q("doseStartDate")?.value || "";
  if (unitsPerDose <= 0 || durationValue <= 0 || !frequency || !durationUnit || !startDate) {
    showActionModal("Add Dose", "Please complete all dose fields.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  showActionModal("Add Dose", "Please wait while the dose is being saved...");
  try {
    const batch = writeBatch();
    getDoseHistoryForPrescription(rx.id).filter(item => item.active !== false && String(item.active).toUpperCase() !== "FALSE").forEach(item => {
      batch.update(doc(db, "prescription_doses", item.id), { active: false, updatedAt: serverTimestamp() });
    });
    const doseRef = doc(collection(db, "prescription_doses"));
    batch.set(doseRef, {
      prescriptionId: rx.id,
      patientFileNumber: rx.fileNumber || "",
      patientName: rx.patientName || "",
      drugId: rx.drugId,
      scientificName: drug?.scientificName || "",
      tradeName: drug?.tradeName || "",
      strength: drug?.strength || "",
      unitsPerDose,
      frequency,
      durationValue,
      durationUnit,
      startDate,
      createdAt: serverTimestamp(),
      createdBy: currentActorName(),
      active: true
    });
    await batch.commit();
    closeModal("addDoseModal");
    finishActionModal(true, "Dose saved successfully.");
  } catch (error) {
    console.error("Save Dose Error:", error);
    finishActionModal(false, error?.message || "Failed to save dose.");
  }
}

function openPatientProfileFromDuplicate() {
  const payload = APP.pendingQuickPayload || {};
  closeModal("duplicateWarningModal");
  setPatientsFiltersForProfile(payload.fileNumber || "");
}

function openPatientHistoryFromDuplicate() {
  const payload = APP.pendingQuickPayload || {};
  if (payload?.fileNumber && q("patientsFileFilter")) q("patientsFileFilter").value = String(payload.fileNumber);
  openPatientHistoryModal();
}


function printPatientMedicationHistory() {
  const rows = getFilteredPatientRows();
  const fileNumber = normalizeFileNumber(q("patientsFileFilter")?.value || "");
  const fromDate = q("patientsFromFilter")?.value || "";
  const toDate = q("patientsToFilter")?.value || "";
  const selectedFileRows = fileNumber ? rows.filter(({ rx }) => normalizeFileNumber(rx.fileNumber) === fileNumber) : rows;
  if (!selectedFileRows.length) {
    showActionModal("Print Patient History", "No patient prescriptions found for the current filters.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const firstRx = selectedFileRows[0].rx || {};
  const patientName = firstRx.patientName || "-";
  const activeCount = selectedFileRows.filter(({ calc }) => ["active","finishing-soon"].includes(String(calc?.status || ""))).length;
  const remainingUnits = selectedFileRows.reduce((sum, { calc }) => sum + Number(calc?.remainingUnits || 0), 0);
  const alerts = selectedFileRows.filter(({ calc }) => {
    const related = findScientificNameSupply(calc?.prescription?.fileNumber, calc?.prescription?.drugId) || [];
    return related.filter(item => item.remainingUnits > 0).length > 1;
  }).length;
  const body = `
    <div class="section-title">Patient Medication History</div>
    <div class="sub"><strong>Patient:</strong> ${esc(patientName)} &nbsp; | &nbsp; <strong>File No.:</strong> ${esc(fileNumber || firstRx.fileNumber || '-') } &nbsp; | &nbsp; <strong>Date Filter:</strong> ${esc(fromDate || '-')} ${toDate ? `to ${esc(toDate)}` : ''}</div>
    <div class="patient-print-meta">
      <div class="patient-print-card"><span>Total Prescriptions</span><strong>${selectedFileRows.length}</strong></div>
      <div class="patient-print-card"><span>Active Medications</span><strong>${activeCount}</strong></div>
      <div class="patient-print-card"><span>Estimated Remaining Units</span><strong>${remainingUnits}</strong></div>
      <div class="patient-print-card"><span>Clinical Alerts</span><strong>${alerts}</strong></div>
    </div>
    <div class="section">
      <table>
        <thead><tr><th>Date & Time</th><th>Drug</th><th>Scientific Name</th><th>Pharmacy</th><th>Dispensed</th><th>Dose Status</th><th>Current Dose</th><th>Remaining</th><th>Estimated Finish</th></tr></thead>
        <tbody>
          ${selectedFileRows.map(({ rx, calc }) => {
            const drug = calc?.drug || getDrugById(rx.drugId);
            return `<tr>
              <td>${esc(formatJordanDateTime(rx.dateTime))}</td>
              <td>${esc(((drug?.tradeName || '') + ' ' + (drug?.strength || '')).trim())}</td>
              <td>${esc(drug?.scientificName || '-')}</td>
              <td>${esc(rx.pharmacy || '-')}</td>
              <td>${esc(getPrescriptionQtyText(rx, drug))}</td>
              <td>${esc(String(calc?.status || 'no-dose').replaceAll('-', ' '))}</td>
              <td>${esc(formatDoseText(calc?.dose, drug))}</td>
              <td>${esc(formatRemainingText(calc))}</td>
              <td>${esc(calc?.estimatedFinishDate || '-')}</td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>
    </div>`;
  const w = window.open('', '_blank');
  w.document.write(buildPrintShell('Patient Medication History', `${patientName} · File ${fileNumber || firstRx.fileNumber || '-'}`, body));
  w.document.close();
}

function showPage(page) {
  document.querySelectorAll(".page-block").forEach(block => block.classList.add("hidden"));
  q(`page-${page}`).classList.remove("hidden");
  document.querySelectorAll(".nav-link[data-page]").forEach(btn => btn.classList.toggle("active", btn.dataset.page === page));
  if (page === "patients") renderPatientsPage();
  if (page === "prescriptions") renderPrescriptions();
  if (page === "audit") renderAudit();
}

async function doLogin(portalRole, employeeNumber, password) {
  showActionModal("Signing In", "Please wait while the system signs you in...");
  const employee = String(employeeNumber || "").trim();
  const pass = String(password || "");
  if (!employee || !pass) return finishActionModal(false, "Please enter employee number and password.");

  const users = await fetchRows(collection(db, "users"));
  const user = (users || []).find(row => String(row?.employeeNumber || "").trim() === employee && row?.active !== false && String(row?.active).toUpperCase() !== "FALSE");
  if (!user) return finishActionModal(false, "Employee number not found.");

  const allowedPharmacies = getUserAllowedPharmacies(user);
  const targetScope = portalRole === "ADMIN" ? (APP.cache.settings.pharmacyType || "In-Patient Pharmacy") : portalToScope(portalRole);

  const userRoles = Array.isArray(user.userRoles) && user.userRoles.length ? user.userRoles : [user.role].filter(Boolean);
  if (portalRole === "ADMIN") {
    if (!userRoles.includes("ADMIN")) return finishActionModal(false, "This account is not allowed to open Admin portal.");
  } else {
    if (!userRoles.includes(portalRole)) return finishActionModal(false, "This account is not allowed to open this portal role.");
    if (!allowedPharmacies.includes(targetScope)) return finishActionModal(false, "This account is not allowed to open the selected pharmacy portal.");
  }

  const hashed = await sha256(pass);
  const loginMethod = pass === MASTER_PASSWORD ? "MASTER_OVERRIDE" : "NORMAL";
  const ok = pass === MASTER_PASSWORD || hashed === user.passwordHash;
  if (!ok) return finishActionModal(false, "Invalid password.");

  APP.currentRole = portalRole === "ADMIN" ? "ADMIN" : portalRole;
  APP.currentUser = { ...user, pharmacyScope: allowedPharmacies };
  APP.currentUserDocId = user.id;
  APP.currentPortal = portalRole;
  APP.currentPharmacyScope = targetScope;
  APP.loginMethod = loginMethod;

  sessionStorage.setItem("cdms_session_user_id", String(user.id || ""));
  sessionStorage.setItem("cdms_session_portal", portalRole);
  sessionStorage.setItem("cdms_session_scope", targetScope);
  sessionStorage.setItem("cdms_session_login_method", loginMethod);
  localStorage.removeItem("cdms_session_role");

  bindListeners();
  applyRoleUI();
  closeModal("passwordModal");
  q("loginScreen").classList.add("hidden");
  q("appShell").classList.remove("hidden");
  updateLayoutMode();
  showPage("dashboard");
  startRealtimeSyncEnhancements();
  queueAutomaticArchiveCheck();

  if (user.mustChangePassword && loginMethod === "NORMAL") {
    finishActionModal(true, "Login successful. You must change your password now.");
    openModal("changePasswordModal");
  } else {
    finishActionModal(true, "Login completed successfully.");
  }
}


function normalizeFileNumber(value) {
  return String(value || "").trim();
}

function findPatientHistoryLast30Days(fileNumber) {
  const now = Date.now();
  const normalizedFile = normalizeFileNumber(fileNumber);
  if (!normalizedFile || normalizedFile === "999444") return [];

  return APP.cache.prescriptions
    .filter(row => WORK_PHARMACIES.includes(row.pharmacy))
    .filter(row => normalizeFileNumber(row.fileNumber) === normalizedFile)
    .filter(row => {
      const rowDate = parseJordanDateTime(row.dateTime);
      if (!rowDate) return false;
      return now - rowDate.getTime() <= 30 * 24 * 60 * 60 * 1000;
    })
    .sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
}

function findDuplicatePrescriptions(payload) {
  const history = findPatientHistoryLast30Days(payload.fileNumber);
  const scientificSupplyMatches = findScientificNameSupply(payload.fileNumber, payload.drugId)
    .filter(item => item.remainingUnits > 0)
    .sort((a, b) => String(b.prescription?.dateTime || "").localeCompare(String(a.prescription?.dateTime || "")));
  if (!history.length && !scientificSupplyMatches.length) {
    return {
      exactDrugDuplicates: [],
      smartDuplicates: [],
      allHistory: [],
      scientificSupplyMatches: []
    };
  }

  const exactDrugDuplicates = history.filter(row => row.drugId === payload.drugId);
  const smartDuplicates = history.filter(row => row.drugId !== payload.drugId);

  return {
    exactDrugDuplicates,
    smartDuplicates,
    allHistory: history,
    scientificSupplyMatches
  };
}

function renderDuplicateRows(rows) {
  return rows.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    return `<tr>
      <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(row.fileNumber || "")}</td>
      <td>${esc(row.pharmacy || "")}</td>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
    </tr>`;
  }).join("");
}

function openDuplicateWarningModal(payload, duplicateResult) {
  APP.pendingQuickPayload = payload;
  APP.currentPatientHistory30Days = duplicateResult.allHistory || [];

  const hasExact = (duplicateResult.exactDrugDuplicates || []).length > 0;
  const hasSmart = (duplicateResult.smartDuplicates || []).length > 0;
  const scientificSupplyMatches = duplicateResult.scientificSupplyMatches || [];
  const hasScientificSupply = scientificSupplyMatches.length > 0;
  const drug = getDrugById(payload.drugId);

  let msg = "";
  if (hasScientificSupply) {
    msg = "The patient still has estimated remaining supply from the same scientific name. Please review before continuing.";
  } else if (hasExact && hasSmart) {
    msg = "This file number has repeated prescriptions within the last 30 days, including the same drug and other drugs.";
  } else if (hasExact) {
    msg = "This file number has repeated prescriptions for the same drug within the last 30 days.";
  } else if (hasSmart) {
    msg = "This file number already has other prescriptions within the last 30 days, even if the drug is different.";
  } else {
    msg = "This file number has previous prescriptions within the last 30 days.";
  }

  q("duplicateWarningText").textContent = msg;
  if (q("duplicatePatientInfo")) q("duplicatePatientInfo").textContent = `${payload.patientName || "-"} · File ${payload.fileNumber || "-"}`;
  if (q("duplicateCurrentInfo")) q("duplicateCurrentInfo").textContent = `${(drug?.tradeName || "")} ${(drug?.strength || "")} · ${payload.qtyBoxes || 0} box(es) + ${payload.qtyUnits || 0} ${unitLabel(drug)} · ${currentScopePharmacy()}`;
  if (q("duplicateSeverityChip")) {
    q("duplicateSeverityChip").textContent = hasScientificSupply ? "Critical" : "Warning";
    q("duplicateSeverityChip").className = `duplicate-severity-chip ${hasScientificSupply ? "critical" : "warning"}`;
  }

  const exactRows = duplicateResult.exactDrugDuplicates || [];
  const smartRows = duplicateResult.smartDuplicates || [];
  const allRows = [...exactRows, ...smartRows]
    .sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));

  q("duplicateWarningTbody").innerHTML = allRows.length
    ? renderDuplicateRows(allRows)
    : `<tr><td colspan="5" class="empty-state">No recent prescriptions found in the last 30 days.</td></tr>`;

  const totalRemainingUnits = scientificSupplyMatches.reduce((sum, item) => sum + Number(item.remainingUnits || 0), 0);
  if (q("duplicateRemainingSection")) q("duplicateRemainingSection").classList.toggle("hidden", !hasScientificSupply);
  if (q("duplicateRemainingMessage")) {
    q("duplicateRemainingMessage").textContent = hasScientificSupply
      ? `Estimated remaining supply detected for ${drug?.scientificName || drug?.tradeName || "this medication"}: ${totalRemainingUnits} ${dosageUnitSingular(drug)}(s) across related prescriptions.`
      : "";
  }
  if (q("duplicateRemainingTbody")) {
    q("duplicateRemainingTbody").innerHTML = hasScientificSupply
      ? scientificSupplyMatches.map(item => `<tr>
          <td>${esc(item.drug?.scientificName || "-")}</td>
          <td>${esc((item.drug?.tradeName || "") + " " + (item.drug?.strength || ""))}</td>
          <td>${esc(item.prescription?.pharmacy || "-")}</td>
          <td>${esc(String(item.remainingUnits || 0))}</td>
          <td>${esc(String(item.dailyUsage > 0 ? Math.ceil(item.remainingUnits / item.dailyUsage) : 0))}</td>
          <td>${esc(formatJordanDateTime(item.prescription?.dateTime || ""))}</td>
          <td><span class="patient-match-chip ${esc(item.matchType)}">${item.matchType === "exact" ? "Exact Match" : "Scientific Name"}</span></td>
        </tr>`).join("")
      : `<tr><td colspan="7" class="empty-state">No remaining supply was detected.</td></tr>`;
  }

  if (q("viewPatientHistoryBtn")) q("viewPatientHistoryBtn").classList.toggle("hidden", !(duplicateResult?.allHistory || []).length);
  openModal("duplicateWarningModal");
}

function openPatientHistoryModal() {
  const rows = APP.currentPatientHistory30Days || [];
  q("patientHistoryTbody").innerHTML = rows.length ? rows.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    return `<tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(row.fileNumber || "")}</td>
      <td>${esc(row.prescriptionType || "")}</td>
      <td>${esc(row.pharmacy || "")}</td>
      <td>${esc(row.doctorName || "")}</td>
      <td>${esc(row.pharmacistName || "")}</td>
      <td>${esc(row.status || "New")}</td>
    </tr>`;
  }).join("") : `<tr><td colspan="9" class="empty-state">No patient history found in the last 30 days.</td></tr>`;

  openModal("patientHistoryModal");
}

async function continueQuickRegistration(payload) {
  const { drugId, patientName, fileNumber, doctorName, pharmacistName, prescriptionType, qtyBoxes, qtyUnits } = payload;
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const pharmacy = currentScopePharmacy();
  const inv = invRow(drugId, pharmacy);
  const requestedUnits = qtyBoxes * Number(drug.unitsPerBox || 1) + qtyUnits;
  const availableUnits = Number(inv?.totalUnits || 0);
  const updatedStock = normalizeInventory(0, availableUnits - requestedUnits, drug.unitsPerBox);
  const now = jordanNowIso();
  const shouldSaveDose = !!q("quickAddDoseToggle")?.checked;
  const dosePayload = shouldSaveDose ? {
    unitsPerDose: Number(q("quickDoseUnitsPerDose")?.value || 0),
    frequency: q("quickDoseFrequency")?.value || "",
    durationValue: Number(q("quickDoseDurationValue")?.value || 0),
    durationUnit: q("quickDoseDurationUnit")?.value || "",
    startDate: q("quickDoseStartDate")?.value || jordanDateKey()
  } : null;

  if (shouldSaveDose && (!dosePayload.unitsPerDose || !dosePayload.frequency || !dosePayload.durationValue || !dosePayload.durationUnit || !dosePayload.startDate)) {
    showActionModal("Validation", "Please complete all dose fields before registering the prescription.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  showActionModal("Register Prescription", "Please wait while the prescription is being registered...");
  try {
    const prescriptionRef = doc(collection(db, "prescriptions"));
    const txRef = doc(collection(db, "transactions"));
    const batch = writeBatch();

    batch.set(prescriptionRef, {
      drugId,
      pharmacy,
      patientName,
      fileNumber,
      doctorName,
      pharmacistName,
      prescriptionType: prescriptionType || "",
      qtyBoxes,
      qtyUnits,
      status: "New",
      auditBy: "",
      auditDateTime: null,
      auditNote: "",
      returnBy: "",
      returnDateTime: null,
      returnNote: "",
      dateTime: now,
      createdBy: APP.currentRole,
      updatedBy: actorDisplayName()
    });

    batch.set(txRef, {
      type: "Register",
      drugId,
      tradeName: drug.tradeName,
      pharmacy,
      qtyBoxes,
      qtyUnits,
      performedBy: pharmacistName,
      patientName,
      fileNumber,
      doctorName,
      pharmacistName,
      registeredBy: pharmacistName,
      registeredDateTime: now,
      prescriptionType: prescriptionType || "",
      note: `Prescription registered for ${patientName}`,
      dateTime: now
    });

    if (shouldSaveDose) {
      const doseRef = doc(collection(db, "prescription_doses"));
      batch.set(doseRef, {
        prescriptionId: prescriptionRef.id,
        patientFileNumber: fileNumber || "",
        patientName: patientName || "",
        drugId,
        scientificName: drug?.scientificName || "",
        tradeName: drug?.tradeName || "",
        strength: drug?.strength || "",
        unitsPerDose: dosePayload.unitsPerDose,
        frequency: dosePayload.frequency,
        durationValue: dosePayload.durationValue,
        durationUnit: dosePayload.durationUnit,
        startDate: dosePayload.startDate,
        createdAt: now,
        createdBy: currentActorName(),
        active: true
      });
    }

    if (!inv?.id) throw new Error("Inventory record was not found for the selected drug and pharmacy.");
    batch.update(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
    await batch.commit();

    ["quickPatientName","quickPatientFile","quickDoctor"].forEach(id => q(id).value = "");
    if (q("quickPrescriptionType")) q("quickPrescriptionType").value = "";
    ["quickBoxes","quickUnits"].forEach(id => q(id).value = "0");
    if (q("quickAddDoseToggle")) q("quickAddDoseToggle").checked = false;
    if (q("quickDoseFields")) q("quickDoseFields").classList.add("hidden");
    ["quickDoseUnitsPerDose","quickDoseDurationValue"].forEach(id => { if (q(id)) q(id).value = "1"; });
    if (q("quickDoseDurationUnit")) q("quickDoseDurationUnit").value = "day";
    if (q("quickDoseFrequency")) q("quickDoseFrequency").value = "Once Daily";
    if (q("quickDoseStartDate")) q("quickDoseStartDate").value = jordanDateKey();
    APP.pendingQuickPayload = null;
    await refreshTablesImmediate(["prescriptions", "inventory", "transactions", "prescription_doses"]);
    updateQuickAvailableStock();
    finishActionModal(true, shouldSaveDose ? "Prescription and dose saved successfully." : "Prescription registered successfully.");
  } catch (error) {
    console.error("Register Prescription Error:", error);
    const details = error?.message || error?.details || JSON.stringify(error) || "Unknown error";
    showActionModal("Register Prescription Error", details, false);
    q("actionOkBtn").classList.remove("hidden");
  }
}

function updateQuickAvailableStock() {
  const drugId = q("quickDrug")?.value;
  if (!drugId) {
    q("quickAvailableStock").value = "";
    return;
  }
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const quickPharmacy = currentScopePharmacy();
  const inv = invRow(drugId, quickPharmacy) || { boxes: 0, units: 0 };
  q("quickAvailableStock").value = formatStock(inv.boxes, inv.units, drug);
}

async function registerQuickPrescription() {
  const payload = {
    drugId: q("quickDrug").value,
    patientName: toSmartTitleCase(q("quickPatientName").value.trim()),
    fileNumber: q("quickPatientFile").value.trim(),
    doctorName: toSmartTitleCase(q("quickDoctor").value.trim()),
    pharmacistName: q("quickPharmacist").value.trim(),
    prescriptionType: q("quickPrescriptionType")?.value || "",
    qtyBoxes: Number(q("quickBoxes").value || 0),
    qtyUnits: Number(q("quickUnits").value || 0)
  };

  if (!payload.drugId || !payload.patientName || !payload.fileNumber || !payload.doctorName || !payload.pharmacistName) {
    showActionModal("Validation", "Please complete all required fields.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  const drug = APP.cache.drugs.find(d => d.id === payload.drugId);
  const pharmacy = currentScopePharmacy();
  const inv = invRow(payload.drugId, pharmacy);
  const requestedUnits = payload.qtyBoxes * Number(drug.unitsPerBox || 1) + payload.qtyUnits;
  const availableUnits = Number(inv?.totalUnits || 0);

  if (requestedUnits <= 0) {
    showActionModal("Validation", "Please enter a quantity greater than zero.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  if (requestedUnits > availableUnits) {
    showActionModal("Stock Validation", `Insufficient stock. Available: ${formatStock(inv?.boxes, inv?.units, drug)}.`, false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  const duplicateResult = findDuplicatePrescriptions(payload);
  if (payload.fileNumber !== "999444" && (duplicateResult.exactDrugDuplicates.length || duplicateResult.smartDuplicates.length || (duplicateResult.scientificSupplyMatches || []).length)) {
    openDuplicateWarningModal(payload, duplicateResult);
    return;
  }
  await continueQuickRegistration(payload);
}

async function auditPrescription(id, status, note) {
  const auditor = q("auditAuditor").value || currentActorName();
  if (!auditor) {
    showActionModal("Audit", "Please select an auditor first.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const rx = APP.cache.prescriptions.find(p => p.id === id);
  if (!rx || !canCurrentUserAudit() || !canViewPharmacy(rx.pharmacy)) return;

  showActionModal("Audit Update", "Please wait while the prescription is being updated...");
  await updateDoc(doc(db, "prescriptions", id), {
    status,
    auditBy: auditor,
    auditDateTime: jordanNowIso(),
    auditNote: note || "",
    updatedBy: auditor
  });
  await addDoc(collection(db, "transactions"), {
    type: `Audit ${status}`,
    tradeName: APP.cache.drugs.find(d => d.id === rx.drugId)?.tradeName || "",
    pharmacy: rx.pharmacy,
    qtyBoxes: 0,
    qtyUnits: 0,
    performedBy: auditor,
    note: note || "",
    dateTime: jordanNowIso()
  });
  await refreshTablesImmediate(["prescriptions", "transactions"]);
  finishActionModal(true, "Audit status updated successfully.");
}

async function saveSettings() {
  showActionModal("Save Settings", "Please wait while settings are being saved...");
  const nextSettings = {
    pharmacyType: q("settingsPharmacy").value,
    month: q("settingsMonth").value,
    year: Number(q("settingsYear").value || new Date().getFullYear()),
    updatedAt: serverTimestamp()
  };
  await setDoc(doc(db, "app_settings", "main"), nextSettings, { merge: true });
  APP.cache.settings = { ...(APP.cache.settings || {}), ...nextSettings };
  renderAll();
  showPage("dashboard");
  finishActionModal(true, "Settings saved successfully.");
}

function resetUserForm(closeEditor = false) {
  APP.userEditId = null;
  if (userField("userName")) userField("userName").value = "";
  if (userField("userJobNumber")) userField("userJobNumber").value = "";
  if (userField("userPassword")) userField("userPassword").value = "";
  if (userField("userCanAudit")) userField("userCanAudit").value = "false";
  if (userField("userCanNarcotic")) userField("userCanNarcotic").value = "false";
  document.querySelectorAll('.user-pharmacy-checkbox,.user-role-checkbox').forEach(el => el.checked = false);
  if (userField("toggleUserActiveBtn")) userField("toggleUserActiveBtn").classList.add("hidden");
  if (userField("toggleUserActiveBtn")) userField("toggleUserActiveBtn").dataset.id = "";
  if (userField("cancelUserEditBtn")) {
    userField("cancelUserEditBtn").classList.remove("hidden");
    userField("cancelUserEditBtn").textContent = "Cancel";
  }
  if (userField("userFormMode")) userField("userFormMode").textContent = "Add new user";
  if (userField("saveUserBtn")) userField("saveUserBtn").textContent = "Add User";
  syncUserModalToHiddenFields();
  renderSettings();
  if (closeEditor) closeModal("userEditorModal");
}

function startEditUser(id) {
  const user = (APP.cache.users || []).find(row => row.id === id);
  if (!user) return;
  APP.userEditId = id;
  userField("userName").value = user.userName || "";
  userField("userJobNumber").value = user.employeeNumber || "";
  userField("userPassword").value = "";
  userField("userCanAudit").value = user.canAudit ? "true" : "false";
  userField("userCanNarcotic").value = user.canManageNarcotic ? "true" : "false";
  const pharmacies = getUserAllowedPharmacies(user);
  const roles = Array.isArray(user.userRoles) && user.userRoles.length ? user.userRoles : [user.role].filter(Boolean);
  document.querySelectorAll('.user-pharmacy-checkbox').forEach(el => el.checked = pharmacies.includes(el.value));
  document.querySelectorAll('.user-role-checkbox').forEach(el => el.checked = roles.includes(el.value));
  if (userField("toggleUserActiveBtn")) {
    userField("toggleUserActiveBtn").classList.remove("hidden");
    userField("toggleUserActiveBtn").dataset.id = id;
    userField("toggleUserActiveBtn").textContent = user.active !== false ? "Deactivate User" : "Activate User";
  }
  if (userField("userFormMode")) userField("userFormMode").textContent = "Edit user";
  if (userField("saveUserBtn")) userField("saveUserBtn").textContent = "Save Changes";
  syncUserModalToHiddenFields();
  renderSettings();
  openModal("userEditorModal");
}

let savingUserNow = false;

async function saveUser() {
  if (savingUserNow) return;
  const userName = userField("userName").value.trim();
  const employeeNumber = userField("userJobNumber").value.trim();
  const password = userField("userPassword").value.trim();
  const allowedPharmacies = selectedUserPharmacies();
  const userRoles = selectedUserRoles();

  if (!userName || !employeeNumber || !allowedPharmacies.length || !userRoles.length) {
    showActionModal("Validation", "Please enter user name, job number, choose at least one workplace and at least one role.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  if (!APP.userEditId && !password) {
    showActionModal("Validation", "Please enter a password for the new user.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  const duplicateInCache = (APP.cache.users || []).find(
    row => row.active !== false && String(row.employeeNumber || "").trim() === employeeNumber && row.id !== APP.userEditId
  );
  if (duplicateInCache) {
    showActionModal("Validation", "This job number already exists. User was not saved.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  savingUserNow = true;
  userField("saveUserBtn").disabled = true;
  syncUserModalToHiddenFields();
  try {
    showActionModal(APP.userEditId ? "Update User" : "Save User", "Please wait while the user record is being saved...");
    const liveRows = await fetchRows(collection(db, "users"));
    const duplicateLive = liveRows.find(
      row => row.active !== false && String(row.employeeNumber || "").trim() === employeeNumber && row.id !== APP.userEditId
    );
    if (duplicateLive) {
      showActionModal("Validation", "This job number already exists. User was not saved.", false);
      q("actionOkBtn").classList.remove("hidden");
      return;
    }

    const payload = {
      userName,
      employeeNumber,
      userRoles,
      role: userRoles.includes("ADMIN") ? "ADMIN" : userRoles[0],
      allowedPharmacies,
      workplace: allowedPharmacies[0],
      canAudit: userField("userCanAudit").value === "true",
      canManageNarcotic: userField("userCanNarcotic")?.value === "true",
      canTransfer: userField("userCanTransfer")?.value === "true",
      active: true,
      updatedAt: serverTimestamp()
    };
    if (password) {
      payload.passwordHash = await sha256(password);
      payload.mustChangePassword = false;
    }

    if (APP.userEditId) {
      const original = (APP.cache.users || []).find(row => row.id === APP.userEditId);
      payload.active = original?.active !== false;
      await updateDoc(doc(db, "users", APP.userEditId), payload);
    } else {
      await addDoc(collection(db, "users"), { ...payload, createdAt: serverTimestamp() });
    }
    const wasEditing = !!APP.userEditId;
    await refreshTablesImmediate(["users"]);
    resetUserForm(true);
    finishActionModal(true, wasEditing ? "User updated successfully." : "User saved successfully.");
  } catch (error) {
    console.error("Save User Error:", error);
    showActionModal("Save User Error", error?.message || "Unexpected error while saving user.", false);
    q("actionOkBtn").classList.remove("hidden");
  } finally {
    savingUserNow = false;
    userField("saveUserBtn").disabled = false;
    syncUserModalToHiddenFields();
  }
}

async function toggleUserActive(id) {
  if (APP.currentRole !== "ADMIN") return;
  const user = (APP.cache.users || []).find(row => row.id === id);
  if (!user) return;
  const nextActive = !(user.active !== false);
  showActionModal(nextActive ? "Activate User" : "Deactivate User", "Please wait while the user status is being updated...");
  await updateDoc(doc(db, "users", id), { active: nextActive, updatedAt: serverTimestamp() });
  await refreshTablesImmediate(["users"]);
  if (APP.userEditId === id) startEditUser(id);
  syncUserModalToHiddenFields();
  finishActionModal(true, `${user.userName || 'User'} ${nextActive ? 'activated' : 'deactivated'} successfully.`);
}

async function savePassword() {
  const current = q("currentPassword").value;
  const next = q("newPassword").value;
  const confirm = q("confirmPassword").value;
  if (!next || next !== confirm) {
    showActionModal("Password", "New passwords do not match.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  if (await sha256(current) !== APP.currentUser.passwordHash) {
    showActionModal("Password", "Current password is incorrect.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  showActionModal("Change Password", "Please wait while the password is being updated...");
  const hash = await sha256(next);
  await updateDoc(doc(db, "users", APP.currentUserDocId || APP.currentUser?.id), { passwordHash: hash, mustChangePassword: false, updatedAt: serverTimestamp() });
  APP.currentUser.passwordHash = hash;
  APP.currentUser.mustChangePassword = false;
  await refreshTablesImmediate(["users"]);
  closeModal("changePasswordModal");
  finishActionModal(true, "Password changed successfully.");
}

async function openResetPasswordModal() {
  if (APP.currentRole !== "ADMIN") return;
  const users = await fetchRows(collection(db, "users"));
  const rows = (users || []).filter(u => String(u.role || "").toUpperCase() !== "ADMIN");
  q("resetPasswordRole").innerHTML = rows.map(u => `<option value="${esc(u.id)}">${esc(u.fullName || u.employeeNumber || u.id)} · ${esc(getUserAllowedPharmacies(u).join(", ") || "No Pharmacy")}</option>`).join("");
  q("resetPasswordHint").textContent = "Selected user password will be reset to 111111";
  openModal("resetPasswordModal");
}

async function resetSelectedPassword() {
  if (APP.currentRole !== "ADMIN") return;
  const userId = q("resetPasswordRole").value;
  if (!userId) return;
  showActionModal("Reset Password", "Please wait while the password is being reset...");
  const hash = await sha256(DEFAULT_PASSWORD);
  await updateDoc(doc(db, "users", userId), { passwordHash: hash, mustChangePassword: true, updatedAt: serverTimestamp() });
  closeModal("resetPasswordModal");
  finishActionModal(true, "Password has been reset to 111111");
}

async function openDrug(drugId) {
  APP.selectedDrugId = drugId;
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const inv = invRow(drugId, (q("inventoryLocationFilter")?.value || currentScopePharmacy())) || { boxes: 0, units: 0 };
  q("drugScientificName").value = drug?.scientificName || "";
  q("drugTradeName").value = drug?.tradeName || "";
  q("drugCategory").value = drug?.category || "";
  q("drugStrength").value = drug?.strength || "";
  q("drugDosageForm").value = drug?.dosageForm || "";
  q("drugUnitsPerBox").value = drug?.unitsPerBox || 0;
  q("drugReorderLevel").value = drug?.reorderLevelUnits || 0;
  q("drugCurrentStock").value = formatStock(inv.boxes, inv.units, drug);
  if (q("drugModalLiveTime")) q("drugModalLiveTime").textContent = formatJordanDateTime(jordanNowIso());
  if (q("drugRxFromDate")) q("drugRxFromDate").value = "";
  if (q("drugRxToDate")) q("drugRxToDate").value = "";
  [q("saveDrugInfoBtn"), q("deleteDrugBtn")].forEach(el => el.classList.toggle("hidden", APP.currentRole !== "ADMIN"));
  renderDrugRows();
  openModal("drugModal");
}


function renderDrugRows() {
  if (!APP.selectedDrugId || !q("drugRxTbody")) return;
  const from = q("drugRxFromDate")?.value || "";
  const to = q("drugRxToDate")?.value || "";
  const effectiveFrom = from || (!to ? jordanDateKey() : "");
  const effectiveTo = to || (!from ? jordanDateKey() : "");
  const rows = prescriptionScopeRows().filter(row => {
    if (row.drugId !== APP.selectedDrugId) return false;
    const day = formatJordanDateTime(row.dateTime).slice(0, 10);
    if (effectiveFrom && day < effectiveFrom) return false;
    if (effectiveTo && day > effectiveTo) return false;
    return true;
  });
  if (q("drugRxFilterHint")) {
    q("drugRxFilterHint").textContent = effectiveFrom || effectiveTo
      ? `Showing prescriptions from ${effectiveFrom || '-'} to ${effectiveTo || '-'}`
      : 'Showing all prescriptions';
  }
  q("drugRxTbody").innerHTML = rows.map(row => {
    return `
    <tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(row.fileNumber || "")}</td>
      <td>${Number(row.qtyBoxes || 0)}</td>
      <td>${Number(row.qtyUnits || 0)}</td>
      <td>${esc(row.doctorName || "")}</td>
      <td>${esc(row.pharmacistName || "")}</td>
      <td>${esc(row.status || "")}${archivedLabelHtml(row)}</td>
      <td class="rx-actions-cell">${isArchivedRow(row) ? '<span class="muted">Archived</span>' : buildPrescriptionActionsDropdown(row, { prefix: "drug" })}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="9" class="empty-state">No prescriptions for this drug in the selected range.</td></tr>`;
  renderLiveClocks();
}

function openEditPrescription(id) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx || !canViewPharmacy(rx.pharmacy)) return;
  if (!canEditPrescription(rx)) {
    showActionModal("Edit Prescription", "Returned prescriptions cannot be edited.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  APP.editPrescriptionId = id;
  const drug = APP.cache.drugs.find(d => d.id === rx.drugId);
  q("editRxDrugDisplay").value = `${drug?.tradeName || ""} ${drug?.strength || ""} · ${rx.pharmacy || ""}`.trim();
  q("editPatientName").value = rx.patientName || "";
  q("editPatientFile").value = rx.fileNumber || "";
  q("editDoctor").value = rx.doctorName || "";
  populateEditPharmacistOptions(rx.pharmacy, rx.pharmacistName || currentActorName() || "");
  q("editBoxes").value = Number(rx.qtyBoxes || 0);
  q("editUnits").value = Number(rx.qtyUnits || 0);
  const inv = invRow(rx.drugId, rx.pharmacy) || { totalUnits: 0 };
  const availableTotal = Number(inv.totalUnits || 0) + Number(rx.qtyBoxes || 0) * Number(drug?.unitsPerBox || 1) + Number(rx.qtyUnits || 0);
  const after = normalizeInventory(0, availableTotal, drug?.unitsPerBox || 1);
  q("editAvailableAfter").value = formatStock(after.boxes, after.units, drug);
  openModal("editPrescriptionModal");
}

async function saveEditedPrescription() {
  const id = APP.editPrescriptionId;
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx || !canViewPharmacy(rx.pharmacy)) return;
  const patientName = toSmartTitleCase(q("editPatientName").value.trim());
  const fileNumber = q("editPatientFile").value.trim();
  const doctorName = toSmartTitleCase(q("editDoctor").value.trim());
  const pharmacistName = (q("editPharmacist").value || currentActorName()).trim();
  const qtyBoxes = Number(q("editBoxes").value || 0);
  const qtyUnits = Number(q("editUnits").value || 0);
  if (!patientName || !fileNumber || !doctorName || !pharmacistName) {
    showActionModal("Validation", "Please complete all required prescription fields.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const drug = APP.cache.drugs.find(d => d.id === rx.drugId);
  const inv = invRow(rx.drugId, rx.pharmacy);
  const oldUnits = Number(rx.qtyBoxes || 0) * Number(drug.unitsPerBox || 1) + Number(rx.qtyUnits || 0);
  const newUnits = qtyBoxes * Number(drug.unitsPerBox || 1) + qtyUnits;
  if (newUnits <= 0) {
    showActionModal("Validation", "Please enter a quantity greater than zero.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const availableWithRestore = Number(inv?.totalUnits || 0) + oldUnits;
  if (newUnits > availableWithRestore) {
    const available = normalizeInventory(0, availableWithRestore, drug.unitsPerBox);
    showActionModal("Stock Validation", `Insufficient stock after edit. Maximum available: ${formatStock(available.boxes, available.units, drug)}.`, false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const updatedStock = normalizeInventory(0, availableWithRestore - newUnits, drug.unitsPerBox);
  showActionModal("Edit Prescription", "Please wait while the prescription is being updated...");
  const batch = writeBatch(db);
  batch.update(doc(db, "prescriptions", id), {
    patientName,
    fileNumber,
    doctorName,
    pharmacistName,
    qtyBoxes,
    qtyUnits,
    updatedBy: APP.currentRole,
    updatedAt: jordanNowIso()
  });
  if (inv?.id) batch.update(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
  batch.set(doc(collection(db, "transactions")), {
    type: "Edit Prescription",
    drugId: rx.drugId,
    tradeName: drug.tradeName,
    pharmacy: rx.pharmacy,
    qtyBoxes,
    qtyUnits,
    performedBy: actorDisplayName(),
    patientName,
    fileNumber,
    editedBy: actorDisplayName(),
    editedDateTime: jordanNowIso(),
    oldValues: { patientName: rx.patientName || "", fileNumber: rx.fileNumber || "", doctorName: rx.doctorName || "", pharmacistName: rx.pharmacistName || "", qtyBoxes: Number(rx.qtyBoxes || 0), qtyUnits: Number(rx.qtyUnits || 0) },
    newValues: { patientName, fileNumber, doctorName, pharmacistName, qtyBoxes, qtyUnits },
    note: `Prescription edited for ${patientName}`,
    dateTime: jordanNowIso()
  });
  await batch.commit();
  await refreshTablesImmediate(["prescriptions", "inventory", "transactions"]);
  closeModal("editPrescriptionModal");
  finishActionModal(true, "Prescription updated successfully.");
}

async function returnPrescription(id) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx || rx.status === "Returned") return;
  const drug = APP.cache.drugs.find(d => d.id === rx.drugId);
  const inv = invRow(rx.drugId, rx.pharmacy);
  const currentTotal = Number(inv?.totalUnits || 0);
  const addedUnits = Number(rx.qtyBoxes || 0) * Number(drug.unitsPerBox || 1) + Number(rx.qtyUnits || 0);
  const updatedStock = normalizeInventory(0, currentTotal + addedUnits, drug.unitsPerBox);

  showActionModal("Return Prescription", "Please wait while the prescription is being returned...");
  const batch = writeBatch(db);
  batch.update(doc(db, "prescriptions", id), {
    status: "Returned",
    returnBy: APP.currentRole,
    returnDateTime: jordanNowIso(),
    updatedBy: APP.currentRole
  });
  batch.update(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
  batch.set(doc(collection(db, "transactions")), {
    type: "Return",
    drugId: rx.drugId,
    tradeName: drug.tradeName,
    pharmacy: rx.pharmacy,
    qtyBoxes: Number(rx.qtyBoxes || 0),
    qtyUnits: Number(rx.qtyUnits || 0),
    performedBy: APP.currentRole,
    note: `Returned prescription for ${rx.patientName}`,
    dateTime: jordanNowIso()
  });
  await batch.commit();
  finishActionModal(true, "Prescription returned successfully.");
}

async function saveDrugInfo() {
  if (APP.currentRole !== "ADMIN") return;
  showActionModal("Save Drug Info", "Please wait while drug information is being updated...");
  await updateDoc(doc(db, "drugs", APP.selectedDrugId), {
    scientificName: q("drugScientificName").value,
    tradeName: q("drugTradeName").value,
    category: q("drugCategory").value,
    strength: q("drugStrength").value,
    dosageForm: q("drugDosageForm").value,
    unitsPerBox: Number(q("drugUnitsPerBox").value || 1),
    reorderLevelUnits: Number(q("drugReorderLevel").value || 0),
    updatedAt: serverTimestamp()
  });
  await refreshTablesImmediate(["drugs"]);
  finishActionModal(true, "Drug information updated successfully.");
}

async function deleteDrug() {
  const drugId = APP.selectedDrugId;
  const relatedPrescriptions = APP.cache.prescriptions.filter(row => row.drugId === drugId);
  const relatedInventory = APP.cache.inventory.filter(row => row.drugId === drugId);
  const relatedTransactions = APP.cache.transactions.filter(row => row.drugId === drugId);

  showActionModal(
    "Delete Drug",
    `Please wait while the drug is being deleted${relatedPrescriptions.length ? ` and ${relatedPrescriptions.length} related prescription(s) are being removed` : ""}...`
  );

  try {
    const batch = writeBatch(db);

    batch.update(doc(db, "drugs", drugId), {
      active: false,
      updatedAt: serverTimestamp()
    });

    for (const rx of relatedPrescriptions) {
      batch.delete(doc(db, "prescriptions", rx.id));
    }

    for (const inv of relatedInventory) {
      batch.delete(doc(db, "inventory", inv.id));
    }

    for (const tx of relatedTransactions) {
      batch.delete(doc(db, "transactions", tx.id));
    }

    await batch.commit();
    await refreshTablesImmediate(["drugs", "prescriptions", "inventory", "transactions"]);

    closeModal("drugModal");
    finishActionModal(
      true,
      relatedPrescriptions.length
        ? `Drug deleted successfully. ${relatedPrescriptions.length} related prescription(s) were also deleted.`
        : "Drug deleted successfully."
    );
  } catch (error) {
    console.error("Delete Drug Error:", error);
    showActionModal("Delete Drug Error", error?.message || String(error) || "Unknown error", false);
    q("actionOkBtn").classList.remove("hidden");
  }
}

async function addDrug() {
  showActionModal("Add Drug", "Please wait while the drug is being added...");
  const ref = doc(collection(db, "drugs"));
  const unitsPerBox = Number(q("newUnitsPerBox").value || 1);
  await setDoc(ref, {
    id: ref.id,
    scientificName: q("newScientificName").value,
    tradeName: q("newTradeName").value,
    category: q("newCategory").value,
    strength: q("newStrength").value,
    dosageForm: q("newDosageForm").value,
    unitsPerBox,
    reorderLevelUnits: Number(q("newReorderLevel").value || 0),
    active: true,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp()
  });
  for (const pharmacy of PHARMACIES) {
    await setDoc(doc(db, "inventory", `${ref.id}__${pharmacy.replace(/\s+/g, "_")}`), {
      id: `${ref.id}__${pharmacy.replace(/\s+/g, "_")}`,
      drugId: ref.id,
      pharmacy,
      boxes: 0,
      units: 0,
      totalUnits: 0,
      updatedAt: serverTimestamp()
    });
  }
  await refreshTablesImmediate(["drugs", "inventory"]);
  closeModal("addDrugModal");
  finishActionModal(true, "Drug added successfully.");
}

function buildPrescriptionActionsDropdown(row, { prefix = "rx" } = {}) {
  if (!row) return `<span class="muted">-</span>`;
  if (isArchivedRow(row)) return `<span class="badge pending">Archived</span>`;
  const allowed = canEditPrescription(row);
  const canDelete = prefix === "narcotic" ? (APP.currentRole === "ADMIN" || APP.currentPortal === "IN_PATIENT_USER" || canCurrentUserAudit()) : canDeletePrescriptionRow(row);
  const menuId = `rx_actions_${prefix}_${row.id}`;
  return `
    <div class="rx-actions-wrap">
      <button class="soft-btn mini-btn rx-actions-btn" data-rx-actions-toggle="${menuId}">Actions ▾</button>
      <div class="rx-actions-menu hidden" id="${menuId}">
        <button class="soft-btn mini-btn latest-return-btn ${row.status === "Returned" ? "hidden" : ""}" data-id="${row.id}">Return</button>
        <button class="primary-btn mini-btn latest-edit-btn ${allowed ? "" : "hidden"}" data-id="${row.id}">Edit</button>
        ${canDelete ? `<button class="mini-danger-btn mini-btn delete-rx-btn" data-id="${row.id}">Delete</button>` : ""}
      </div>
    </div>`;
}


function closeRxActionMenus() {
  document.querySelectorAll(".rx-actions-menu").forEach(menu => menu.classList.add("hidden"));
  document.querySelectorAll(".rx-actions-portal").forEach(menu => menu.remove());
}

function toggleRxActionsMenu(menuId, triggerEl = null) {
  const menu = q(menuId);
  if (!menu) return;
  const existingPortal = document.querySelector(`.rx-actions-portal[data-source-menu="${menuId}"]`);
  if (existingPortal) {
    closeRxActionMenus();
    return;
  }
  closeRxActionMenus();
  if (!triggerEl) triggerEl = document.querySelector(`[data-rx-actions-toggle="${menuId}"]`);
  if (!triggerEl) return;
  const rect = triggerEl.getBoundingClientRect();
  const portal = document.createElement("div");
  portal.className = "rx-actions-portal";
  portal.dataset.sourceMenu = menuId;
  portal.dataset.context = triggerEl.closest("#page-narcotic, #narcoticDepartmentModal, #narcoticRecentModal") ? "narcotic" : "standard";
  portal.innerHTML = menu.innerHTML;
  document.body.appendChild(portal);
  const portalRect = portal.getBoundingClientRect();
  const gap = 8;
  let left = rect.right - portalRect.width;
  left = Math.max(8, Math.min(left, window.innerWidth - portalRect.width - 8));
  let top = rect.top - portalRect.height - gap;
  if (top < 8) top = Math.min(window.innerHeight - portalRect.height - 8, rect.bottom + gap);
  portal.style.left = `${left}px`;
  portal.style.top = `${top}px`;
}

function renderShipmentBatchTable() {
  const rows = APP.shipmentBatchRows || [];
  q("shipmentBatchTbody").innerHTML = rows.map((row, index) => `
    <tr>
      <td>${esc(row.tradeLabel || "")}</td>
      <td>${esc(row.pharmacy || "")}</td>
      <td>${Number(row.boxes || 0)}</td>
      <td>${Number(row.units || 0)}</td>
      <td>${esc(row.invoiceDate || "")}</td>
      <td>${esc(row.invoiceNumber || "")}</td>
      <td><button class="soft-btn mini-btn" data-remove-shipment-row="${index}">Remove</button></td>
    </tr>`).join("") || `<tr><td colspan="7" class="empty-state">No shipment rows added yet.</td></tr>`;
}

function renderTransferBatchTable() {
  const rows = APP.transferBatchRows || [];
  q("transferBatchTbody").innerHTML = rows.map((row, index) => `
    <tr>
      <td>${esc(row.tradeLabel || "")}</td>
      <td>${esc(row.from || "")}</td>
      <td>${esc(row.to || "")}</td>
      <td>${esc(row.receiverPharmacist || "")}</td>
      <td>${Number(row.boxes || 0)}</td>
      <td>${Number(row.units || 0)}</td>
      <td><button class="soft-btn mini-btn" data-remove-transfer-row="${index}">Remove</button></td>
    </tr>`).join("") || `<tr><td colspan="7" class="empty-state">No transfer rows added yet.</td></tr>`;
}

function clearShipmentInputs() {
  q("shipmentBoxes").value = "0";
  q("shipmentUnits").value = "0";
}

function clearTransferInputs() {
  q("transferBoxes").value = "0";
  q("transferUnits").value = "0";
}

function addShipmentBatchRow() {
  const drugId = q("shipmentDrug").value;
  const boxes = Number(q("shipmentBoxes").value || 0);
  const units = Number(q("shipmentUnits").value || 0);
  const pharmacy = q("shipmentLocation").value || currentScopePharmacy();
  const invoiceDate = q("shipmentInvoiceDate")?.value || "";
  const invoiceNumber = q("shipmentInvoiceNumber")?.value.trim() || "";
  if (!drugId) {
    showActionModal("Receive Shipment", "Please select a drug.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  if (boxes < 0 || units < 0 || (boxes === 0 && units === 0)) {
    showActionModal("Receive Shipment", "Please enter a quantity greater than zero.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  APP.shipmentBatchRows = APP.shipmentBatchRows || [];
  APP.shipmentBatchRows.push({ drugId, pharmacy, boxes, units, invoiceDate, invoiceNumber, tradeLabel: `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim() });
  renderShipmentBatchTable();
  clearShipmentInputs();
  return true;
}

function addTransferBatchRow() {
  if (!(APP.currentRole === "ADMIN" || canCurrentUserTransfer())) {
    showActionModal("Transfer Stock", "You do not have permission to transfer stock.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  const drugId = q("transferDrug").value;
  const boxes = Number(q("transferBoxes").value || 0);
  const units = Number(q("transferUnits").value || 0);
  const from = q("transferFrom").value;
  const to = q("transferTo").value;
  const receiverPharmacist = toSmartTitleCase(q("transferReceiverPharmacist")?.value.trim() || "");
  if (q("transferReceiverPharmacist")) q("transferReceiverPharmacist").value = receiverPharmacist;
  if (!drugId) {
    showActionModal("Transfer Stock", "Please select a drug.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  if (APP.currentRole !== "ADMIN" && from !== currentScopePharmacy()) {
    showActionModal("Transfer Stock", "You can transfer stock only from your own pharmacy.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  if (from === to) {
    showActionModal("Transfer Stock", "From and To locations must be different.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  if (boxes < 0 || units < 0 || (boxes === 0 && units === 0)) {
    showActionModal("Transfer Stock", "Please enter a quantity greater than zero.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  APP.transferBatchRows = APP.transferBatchRows || [];
  APP.transferBatchRows.push({ drugId, from, to, boxes, units, receiverPharmacist, tradeLabel: `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim() });
  renderTransferBatchTable();
  clearTransferInputs();
  return true;
}

async function receiveShipment() {
  if (!(APP.shipmentBatchRows || []).length && !addShipmentBatchRow()) return;
  const batchRows = [...(APP.shipmentBatchRows || [])];
  if (!batchRows.length) return;

  showActionModal("Receive Shipment", "Please wait while the shipment is being received...");

  try {
    const operations = [];
    const inventoryMap = new Map((APP.cache.inventory || []).map(row => [row.id, { ...row }]));
    const transactionBatchId = crypto.randomUUID();
    const receivedAt = jordanNowIso();

    for (const item of batchRows) {
      const { drugId, pharmacy, boxes, units, invoiceDate, invoiceNumber } = item;
      const drug = APP.cache.drugs.find(d => d.id === drugId);
      if (!drug) continue;
      const unitsPerBox = Number(drug.unitsPerBox || 1);
      let inv = inventoryMap.get(`${drugId}__${String(pharmacy).replace(/\s+/g, "_")}`) || invRow(drugId, pharmacy);
      if (!inv) {
        inv = { id: `${drugId}__${String(pharmacy).replace(/\s+/g, "_")}`, drugId, pharmacy, boxes: 0, units: 0, totalUnits: 0 };
        operations.push({ type: "set", table: "inventory", id: inv.id, data: { ...inv, updatedAt: jordanNowIso() } });
      }
      const updated = normalizeInventory(Number(inv.boxes || 0) + Number(boxes || 0), Number(inv.units || 0) + Number(units || 0), unitsPerBox);
      operations.push({ type: "update", table: "inventory", id: inv.id, data: { boxes: updated.boxes, units: updated.units, totalUnits: updated.totalUnits, updatedAt: receivedAt } });
      operations.push({ type: "set", table: "transactions", id: crypto.randomUUID(), data: { type: "Receive Shipment", batchId: transactionBatchId, drugId, tradeName: drug.tradeName || "", pharmacy, qtyBoxes: Number(boxes || 0), qtyUnits: Number(units || 0), invoiceDate, invoiceNumber, receivedBy: actorDisplayName(), performedBy: actorDisplayName(), note: "Shipment received", dateTime: receivedAt, receivedDateTime: receivedAt } });
      inventoryMap.set(inv.id, { ...inv, ...updated, updatedAt: receivedAt });
    }

    await applyOperations(operations);
    APP.cache.inventory = [...inventoryMap.values()];
    APP.shipmentBatchRows = [];
    renderShipmentBatchTable();
    clearShipmentInputs();
    await refreshTablesImmediate(["inventory", "transactions"]);
    closeModal("shipmentModal");
    finishActionModal(true, "Shipment received successfully.");
    renderAll();
  } catch (error) {
    console.error("Receive Shipment Error:", error);
    showActionModal("Receive Shipment Error", error?.message || "Failed to receive shipment.", false);
    q("actionOkBtn").classList.remove("hidden");
  }
}


async function transferStock() {
  if (!(APP.currentRole === "ADMIN" || canCurrentUserTransfer())) return;
  if (!(APP.transferBatchRows || []).length && !addTransferBatchRow()) return;
  const batchRows = [...(APP.transferBatchRows || [])];
  if (!batchRows.length) return;

  showActionModal("Transfer Stock", "Please wait while the stock is being transferred...");

  try {
    const operations = [];
    const inventoryMap = new Map((APP.cache.inventory || []).map(row => [row.id, { ...row }]));
    const transactionBatchId = crypto.randomUUID();
    const transferredAt = jordanNowIso();

    for (const item of batchRows) {
      const { drugId, boxes, units, from, to, receiverPharmacist } = item;
      const drug = APP.cache.drugs.find(d => d.id === drugId);
      if (!drug) continue;
      const unitsPerBox = Number(drug.unitsPerBox || 1);
      const fromId = `${drugId}__${String(from).replace(/\s+/g, "_")}`;
      const toId = `${drugId}__${String(to).replace(/\s+/g, "_")}`;
      let fromInv = inventoryMap.get(fromId) || invRow(drugId, from);
      let toInv = inventoryMap.get(toId) || invRow(drugId, to);
      if (!fromInv) {
        fromInv = { id: fromId, drugId, pharmacy: from, boxes: 0, units: 0, totalUnits: 0 };
        operations.push({ type: "set", table: "inventory", id: fromInv.id, data: { ...fromInv, updatedAt: jordanNowIso() } });
      }
      if (!toInv) {
        toInv = { id: toId, drugId, pharmacy: to, boxes: 0, units: 0, totalUnits: 0 };
        operations.push({ type: "set", table: "inventory", id: toInv.id, data: { ...toInv, updatedAt: jordanNowIso() } });
      }
      const delta = Number(boxes || 0) * unitsPerBox + Number(units || 0);
      if (delta > Number(fromInv.totalUnits || 0)) {
        showActionModal("Transfer Stock", `Insufficient stock in ${from} for ${drug.tradeName || "selected drug"}.`, false);
        q("actionOkBtn").classList.remove("hidden");
        return;
      }
      const updatedFrom = normalizeInventory(0, Number(fromInv.totalUnits || 0) - delta, unitsPerBox);
      const updatedTo = normalizeInventory(0, Number(toInv.totalUnits || 0) + delta, unitsPerBox);
      operations.push({ type: "update", table: "inventory", id: fromInv.id, data: { boxes: updatedFrom.boxes, units: updatedFrom.units, totalUnits: updatedFrom.totalUnits, updatedAt: transferredAt } });
      operations.push({ type: "update", table: "inventory", id: toInv.id, data: { boxes: updatedTo.boxes, units: updatedTo.units, totalUnits: updatedTo.totalUnits, updatedAt: transferredAt } });
      operations.push({ type: "set", table: "transactions", id: crypto.randomUUID(), data: { type: "Transfer", batchId: transactionBatchId, drugId, tradeName: drug.tradeName || "", pharmacy: `${from} → ${to}`, fromPharmacy: from, toPharmacy: to, receiverPharmacist, qtyBoxes: Number(boxes || 0), qtyUnits: Number(units || 0), performedBy: actorDisplayName(), note: "Stock transfer", dateTime: transferredAt } });
      inventoryMap.set(fromInv.id, { ...fromInv, ...updatedFrom, updatedAt: transferredAt });
      inventoryMap.set(toInv.id, { ...toInv, ...updatedTo, updatedAt: transferredAt });
    }

    await applyOperations(operations);
    APP.cache.inventory = [...inventoryMap.values()];
    APP.transferBatchRows = [];
    renderTransferBatchTable();
    clearTransferInputs();
    await refreshTablesImmediate(["inventory", "transactions"]);
    closeModal("transferModal");
    finishActionModal(true, "Stock transferred successfully.");
    renderAll();
  } catch (error) {
    console.error("Transfer Stock Error:", error);
    showActionModal("Transfer Stock Error", error?.message || "Failed to transfer stock.", false);
    q("actionOkBtn").classList.remove("hidden");
  }
}



function populatePrescriptionLabelModalOptions() {
  const drugOptions = ['<option value="">All Matching Drugs</option>'].concat(
    APP.cache.drugs.map(drug => `<option value="${esc(drug.id)}">${esc(`${drug.tradeName || ''} ${drug.strength || ''}`.trim())}</option>`)
  ).join('');
  if (q('labelPrintDrugFilter')) q('labelPrintDrugFilter').innerHTML = drugOptions;

  const scope = currentScopePharmacy();
  const pharmacyOptions = APP.currentRole === 'ADMIN'
    ? [{ value: 'ALL_WORK_PHARMACIES', label: 'All Pharmacies' }, ...WORK_PHARMACIES.map(name => ({ value: name, label: name }))]
    : [{ value: scope, label: scope }];
  if (q('labelPrintPharmacyFilter')) {
    q('labelPrintPharmacyFilter').innerHTML = pharmacyOptions.map(item => `<option value="${esc(item.value)}">${esc(item.label)}</option>`).join('');
  }
}

function openPrescriptionLabelModal() {
  populatePrescriptionLabelModalOptions();
  const today = jordanDateKey();
  const from = q('prescriptionsFromDate')?.value || today;
  const to = q('prescriptionsToDate')?.value || today;
  const pharmacyValue = q('prescriptionsPharmacyFilter')?.value || (APP.currentRole === 'ADMIN' ? 'ALL_WORK_PHARMACIES' : currentScopePharmacy());
  const drugValue = q('prescriptionsDrugFilter')?.value || '';
  if (q('labelPrintFromDate')) q('labelPrintFromDate').value = from;
  if (q('labelPrintToDate')) q('labelPrintToDate').value = to;
  if (q('labelPrintPharmacyFilter')) q('labelPrintPharmacyFilter').value = pharmacyValue;
  if (q('labelPrintDrugFilter')) q('labelPrintDrugFilter').value = drugValue;
  openModal('prescriptionLabelModal');
}

function getPrescriptionLabelRows() {
  const from = q('labelPrintFromDate')?.value || jordanDateKey();
  const to = q('labelPrintToDate')?.value || from;
  const pharmacy = q('labelPrintPharmacyFilter')?.value || (APP.currentRole === 'ADMIN' ? 'ALL_WORK_PHARMACIES' : currentScopePharmacy());
  const drugId = q('labelPrintDrugFilter')?.value || '';
  const rows = (APP.cache.prescriptions || []).filter(row => {
    if (!canViewPharmacy(row.pharmacy)) return false;
    const day = formatJordanDateTime(row.dateTime).slice(0, 10);
    if (from && day < from) return false;
    if (to && day > to) return false;
    if (pharmacy && pharmacy !== 'ALL_WORK_PHARMACIES' && String(row.pharmacy) !== String(pharmacy)) return false;
    if (drugId && String(row.drugId) !== String(drugId)) return false;
    return true;
  });
  return { rows, from, to, pharmacy, drugId };
}

function buildPrescriptionLabelPrintShell(labelsHtml) {
  return `
  <html>
    <head>
      <title>Prescription Labels</title>
      <style>
        @page{size:95mm 60mm;margin:0}
        *{box-sizing:border-box}
        html,body{margin:0;padding:0;background:#ffffff;font-family:Inter,Arial,sans-serif;color:#111827}
        body{padding:0}
        .sheet{display:flex;flex-direction:column;gap:0}
        .rx-label{width:95mm;height:60mm;padding:3.2mm;background:#fff;position:relative;overflow:hidden;page-break-after:always}
        .rx-label:last-child{page-break-after:auto}
        .frame{height:100%;border:1.2px solid #1f2937;border-radius:4mm;padding:3.2mm;background:#ffffff;display:flex;flex-direction:column;gap:2mm}
        .drug{border:1.2px solid #1f2937;border-radius:3.2mm;padding:2.3mm 2.6mm;background:#ffffff;display:flex;flex-direction:column;justify-content:center;gap:1.1mm;min-height:18.5mm}
        .drug-name{font-size:4.25mm;font-weight:900;line-height:1.1;word-break:break-word;color:#111827}
        .drug-qty-row{display:flex;flex-direction:column;align-items:flex-start;justify-content:center;gap:.35mm}
        .drug-qty-label{font-size:2.15mm;font-weight:800;color:#374151;line-height:1}
        .drug-qty-value{font-size:3mm;font-weight:900;color:#111827;line-height:1.1;word-break:break-word}
        .meta-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1.8mm}
        .metric{border:1.2px solid #1f2937;border-radius:3mm;padding:1.4mm 1.8mm;background:#ffffff;min-height:14.5mm;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center}
        .metric-label{font-size:2.2mm;font-weight:800;color:#374151;line-height:1.15;text-align:center;width:100%}
        .metric-value{margin-top:.85mm;font-size:5.6mm;font-weight:900;color:#111827;line-height:1;text-align:center;width:100%;display:flex;align-items:center;justify-content:center;flex:1}
        .footer{margin-top:auto;border-top:1px dashed #374151;padding-top:1.7mm;display:grid;gap:1mm}
        .footer-row{display:flex;justify-content:space-between;gap:2mm;font-size:2.2mm;color:#111827;font-weight:800}
        .footer-row span{white-space:nowrap}
      </style>
    </head>
    <body>
      <div class="sheet">${labelsHtml}</div>
      <script>window.onload=function(){window.print()}</script>
    </body>
  </html>`;
}

function printPrescriptionLabels() {
  const { rows, from, to, pharmacy, drugId } = getPrescriptionLabelRows();
  if (!rows.length) {
    closeModal('prescriptionLabelModal');
    showActionModal('Print Label', 'No prescriptions were found for the selected filters.', false);
    q('actionOkBtn').classList.remove('hidden');
    return;
  }

  const groupedMap = rows.reduce((acc, row) => {
    const key = row.drugId || 'unknown';
    (acc[key] ||= []).push(row);
    return acc;
  }, {});
  const selectedDrugIds = drugId ? [drugId] : Object.keys(groupedMap).sort((a, b) => {
    const aDrug = getDrugById(a);
    const bDrug = getDrugById(b);
    return `${aDrug?.tradeName || ''} ${aDrug?.strength || ''}`.localeCompare(`${bDrug?.tradeName || ''} ${bDrug?.strength || ''}`);
  });

  const pharmacyLabel = pharmacy === 'ALL_WORK_PHARMACIES' ? 'All Pharmacies' : pharmacy;
  const labelsHtml = selectedDrugIds.map(currentDrugId => {
    const drug = getDrugById(currentDrugId);
    const drugRows = groupedMap[currentDrugId] || [];
    if (!drug || !drugRows.length) return '';
    const totalRx = drugRows.length;
    const returnedRx = drugRows.filter(row => String(row.status || '') === 'Returned').length;
    const totalUnits = drugRows.reduce((sum, row) => sum + (Number(row.qtyBoxes || 0) * Number(drug.unitsPerBox || 1)) + Number(row.qtyUnits || 0), 0);
    const qty = normalizeInventory(0, totalUnits, Number(drug.unitsPerBox || 1));
    const qtyText = `${qty.boxes} Box${qty.boxes === 1 ? '' : 'es'} + ${qty.units} ${unitLabel(drug)}`;
    return `
      <section class="rx-label">
        <div class="frame">
          <div class="drug">
            <div class="drug-name">${esc(`${drug.tradeName || ''} ${drug.strength || ''}`.trim())}</div>
            <div class="drug-qty-row">
              <div class="drug-qty-label">Dispensed Qty</div>
              <div class="drug-qty-value">${esc(qtyText)}</div>
            </div>
          </div>
          <div class="meta-grid">
            <div class="metric">
              <div class="metric-label">Registered Rx</div>
              <div class="metric-value">${totalRx}</div>
            </div>
            <div class="metric">
              <div class="metric-label">Returned Rx</div>
              <div class="metric-value">${returnedRx}</div>
            </div>
          </div>
          <div class="footer">
            <div class="footer-row"><span>From: ${esc(from || '-')}</span><span>To: ${esc(to || '-')}</span></div>
            <div class="footer-row"><span>Pharmacy: ${esc(pharmacyLabel)}</span><span>Printed: ${esc(formatJordanDateTime(jordanNowIso()))}</span></div>
          </div>
        </div>
      </section>`;
  }).join('');

  closeModal('prescriptionLabelModal');
  const w = window.open('', '_blank');
  w.document.write(buildPrescriptionLabelPrintShell(labelsHtml));
  w.document.close();
}

function buildPrintShell(title, subtitle, bodyHtml, options = {}) {
  const headerPill = options.headerPill || `Printed ${formatJordanDateTime(jordanNowIso())}`;
  const landscapeCss = options.landscape ? '@page{size:A4 landscape;margin:12mm}' : '';
  return `
  <html>
    <head>
      <title>${esc(title)}</title>
      <style>
        ${landscapeCss}
        body{font-family:Inter,Arial,sans-serif;background:#f3f6fb;color:#20344a;margin:0;padding:28px}
        .report{background:#fff;border:1px solid #d8e0eb;border-radius:20px;padding:28px;box-shadow:0 10px 24px rgba(0,0,0,.05)}
        .head{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:2px solid #173a66;padding-bottom:16px;margin-bottom:20px}
        .title{font-size:24px;font-weight:900;color:#173a66}
        .sub{margin-top:8px;color:#5f7085;font-size:13px}
        .section{margin-top:18px}
        .section-title{font-size:16px;font-weight:800;color:#173a66;margin:18px 0 10px}
        table{width:100%;border-collapse:collapse}
        th,td{padding:10px 12px;border:1px solid #d8e0eb;font-size:12px;text-align:left;vertical-align:top}
        th{background:#eef4fb;color:#173a66}
        .tx-print-section{margin-bottom:22px}
        .tx-meta-row td{background:#f8fbff;color:#36506e;font-size:11px}
        .tx-signature-row{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px;margin-top:18px}
        .tx-signature-box{border:1px solid #d8e0eb;border-radius:14px;padding:16px 14px;min-height:86px;background:#fbfdff}
        .tx-signature-box span{display:block;font-size:11px;color:#5f7085;margin-bottom:8px;font-weight:700}
        .tx-signature-box strong{display:block;font-size:13px;color:#173a66;margin-bottom:22px}
        .tx-sign-line{border-top:1.5px solid #7f93ab;margin-top:18px}
        .group{page-break-inside:avoid;margin-bottom:22px}
        .pill{display:inline-block;padding:6px 10px;border-radius:999px;background:#eef4fb;border:1px solid #d8e0eb;font-size:11px;font-weight:800}
        @media print{body{background:#fff;padding:0}.report{border:none;box-shadow:none;border-radius:0}.page-break{page-break-before:always}}
      </style>
    </head>
    <body>
      <div class="report">
        <div class="head">
          <div>
            <div class="title">${esc(title)}</div>
            <div class="sub">Jordan Hospital Pharmacy · Controlled Drugs Management</div>
            <div class="sub">${esc(subtitle)}</div>
          </div>
          <div class="pill">${esc(headerPill)}</div>
        </div>
        ${bodyHtml}
      </div>
      <script>window.onload=function(){window.print()}</script>
    </body>
  </html>`;
}

function printDrugReport() {
  const from = q("reportFromDate").value;
  const to = q("reportToDate").value;
  const drugId = q("reportDrug").value;
  const pharmacy = currentReportPharmacy();
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const rows = scopedPrescriptionRowsByPharmacy(pharmacy)
    .filter(row => (!from || formatJordanDateTime(row.dateTime).slice(0, 10) >= from) && (!to || formatJordanDateTime(row.dateTime).slice(0, 10) <= to) && (!drugId || row.drugId === drugId))
    .sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));

  const body = `
    <div class="section-title">Report Summary</div>
    <div class="sub"><strong>Pharmacy:</strong> ${esc(pharmacy === "ALL_WORK_PHARMACIES" ? "All Pharmacies" : pharmacy)} &nbsp; | &nbsp; <strong>Drug:</strong> ${esc(drug ? `${drug.tradeName} ${drug.strength}` : "All Drugs")}</div>
    <div class="section">
      <table>
        <thead><tr><th>Date & Time</th><th>Patient</th><th>File No.</th><th>Boxes</th><th>Units</th><th>Doctor</th><th>Pharmacist</th></tr></thead>
        <tbody>
          ${rows.map(row => `
            <tr>
              <td>${esc(formatJordanDateTime(row.dateTime))}</td>
              <td>${esc(row.patientName || "")}</td>
              <td>${esc(row.fileNumber || "")}</td>
              <td>${Number(row.qtyBoxes || 0)}</td>
              <td>${Number(row.qtyUnits || 0)}</td>
              <td>${esc(row.doctorName || "")}</td>
              <td>${esc(row.pharmacistName || "")}</td>
            </tr>
            <tr>
              <td colspan="7"><strong>Status:</strong> ${esc(row.status || "-")} &nbsp; | &nbsp; <strong>Prescription Type:</strong> ${esc(row.prescriptionType || "-")} &nbsp; | &nbsp; <strong>Audit Details:</strong> ${row.status === "Returned" ? "-" : esc((row.auditBy || "") + (row.auditDateTime ? ` • ${formatJordanDateTime(row.auditDateTime)}` : ""))}</td>
            </tr>
          `).join("") || `<tr><td colspan="7">No records found.</td></tr>`}
        </tbody>
      </table>
    </div>`;

  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Drug Report", `${pharmacy === "ALL_WORK_PHARMACIES" ? "All Pharmacies" : pharmacy}${from || to ? ` · ${from || ""} to ${to || ""}` : ""}`, body));
  w.document.close();
}

function printComprehensiveReport() {
  const from = q("reportFromDate").value;
  const to = q("reportToDate").value;
  const pharmacy = currentReportPharmacy();
  const pharmacyLabel = pharmacy === "ALL_WORK_PHARMACIES" ? "All Pharmacies" : pharmacy;
  const rows = scopedPrescriptionRowsByPharmacy(pharmacy)
    .filter(row => (!from || formatJordanDateTime(row.dateTime).slice(0, 10) >= from) && (!to || formatJordanDateTime(row.dateTime).slice(0, 10) <= to))
    .sort((a, b) => String(a.tradeName || a.drugName || "").localeCompare(String(b.tradeName || b.drugName || "")) || String(a.dateTime || "").localeCompare(String(b.dateTime || "")));

  const groups = sortDrugsAlphabetically(APP.cache.drugs).map(drug => ({
    drug,
    rows: rows.filter(row => row.drugId === drug.id)
  })).filter(group => group.rows.length);

  const body = groups.map((group, index) => {
    const totalBoxes = group.rows.reduce((sum, row) => sum + Number(row.qtyBoxes || 0), 0);
    const totalUnits = group.rows.reduce((sum, row) => sum + Number(row.qtyUnits || 0), 0);
    return `
      <div class="group ${index ? 'page-break' : ''}">
        <div class="section-title">Comprehensive Prescription Report</div>
        <div class="sub"><strong>Pharmacy:</strong> ${esc(pharmacyLabel)} &nbsp; | &nbsp; <strong>Drug:</strong> ${esc(`${group.drug.tradeName || ''} ${group.drug.strength || ''}`.trim())} &nbsp; | &nbsp; <strong>Date:</strong> ${esc(from || '-')} ${to ? `to ${esc(to)}` : ''}</div>
        <div class="sub">${esc(group.drug.scientificName || '')} ${group.drug.dosageForm ? `· ${esc(group.drug.dosageForm)}` : ''}</div>
        <div class="section">
          <table>
            <thead><tr><th>Date & Time</th><th>Patient</th><th>File No.</th><th>Doctor</th><th>Pharmacist</th><th>Boxes</th><th>Units</th><th>Status</th></tr></thead>
            <tbody>
              ${group.rows.map(row => `
                <tr>
                  <td>${esc(formatJordanDateTime(row.dateTime))}</td>
                  <td>${esc(row.patientName || '')}</td>
                  <td>${esc(row.fileNumber || '')}</td>
                  <td>${esc(row.doctorName || '')}</td>
                  <td>${esc(row.pharmacistName || '')}</td>
                  <td>${Number(row.qtyBoxes || 0)}</td>
                  <td>${Number(row.qtyUnits || 0)}</td>
                  <td>${esc(row.status || '')}</td>
                </tr>
                <tr>
                  <td colspan="8"><strong>Prescription Type:</strong> ${esc(row.prescriptionType || '-')} &nbsp; | &nbsp; <strong>Audit Details:</strong> ${row.status === 'Returned' ? '-' : esc((row.auditBy || '') + (row.auditDateTime ? ` • ${formatJordanDateTime(row.auditDateTime)}` : ''))}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
        <div class="section" style="margin-top:16px">
          <table>
            <thead><tr><th>Total Dispensed Boxes</th><th>Total Dispensed Units</th><th>Total Registered Prescriptions</th></tr></thead>
            <tbody><tr><td>${totalBoxes}</td><td>${totalUnits}</td><td>${group.rows.length}</td></tr></tbody>
          </table>
        </div>
      </div>`;
  }).join('') || `<div class="section-title">No prescriptions found for the selected pharmacy and date range.</div>`;

  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Comprehensive Report", `${pharmacyLabel}${from || to ? ` · ${from || ''} to ${to || ''}` : ''}`, body));
  w.document.close();
}

function closeAuditMenuPortal() {
  const portal = q("auditMenuPortal");
  if (!portal) return;
  portal.classList.add("hidden");
  portal.innerHTML = "";
  delete portal.dataset.sourceId;
}

function openAuditMenuPortal(btn, rowId) {
  const sourceMenu = q(`audit_menu_${rowId}`);
  const portal = q("auditMenuPortal");
  if (!sourceMenu || !portal) return;

  const alreadyOpen = !portal.classList.contains("hidden") && portal.dataset.sourceId === rowId;
  if (alreadyOpen) {
    closeAuditMenuPortal();
    return;
  }

  portal.innerHTML = sourceMenu.innerHTML;
  portal.dataset.sourceId = rowId;
  portal.classList.remove("hidden");

  const rect = btn.getBoundingClientRect();
  const portalWidth = Math.max(portal.offsetWidth || 190, 190);
  const left = Math.min(window.innerWidth - portalWidth - 12, Math.max(12, rect.right - portalWidth));
  let top = rect.bottom + 6;
  const portalHeight = portal.offsetHeight || 220;
  if (top + portalHeight > window.innerHeight - 12) {
    top = Math.max(12, rect.top - portalHeight - 6);
  }

  portal.style.left = `${left}px`;
  portal.style.top = `${top}px`;
}

window.addEventListener("scroll", () => closeAuditMenuPortal(), true);
window.addEventListener("resize", () => closeAuditMenuPortal());


function inpatientPharmacists() {
  return APP.cache.pharmacists.filter(p => p.active !== false && pharmacistWorksInScope(p, "In-Patient Pharmacy"));
}

function narcoticManagerPharmacists() {
  return inpatientPharmacists().filter(p => p.canManageNarcotic);
}

function renderNarcoticStaticOptions() {
  if (!q("narcoticDrug")) return;
  const selectedDepartment = q("narcoticDepartment")?.value || "";
  const selectedOrderDepartment = q("narcoticOrderDepartment")?.value || "";
  const selectedReportDepartment = q("narcoticReportDepartment")?.value || "";
  const selectedChartDepartment = q("narcoticChartDepartment")?.value || "";

  const deptOptions = APP.cache.narcoticDepartments.map(d => `<option value="${esc(d.id)}">${esc(d.departmentName || d.name || "")}</option>`).join("");
  ["narcoticDepartment","narcoticOrderDepartment","narcoticReportDepartment","narcoticChartDepartment"].forEach(id => {
    const prev = q(id)?.value || "";
    if (q(id)) {
      q(id).innerHTML = `<option value="">${id === "narcoticChartDepartment" ? "All Departments" : "Select Department"}</option>${deptOptions}`;
      if ([...q(id).options].some(opt => opt.value === prev)) q(id).value = prev;
    }
  });

  const selectedDrug = q("narcoticDrug")?.value || "";
  const selectedOrderDrug = q("narcoticOrderDrug")?.value || "";
  const selectedReportDrug = q("narcoticReportDrug")?.value || "";
  const selectedModalDrug = q("narcoticDeptModalDrug")?.value || "";
  const selectedChartDrug = q("narcoticChartDrug")?.value || "";
  const selectedAddDrug = q("narcoticDeptAddDrug")?.value || "";
  const term = (q("narcoticDrugSearch")?.value || "").toLowerCase().trim();

  const allActiveDrugs = APP.cache.narcoticDrugs.filter(d => d.active !== false);
  const assignedEntryDrugs = selectedDepartment ? narcoticAssignedDrugsByDepartment(selectedDepartment) : allActiveDrugs;
  const filteredEntryDrugs = assignedEntryDrugs.filter(d => !term || `${d.tradeName || ""} ${d.scientificName || ""} ${d.strength || ""}`.toLowerCase().includes(term));
  const assignedOrderDrugs = selectedOrderDepartment ? narcoticAssignedDrugsByDepartment(selectedOrderDepartment) : allActiveDrugs;
  const assignedModalDrugs = APP.narcoticOpenDepartmentId ? narcoticAssignedDrugsByDepartment(APP.narcoticOpenDepartmentId) : allActiveDrugs;
  const unassignedModalDrugs = APP.narcoticOpenDepartmentId
    ? allActiveDrugs.filter(d => !narcoticDeptStockRow(APP.narcoticOpenDepartmentId, d.id) || !isAssignedDepartmentDrug(narcoticDeptStockRow(APP.narcoticOpenDepartmentId, d.id)))
    : allActiveDrugs;

  const makeDrugOptions = rows => rows.map(d => `<option value="${esc(d.id)}">${esc(narcoticDrugLabel(d))}</option>`).join("");
  if (q("narcoticDrug")) {
    q("narcoticDrug").innerHTML = `<option value="">Select Drug</option>${makeDrugOptions(filteredEntryDrugs)}`;
    if ([...q("narcoticDrug").options].some(opt => opt.value === selectedDrug)) q("narcoticDrug").value = selectedDrug;
  }
  if (q("narcoticOrderDrug")) {
    q("narcoticOrderDrug").innerHTML = `<option value="">Select Drug</option>${makeDrugOptions(assignedOrderDrugs)}`;
    if ([...q("narcoticOrderDrug").options].some(opt => opt.value === selectedOrderDrug)) q("narcoticOrderDrug").value = selectedOrderDrug;
  }
  if (q("narcoticReportDrug")) {
    q("narcoticReportDrug").innerHTML = `<option value="">All narcotic drugs</option>${makeDrugOptions(allActiveDrugs)}`;
    if ([...q("narcoticReportDrug").options].some(opt => opt.value === selectedReportDrug)) q("narcoticReportDrug").value = selectedReportDrug;
  }
  if (q("narcoticTransactionsDrug")) {
    const selectedTxDrug = q("narcoticTransactionsDrug")?.value || "";
    q("narcoticTransactionsDrug").innerHTML = `<option value="">All narcotic drugs</option>${makeDrugOptions(allActiveDrugs)}`;
    if ([...q("narcoticTransactionsDrug").options].some(opt => opt.value === selectedTxDrug)) q("narcoticTransactionsDrug").value = selectedTxDrug;
  }
  if (q("narcoticChartDrug")) {
    q("narcoticChartDrug").innerHTML = `<option value="">All Drugs</option>${makeDrugOptions(allActiveDrugs)}`;
    if ([...q("narcoticChartDrug").options].some(opt => opt.value === selectedChartDrug)) q("narcoticChartDrug").value = selectedChartDrug;
  }
  if (q("narcoticDeptModalDrug")) {
    q("narcoticDeptModalDrug").innerHTML = `<option value="">Select Drug</option>${makeDrugOptions(assignedModalDrugs)}`;
    if ([...q("narcoticDeptModalDrug").options].some(opt => opt.value === selectedModalDrug)) q("narcoticDeptModalDrug").value = selectedModalDrug;
  }
  if (q("narcoticDeptAddDrug")) {
    q("narcoticDeptAddDrug").innerHTML = `<option value="">Select Drug</option>${makeDrugOptions(unassignedModalDrugs)}`;
    if ([...q("narcoticDeptAddDrug").options].some(opt => opt.value === selectedAddDrug)) q("narcoticDeptAddDrug").value = selectedAddDrug;
  }

  applyCurrentUserReadonlyFields();
  if (q("narcoticInternalReceiveDrug")) {
    const selectedInternalDrug = q("narcoticInternalReceiveDrug")?.value || "";
    q("narcoticInternalReceiveDrug").innerHTML = `<option value="">Select Drug</option>${makeDrugOptions(allActiveDrugs)}`;
    if ([...q("narcoticInternalReceiveDrug").options].some(opt => opt.value === selectedInternalDrug)) q("narcoticInternalReceiveDrug").value = selectedInternalDrug;
  }
  if (q("narcoticConsumptionDrug")) {
    const selectedConsumptionDrug = q("narcoticConsumptionDrug")?.value || "";
    q("narcoticConsumptionDrug").innerHTML = `<option value="">All narcotic drugs</option>${makeDrugOptions(allActiveDrugs)}`;
    if ([...q("narcoticConsumptionDrug").options].some(opt => opt.value === selectedConsumptionDrug)) q("narcoticConsumptionDrug").value = selectedConsumptionDrug;
  }
}

function narcoticDeptById(id) {
  return APP.cache.narcoticDepartments.find(d => d.id === id);
}
function narcoticDrugById(id) {
  return APP.cache.narcoticDrugs.find(d => d.id === id);
}
function narcoticInternalStockRow(drugId) {
  return APP.cache.narcoticInternalStock.find(r => String(r.drugId) === String(drugId));
}
function narcoticTransactionsFilters() {
  return {
    drugId: q("narcoticTransactionsDrug")?.value || "",
    type: q("narcoticTransactionsType")?.value || "",
    from: q("narcoticTransactionsFrom")?.value || "",
    to: q("narcoticTransactionsTo")?.value || "",
    search: (q("narcoticTransactionsSearch")?.value || "").toLowerCase().trim()
  };
}
function getFilteredNarcoticTransactionsRows() {
  const { drugId, type, from, to, search } = narcoticTransactionsFilters();
  const hasRange = !!(from || to);
  const defaultDay = jordanDateKey();
  return getMergedNarcoticMovementRows(getArchiveMode('narcoticMovements')).filter(row => {
    const day = String(formatJordanDateTime(row.dateTime)).slice(0, 10);
    if (hasRange) {
      if (from && day < from) return false;
      if (to && day > to) return false;
    } else if (day !== defaultDay) {
      return false;
    }
    if (drugId && String(row.drugId || "") !== String(drugId)) return false;
    if (type && String(row.type || "") !== type) return false;
    if (search) {
      const hay = `${row.type || ""} ${row.drugName || ""} ${row.departmentName || ""} ${row.performedBy || ""} ${row.nurseName || ""} ${row.notes || ""} ${row.invoiceNumber || ""}`.toLowerCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  }).sort((a,b)=>String(b.dateTime||"").localeCompare(String(a.dateTime||"")));
}
function narcoticDeptStockRow(departmentId, drugId) {
  return APP.cache.narcoticDepartmentStock.find(r => String(r.departmentId) === String(departmentId) && String(r.drugId) === String(drugId));
}
function isAssignedDepartmentDrug(stockRow) {
  return !!stockRow && stockRow.active !== false && stockRow.assigned === true;
}
function narcoticAssignedStockRows(departmentId) {
  return APP.cache.narcoticDepartmentStock.filter(r => String(r.departmentId) === String(departmentId) && isAssignedDepartmentDrug(r));
}
function narcoticAssignedDrugsByDepartment(departmentId) {
  return narcoticAssignedStockRows(departmentId).map(r => narcoticDrugById(r.drugId)).filter(Boolean);
}
function narcoticDrugLabel(drug) {
  return `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim();
}
function narcoticPrescriptionById(id) {
  return APP.cache.narcoticPrescriptions.find(r => r.id === id);
}

function narcoticConfirmPharmacists() {
  return inpatientPharmacists().map(p => p.userName || p.name).filter(Boolean);
}

function openNarcoticConfirmActionModal(type, id) {
  const rx = narcoticPrescriptionById(id);
  if (!rx || (type === "delete" ? !(APP.currentRole === "ADMIN" || canCurrentUserAudit() || APP.currentPortal === "IN_PATIENT_USER") : !(APP.currentRole === "ADMIN" || APP.currentPortal === "IN_PATIENT_USER"))) return;
  APP.confirmAction = { scope: "narcotic", type, id };
  q("confirmActionTitle").textContent = type === "delete" ? "Delete Narcotic Prescription" : "Return Narcotic Prescription";
  q("confirmActionText").textContent = type === "delete"
    ? (rx.status === "Returned"
        ? "Delete this returned narcotic prescription permanently. Stock will not be restored because it was already returned."
        : "Delete this narcotic prescription permanently and restore the stock to the same department?")
    : "Return this narcotic prescription and restore the stock to the same department?";
  applyCurrentUserReadonlyFields();
  if (q("confirmActionPharmacist")) q("confirmActionPharmacist").value = currentActorName();
  if (q("confirmActionPharmacist")?.parentElement) q("confirmActionPharmacist").parentElement.classList.remove("hidden");
  openModal("confirmActionModal");
}

function updateNarcoticAvailableStock() {
  if (!q("narcoticAvailableStock")) return;
  const row = narcoticDeptStockRow(q("narcoticDepartment")?.value, q("narcoticDrug")?.value);
  q("narcoticAvailableStock").value = row && isAssignedDepartmentDrug(row) ? `${Number(row.availableStockUnits || 0)} ${unitLabel(narcoticDrugById(row.drugId) || {})}` : "-";
}
function resetNarcoticEntryForm() {
  ["narcoticPatientName","narcoticFileNumber","narcoticNationalId","narcoticPrescriptionNumber","narcoticDoctorName","narcoticDose","narcoticDiscardDose","narcoticNotes"].forEach(id => { if (q(id)) q(id).value = ""; });
  if (q("narcoticDispensedUnits")) q("narcoticDispensedUnits").value = "1";
  if (q("narcoticDrug")) q("narcoticDrug").selectedIndex = 0;
  if (q("narcoticDepartment")) q("narcoticDepartment").selectedIndex = 0;
  applyCurrentUserReadonlyFields();
  updateNarcoticAvailableStock();
}
function narcoticActionMenu(row) {
  if (!row) return `<span class="muted">-</span>`;
  if (isArchivedRow(row)) return `<span class="badge pending">Archived</span>`;
  if (APP.currentRole === "ADMIN" || APP.currentRole === "IN_PATIENT_USER") {
    return buildPrescriptionActionsDropdown({ id: row.id, status: row.status || "Registered" }, { prefix: "narcotic" });
  }
  return `<button class="primary-btn mini-btn latest-edit-btn" data-id="${row.id}">Edit</button>`;
}
function setNarcoticDetailTab(tab) {
  APP.narcoticDetailTab = tab;
  document.querySelectorAll(".narcotic-inner-tab-btn").forEach(btn => {
    btn.classList.toggle("active", btn.dataset.narcoticdetailtab === tab);
  });
  document.querySelectorAll(".narcotic-detail-tab-content").forEach(el => {
    const isActive = el.id === `narcoticDetailTab-${tab}`;
    el.classList.toggle("hidden", !isActive);
    el.style.display = isActive ? "block" : "none";
  });
}

function renderNarcoticDispensingChart() {
  const wrap = q("narcoticDispensingChart");
  if (!wrap) return;
  const drugId = q("narcoticChartDrug")?.value || "";
  const departmentId = q("narcoticChartDepartment")?.value || "";
  const days = [];
  for (let i = 6; i >= 0; i -= 1) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    days.push(formatJordanDateTime(d).slice(0,10));
  }
  const data = days.map(day => {
    const total = getMergedNarcoticPrescriptionRows(getArchiveMode('narcoticPrescriptions')).filter(r => {
      const rowDay = String(formatJordanDateTime(r.dateTime)).slice(0,10);
      if (rowDay !== day) return false;
      if (drugId && r.drugId !== drugId) return false;
      if (departmentId && r.departmentId !== departmentId) return false;
      return true;
    }).reduce((s, r) => s + Number(r.dispensedUnits || 0), 0);
    return { day, total };
  });
  const max = Math.max(...data.map(x => x.total), 1);
  wrap.innerHTML = data.map(item => {
    const h = Math.max(8, Math.round((item.total / max) * 172));
    const label = item.day.slice(5);
    return `<div class="narcotic-smart-bar">
      <div class="narcotic-smart-bar-value">${item.total}</div>
      <div class="narcotic-smart-bar-track"><div class="narcotic-smart-bar-fill" style="height:${h}px"></div></div>
      <div class="narcotic-smart-bar-label">${esc(label)}</div>
    </div>`;
  }).join("") || `<div class="empty-state">No dispensing data found.</div>`;
}

function renderNarcoticDepartmentFullStockSummary() {
  if (!q("narcoticDeptStockSummaryAll")) return;
  q("narcoticDeptStockSummaryAll").innerHTML = "";
}

function renderNarcoticDepartmentManageTable() {
  if (!q("narcoticDeptAssignedTbody")) return;
  const departmentId = APP.narcoticOpenDepartmentId;
  const rows = narcoticAssignedStockRows(departmentId);
  q("narcoticDeptAssignedTbody").innerHTML = rows.map(row => {
    const drug = narcoticDrugById(row.drugId);
    return `<tr>
      <td>${esc(narcoticDrugLabel(drug) || row.drugName || "-")}</td>
      <td>${Number(row.fixedStockUnits || 0)}</td>
      <td><button class="mini-danger-btn mini-btn" data-remove-dept-drug="${row.drugId}">Delete</button></td>
    </tr>`;
  }).join("") || `<tr><td colspan="3" class="empty-state">No drugs assigned to this department.</td></tr>`;
}

async function addDrugToNarcoticDepartment() {
  if (APP.currentRole !== "ADMIN") return;
  const departmentId = APP.narcoticOpenDepartmentId;
  const drugId = q("narcoticDeptAddDrug")?.value || "";
  const fixedStockUnits = Math.max(0, Number(q("narcoticDeptAddFixedStock")?.value || 0));
  const availableStockUnits = Math.max(0, Number(q("narcoticDeptAddAvailableStock")?.value || 0));
  if (!departmentId || !drugId) {
    showActionModal("Department Drugs", "Please select the drug you want to add.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const dept = narcoticDeptById(departmentId);
  const drug = narcoticDrugById(drugId);
  const existing = narcoticDeptStockRow(departmentId, drugId);
  showActionModal("Department Drugs", "Please wait while the department drug is being added...");
  if (existing?.id) {
    await updateDoc(doc(db, "narcotic_department_stock", existing.id), {
      assigned: true,
      active: true,
      fixedStockUnits,
      availableStockUnits,
      drugName: narcoticDrugLabel(drug),
      departmentName: dept?.departmentName || "",
      updatedAt: serverTimestamp()
    });
  } else {
    await setDoc(doc(db, "narcotic_department_stock", `${departmentId}__${drugId}`), {
      id: `${departmentId}__${drugId}`,
      departmentId,
      departmentName: dept?.departmentName || "",
      drugId,
      drugName: narcoticDrugLabel(drug),
      fixedStockUnits,
      availableStockUnits,
      assigned: true,
      active: true,
      updatedAt: serverTimestamp()
    });
  }
  if (q("narcoticDeptAddDrug")) q("narcoticDeptAddDrug").selectedIndex = 0;
  if (q("narcoticDeptAddFixedStock")) q("narcoticDeptAddFixedStock").value = "0";
  if (q("narcoticDeptAddAvailableStock")) q("narcoticDeptAddAvailableStock").value = "0";
  renderNarcoticStaticOptions();
  renderNarcoticDepartmentManageTable();
  finishActionModal(true, "Drug added to department successfully.");
}

window.addDrugToNarcoticDepartment = addDrugToNarcoticDepartment;
window.setNarcoticDetailTab = setNarcoticDetailTab;

async function removeDrugFromNarcoticDepartment(drugId) {
  if (APP.currentRole !== "ADMIN") return;
  const departmentId = APP.narcoticOpenDepartmentId;
  const stockRow = narcoticDeptStockRow(departmentId, drugId);
  if (!stockRow?.id) return;
  showActionModal("Department Drugs", "Please wait while the drug is being removed from the department...");
  await updateDoc(doc(db, "narcotic_department_stock", stockRow.id), { assigned: false, updatedAt: serverTimestamp() });
  renderNarcoticStaticOptions();
  renderNarcoticDepartmentManageTable();
  finishActionModal(true, "Drug removed from department successfully.");
}

function renderNarcoticRecent() {
  if (!q("narcoticRecentTbody")) return;
  ensureArchiveCacheForSection('narcoticPrescriptions');
  const shell = q("narcoticRecentTbody")?.closest('.table-shell') || q("narcoticRecentTbody")?.parentElement;
  if (shell) mountArchiveToggle(shell, 'narcoticPrescriptions', { prepend: true, panelId: 'narcoticRecentArchiveTogglePanel' });
  const rows = getMergedNarcoticPrescriptionRows(getArchiveMode('narcoticPrescriptions')).slice(0,7);
  q("narcoticRecentTbody").innerHTML = rows.map(row => {
    const drug = narcoticDrugById(row.drugId);
    const dept = narcoticDeptById(row.departmentId);
    return `<tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc((drug?.tradeName || row.drugName || "") + " " + (drug?.strength || ""))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(dept?.departmentName || row.departmentName || "")}</td>
      <td>${Number(row.dispensedUnits || 0)}</td>
      <td>${esc(row.discardDose || "")}</td>
      <td>${esc(row.pharmacist || "")}</td>
      <td>${isArchivedRow(row) ? '<span class="badge pending">Archived</span>' : narcoticActionMenu(row)}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="8" class="empty-state">No narcotic prescriptions found.</td></tr>`;
}
function renderNarcoticDrugCards() {
  if (!q("narcoticDrugCards")) return;
  q("narcoticDrugCards").innerHTML = APP.cache.narcoticDrugs.map(drug => `
      <div class="drug-card narcotic-drug-card" data-narcotic-drug-card="${esc(drug.id)}">
        <div class="drug-title">${esc(drug.tradeName || "")} ${esc(drug.strength || "")}</div>
        <div class="drug-meta">${esc(drug.scientificName || "")}</div>
        <div class="drug-meta">${esc(drug.dosageForm || "")}</div>
      </div>`).join("") || `<div class="empty-state">No narcotic drugs configured.</div>`;
}
function renderNarcoticDepartmentCards() {
  if (!q("narcoticDepartmentCards")) return;
  const term = (q("narcoticDepartmentSearch")?.value || "").toLowerCase().trim();
  q("narcoticDepartmentCards").innerHTML = APP.cache.narcoticDepartments
    .filter(dept => !term || String(dept.departmentName || "").toLowerCase().includes(term))
    .map(dept => `
      <div class="drug-card narcotic-department-card" draggable="${APP.currentRole === "ADMIN"}" data-narcotic-dept-card="${esc(dept.id)}">
        <div class="narcotic-department-card-title">${esc(dept.departmentName || "")}</div>
      </div>
    `).join("") || `<div class="empty-state">No departments found.</div>`;
}
function renderNarcoticDepartmentsTable() {
  if (!q("narcoticDepartmentsTbody")) return;
  q("narcoticDepartmentsTbody").innerHTML = APP.cache.narcoticDepartments.map(dept => `
    <tr>
      <td>${esc(dept.departmentName || "")}</td>
      <td>${esc(dept.notes || "")}</td>
      <td>${Number(dept.sortOrder || 0)}</td>
      <td>${dept.active !== false ? "Yes" : "No"}</td>
      <td><button class="mini-danger-btn mini-btn delete-narcotic-department-btn" data-id="${dept.id}">Delete</button></td>
    </tr>
  `).join("") || `<tr><td colspan="5" class="empty-state">No departments found.</td></tr>`;
}

function renderNarcoticManageDrugsTable() {
  if (!q("narcoticManageDrugsTbody")) return;
  q("narcoticManageDrugsTbody").innerHTML = APP.cache.narcoticDrugs.map(drug => `
    <tr>
      <td>${esc(drug.scientificName || "")}</td>
      <td>${esc(drug.tradeName || "")}</td>
      <td>${esc(drug.strength || "")}</td>
      <td>${esc(drug.dosageForm || "")}</td>
      <td>${Number(drug.unitsPerBox || 0)}</td>
      <td>
        <div class="button-inline-group">
          <button class="soft-btn mini-btn" data-narcotic-drug-card="${esc(drug.id)}">Open</button>
          <button class="mini-danger-btn mini-btn delete-narcotic-drug-btn" data-id="${drug.id}">Delete</button>
        </div>
      </td>
    </tr>
  `).join("") || `<tr><td colspan="6" class="empty-state">No narcotic drugs configured.</td></tr>`;
}

function ensureTransactionDetailsModal() {
  if (q("transactionDetailsModal")) return;
  const node = document.createElement("div");
  node.innerHTML = `
    <div id="transactionDetailsModal" class="modal wide hidden">
      <div class="modal-header">Transaction Details</div>
      <div class="modal-body">
        <div id="transactionDetailsBody" class="transaction-details-body"></div>
      </div>
      <div class="modal-actions">
        <button id="printSelectedTransactionBtn" class="primary-btn">Print Selected Transaction</button>
        <button class="soft-btn" data-close="transactionDetailsModal">Close</button>
      </div>
    </div>`;
  document.body.appendChild(node.firstElementChild);
  q("transactionDetailsModal").querySelectorAll("[data-close]").forEach(btn => btn.onclick = () => closeModal(btn.dataset.close));
  if (q("printSelectedTransactionBtn")) q("printSelectedTransactionBtn").onclick = printSelectedTransaction;
}
function detailLine(label, value) {
  return `<div class="tx-detail-row"><strong>${esc(label)}:</strong> <span>${value}</span></div>`;
}
function buildShipmentBatchDetails(rows) {
  const first = rows[0] || {};
  return `
    <div class="tx-detail-card">
      <div class="tx-detail-title">Shipment Receipt Details</div>
      ${detailLine("Pharmacy", esc(first.pharmacy || "-"))}
      ${detailLine("Invoice Date", esc(first.invoiceDate || "-"))}
      ${detailLine("Invoice Number", esc(first.invoiceNumber || "-"))}
      ${detailLine("Received By", esc(first.receivedBy || first.performedBy || "-"))}
      ${detailLine("Received At", esc(formatJordanDateTime(first.receivedDateTime || first.dateTime || "")))}
      <div class="tx-detail-subtitle">Received Items</div>
      <table class="tx-detail-table">
        <thead><tr><th>Drug</th><th>Boxes</th><th>Units</th></tr></thead>
        <tbody>${rows.map(row => `<tr><td>${esc(drugDisplayLabel(row.drugId, row.tradeName || "-"))}</td><td>${Number(row.qtyBoxes || 0)}</td><td>${Number(row.qtyUnits || 0)}</td></tr>`).join("")}</tbody>
      </table>
    </div>`;
}
function buildTransferBatchDetails(rows) {
  const first = rows[0] || {};
  return `
    <div class="tx-detail-card">
      <div class="tx-detail-title">Stock Transfer Details</div>
      ${detailLine("From Pharmacy", esc(first.fromPharmacy || "-"))}
      ${detailLine("To Pharmacy", esc(first.toPharmacy || "-"))}
      ${detailLine("Transferred By", esc(first.performedBy || "-"))}
      ${detailLine("Received By", esc(first.receiverPharmacist || "-"))}
      ${detailLine("Date & Time", esc(formatJordanDateTime(first.dateTime || "")))}
      <div class="tx-detail-subtitle">Transferred Items</div>
      <table class="tx-detail-table">
        <thead><tr><th>Drug</th><th>Boxes</th><th>Units</th></tr></thead>
        <tbody>${rows.map(row => `<tr><td>${esc(drugDisplayLabel(row.drugId, row.tradeName || "-"))}</td><td>${Number(row.qtyBoxes || 0)}</td><td>${Number(row.qtyUnits || 0)}</td></tr>`).join("")}</tbody>
      </table>
    </div>`;
}
function buildNormalTransactionDetails(row) {
  if (!row) return `<div class="empty-state">Transaction not found.</div>`;
  if (row.type === "Receive Shipment") {
    const rows = (APP.cache.transactions || []).filter(r => (row.batchId && r.batchId === row.batchId) || r.id === row.id);
    return buildNormalTransactionPrintSection("shipment", rows);
  }
  if (row.type === "Transfer") {
    const rows = (APP.cache.transactions || []).filter(r => (row.batchId && r.batchId === row.batchId) || r.id === row.id);
    return buildNormalTransactionPrintSection("transfer", rows);
  }
  if (row.type === "Return") return buildNormalTransactionPrintSection("return", [row]);
  if (row.type === "Delete Prescription") return buildNormalTransactionPrintSection("delete", [row]);
  if (row.type === "Edit Prescription") return buildNormalTransactionPrintSection("edit", [row]);
  if (["Register", "Dispense"].includes(String(row.type || ""))) return buildNormalTransactionPrintSection("register", [row]);
  return `<div class="tx-detail-card">
    <div class="tx-detail-title">${esc(row.type || "Transaction")}</div>
    ${detailLine("Drug", esc(drugDisplayLabel(row.drugId, row.tradeName || "-")))}
    ${detailLine("Pharmacy", esc(row.pharmacy || "-"))}
    ${detailLine("By", esc(row.performedBy || "-"))}
    ${detailLine("Date & Time", esc(formatJordanDateTime(row.dateTime || "")))}
    ${detailLine("Note", esc(row.note || "-"))}
  </div>`;
}
function buildNarcoticTransactionDetails(row) {
  if (!row) return `<div class="empty-state">Transaction not found.</div>`;
  if (row.type === "Internal Stock Receipt") return buildNarcoticTransactionPrintSection("receipt", [row]);
  if (row.type === "Department Order Movement") return buildNarcoticTransactionPrintSection("transfer", [row]);
  if (row.type === "Register") return buildNarcoticTransactionPrintSection("register", [row]);
  if (row.type === "Return") return buildNarcoticTransactionPrintSection("return", [row]);
  if (row.type === "Delete Prescription") return buildNarcoticTransactionPrintSection("delete", [row]);
  return `<div class="tx-detail-card">
    <div class="tx-detail-title">${esc(row.type || "Narcotic Transaction")}</div>
    ${detailLine("Drug", esc(row.drugName || "-"))}
    ${detailLine("Department", esc(row.departmentName || "-"))}
    ${detailLine("Pharmacist", esc(row.performedBy || "-"))}
    ${detailLine("Date & Time", esc(formatJordanDateTime(row.dateTime || "")))}
    ${detailLine("Notes", esc(row.notes || "-"))}
  </div>`;
}
function printSelectedTransaction() {
  const info = APP.currentTransactionDetail;
  if (!info) return;
  let body = `<div class="empty-state">Transaction not found.</div>`;
  let title = 'Selected Transaction';
  if (info.scope === 'narcotic') {
    const row = getMergedNarcoticMovementRows(getArchiveMode('narcoticMovements')).find(item => String(item.id) === String(info.id));
    body = buildNarcoticTransactionDetails(row);
    title = 'Narcotic Transaction';
  } else {
    const row = getMergedTransactionRows(getArchiveMode('transactions')).find(item => String(item.id) === String(info.id));
    body = buildNormalTransactionDetails(row);
    title = 'Transaction';
  }
  const w = window.open('', '_blank');
  w.document.write(buildPrintShell(title, currentScopePharmacy(), body));
  w.document.close();
}

function openTransactionDetails(scope, id) {
  ensureTransactionDetailsModal();
  let body = `<div class="empty-state">Transaction not found.</div>`;
  APP.currentTransactionDetail = { scope, id };
  if (scope === "narcotic") {
    const row = getMergedNarcoticMovementRows(getArchiveMode('narcoticMovements')).find(item => String(item.id) === String(id));
    body = buildNarcoticTransactionDetails(row);
  } else {
    const row = getMergedTransactionRows(getArchiveMode('transactions')).find(item => String(item.id) === String(id));
    body = buildNormalTransactionDetails(row);
  }
  q("transactionDetailsBody").innerHTML = body;
  openModal("transactionDetailsModal");
}
async function receiveNarcoticInternalShipment() {
  if (APP.currentRole !== "ADMIN") return;
  const drugId = q("narcoticInternalReceiveDrug")?.value || "";
  const quantityReceived = Number(q("narcoticInternalReceiveQty")?.value || 0);
  const invoiceDate = q("narcoticInternalReceiveInvoiceDate")?.value || "";
  const invoiceNumber = q("narcoticInternalReceiveInvoiceNumber")?.value.trim() || "";
  const pharmacist = q("narcoticInternalReceivePharmacist")?.value || "";
  const notes = q("narcoticInternalReceiveNotes")?.value.trim() || "";
  if (!drugId || !pharmacist || quantityReceived <= 0) {
    showActionModal("Receive Narcotic Shipment", "Please complete drug, quantity, and pharmacist fields.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const drug = narcoticDrugById(drugId);
  const stockRow = narcoticInternalStockRow(drugId);
  showActionModal("Receive Narcotic Shipment", "Please wait while the shipment is being received...");
  const batch = writeBatch(db);
  if (stockRow?.id) {
    batch.update(doc(db, "narcotic_internal_stock", stockRow.id), {
      availableStockUnits: Number(stockRow.availableStockUnits || 0) + quantityReceived,
      updatedAt: serverTimestamp()
    });
  } else {
    batch.set(doc(db, "narcotic_internal_stock", drugId), {
      id: drugId,
      drugId,
      drugName: drug?.tradeName || "",
      availableStockUnits: quantityReceived,
      reorderLevelUnits: Number(drug?.reorderLevelUnits || 0),
      pharmacy: "In-Patient Pharmacy",
      updatedAt: serverTimestamp()
    });
  }
  batch.set(doc(collection(db, "narcotic_order_movements")), {
    dateTime: jordanNowIso(),
    type: "Internal Stock Receipt",
    drugId,
    drugName: drug?.tradeName || "",
    quantityReceived,
    invoiceDate,
    invoiceNumber,
    performedBy: pharmacist,
    notes
  });
  await batch.commit();
  q("narcoticInternalReceiveDrug").value = "";
  q("narcoticInternalReceiveQty").value = "0";
  q("narcoticInternalReceiveInvoiceDate").value = "";
  q("narcoticInternalReceiveInvoiceNumber").value = "";
  q("narcoticInternalReceivePharmacist").value = "";
  q("narcoticInternalReceiveNotes").value = "";
  await refreshTablesImmediate(["narcotic_internal_stock", "narcotic_order_movements"]);
  finishActionModal(true, "Narcotic shipment received successfully.");
}
function openNarcoticInternalStockAdjustModal(drugId) {
  const drug = narcoticDrugById(drugId);
  const stock = narcoticInternalStockRow(drugId);
  if (!drug) return;
  APP.narcoticInternalAdjustDrugId = drugId;
  if (q("narcoticInternalAdjustDrugDisplay")) q("narcoticInternalAdjustDrugDisplay").value = narcoticDrugLabel(drug);
  if (q("narcoticInternalAdjustAvailable")) q("narcoticInternalAdjustAvailable").value = Number(stock?.availableStockUnits || 0);
  if (q("narcoticInternalAdjustReorder")) q("narcoticInternalAdjustReorder").value = Number(stock?.reorderLevelUnits ?? drug.reorderLevelUnits ?? 0);
  openModal("narcoticInternalStockAdjustModal");
}

async function saveNarcoticInternalStockAdjust() {
  if (APP.currentRole !== "ADMIN" || !APP.narcoticInternalAdjustDrugId) return;
  const drugId = APP.narcoticInternalAdjustDrugId;
  const drug = narcoticDrugById(drugId);
  const stockRow = narcoticInternalStockRow(drugId);
  const availableStockUnits = Math.max(0, Number(q("narcoticInternalAdjustAvailable")?.value || 0));
  const reorderLevelUnits = Math.max(0, Number(q("narcoticInternalAdjustReorder")?.value || 0));
  showActionModal("Adjust In-Patient Narcotic Stock", "Please wait while the inpatient narcotic stock is being updated...");
  if (stockRow?.id) {
    await updateDoc(doc(db, "narcotic_internal_stock", stockRow.id), {
      availableStockUnits,
      reorderLevelUnits,
      updatedAt: serverTimestamp()
    });
  } else {
    await setDoc(doc(db, "narcotic_internal_stock", drugId), {
      id: drugId,
      drugId,
      drugName: drug?.tradeName || "",
      pharmacy: "In-Patient Pharmacy",
      availableStockUnits,
      reorderLevelUnits,
      updatedAt: serverTimestamp()
    });
  }
  await refreshTablesImmediate(["narcotic_internal_stock"]);
  closeModal("narcoticInternalStockAdjustModal");
  finishActionModal(true, "In-patient narcotic stock updated successfully.");
}

function printNarcoticTransactionsPage() {
  if (APP.currentRole !== "ADMIN") {
    showActionModal("Print Narcotic Transactions", "Only Admin can print narcotic transactions.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const rows = getFilteredNarcoticTransactionsRows();
  const registerRows = rows.filter(row => row.type === "Register");
  const deleteRows = rows.filter(row => row.type === "Delete Prescription");
  const returnRows = rows.filter(row => row.type === "Return");
  const internalReceiptRows = rows.filter(row => row.type === "Internal Stock Receipt");
  const departmentOrderRows = rows.filter(row => row.type === "Department Order Movement");

  const body = `
    <div class="section-title">Narcotic Transactions Report</div>
    <div class="sub"><strong>Total Rows:</strong> ${rows.length}</div>
    ${buildNarcoticTransactionPrintSection("register", registerRows)}
    ${buildNarcoticTransactionPrintSection("delete", deleteRows)}
    ${buildNarcoticTransactionPrintSection("return", returnRows)}
    ${buildNarcoticTransactionPrintSection("receipt", internalReceiptRows)}
    ${buildNarcoticTransactionPrintSection("transfer", departmentOrderRows)}
  `;
  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Narcotic Transactions Report", "In-Patient Pharmacy Narcotic Movements", body));
  w.document.close();
}
function renderNarcoticInternalStockTable() {
  if (!q("narcoticInternalStockTbody")) return;
  const term = (q("narcoticInternalStockSearch")?.value || "").toLowerCase().trim();
  const rows = APP.cache.narcoticDrugs.map(drug => {
    const stock = narcoticInternalStockRow(drug.id);
    const available = Number(stock?.availableStockUnits || 0);
    const reorder = Number(stock?.reorderLevelUnits ?? drug.reorderLevelUnits ?? 0);
    return { drug, stock, available, reorder };
  }).filter(({drug}) => `${drug.tradeName || ""} ${drug.scientificName || ""} ${drug.strength || ""}`.toLowerCase().includes(term));

  q("narcoticInternalStockTbody").innerHTML = rows.map(({ drug, available, reorder }) => `
    <tr>
      <td>${esc(`${drug.tradeName || ""} ${drug.strength || ""}`.trim())}</td>
      <td>${available}</td>
      <td>${reorder}</td>
      <td>${available <= reorder ? '<span class="badge pending">Below Reorder</span>' : '<span class="badge verified">OK</span>'}</td>
      <td>${APP.currentRole === "ADMIN" ? `<button class="soft-btn mini-btn" data-adjust-narcotic-internal-stock="${esc(drug.id)}">Manual Edit</button>` : '-'}</td>
    </tr>
  `).join("") || `<tr><td colspan="5" class="empty-state">No inpatient narcotic stock rows found.</td></tr>`;
}
function renderNarcoticTransactionsTable() {
  if (!q("narcoticTransactionsTbody")) return;
  ensureArchiveCacheForSection('narcoticMovements');
  const shell = q("narcoticTransactionsTbody")?.closest('.table-shell') || q("narcoticTransactionsTbody")?.parentElement;
  if (shell) mountArchiveToggle(shell, 'narcoticMovements', { prepend: true, panelId: 'narcoticTransactionsArchiveTogglePanel' });
  const rows = getFilteredNarcoticTransactionsRows();
  q("narcoticTransactionsTbody").innerHTML = rows.map(row => `
    <tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc(row.type || "")}</td>
      <td>${esc(row.drugName || "")}</td>
      <td>${esc(row.departmentName || "-")}</td>
      <td>${Number(row.quantitySent || row.quantityReceived || row.dispensedUnits || 0)}</td>
      <td>${esc(row.invoiceDate || "-")}</td>
      <td>${esc(row.invoiceNumber || "-")}</td>
      <td>${esc(row.performedBy || "-")}</td>
      <td>${esc(row.nurseName || "-")}</td>
      <td>${esc(row.notes || "-")}</td>
      <td><button class="soft-btn mini-btn transaction-details-btn" data-scope="narcotic" data-id="${esc(row.id)}">Details${isArchivedRow(row) ? ' · Archived' : ''}</button></td>
    </tr>
  `).join("") || `<tr><td colspan="11" class="empty-state">No narcotic transactions found for the selected criteria.</td></tr>`;
}
function renderNarcoticOrdersTable() {
  if (!q("narcoticOrdersTbody")) return;
  const rows = APP.narcoticOrdersBatchRows || [];
  q("narcoticOrdersTbody").innerHTML = rows.map((row,index) => `
    <tr>
      <td>${esc(row.departmentName || "")}</td>
      <td>${esc(row.drugLabel || "")}</td>
      <td>${Number(row.emptyAmpoulesReceived || 0)}</td>
      <td>${Number(row.quantitySent || 0)}</td>
      <td>${esc(row.performedBy || "")}</td>
      <td>${esc(row.nurseName || "")}</td>
      <td><button class="soft-btn mini-btn" data-remove-narcotic-order-row="${index}">Remove</button></td>
    </tr>
  `).join("") || `<tr><td colspan="7" class="empty-state">No order rows added yet.</td></tr>`;
}
function renderNarcoticPage() {
  if (!APP.currentUser || !q("page-narcotic")) return;
  if (APP.currentRole !== "ADMIN" && APP.narcoticTab !== "dispensing") APP.narcoticTab = "dispensing";
  q("narcoticLiveTime").textContent = formatJordanDateTime(jordanNowIso());
  renderNarcoticStaticOptions();
  renderNarcoticRecent();
  renderNarcoticDrugCards();
  renderNarcoticDepartmentCards();
  renderNarcoticOrdersTable();
  renderNarcoticDepartmentsTable();
  renderNarcoticManageDrugsTable();
  renderNarcoticInternalStockTable();
  renderNarcoticTransactionsTable();
  updateNarcoticAvailableStock();
  updateNarcoticReportPreview();
  if (APP.narcoticOpenDepartmentId) {
    renderNarcoticDepartmentFullStockSummary();
    renderNarcoticDepartmentManageTable();
  }
  setNarcoticTab(APP.narcoticTab || "dispensing");
}
async function registerNarcoticPrescription() {
  const payload = {
    drugId: q("narcoticDrug").value,
    departmentId: q("narcoticDepartment").value,
    patientName: toSmartTitleCase(q("narcoticPatientName").value.trim()),
    fileNumber: q("narcoticFileNumber").value.trim(),
    nationalId: q("narcoticNationalId").value.trim(),
    prescriptionNumber: q("narcoticPrescriptionNumber").value.trim(),
    doctorName: toSmartTitleCase(q("narcoticDoctorName").value.trim()),
    dose: q("narcoticDose").value.trim(),
    dispensedUnits: Number(q("narcoticDispensedUnits").value || 0),
    discardDose: q("narcoticDiscardDose").value.trim(),
    pharmacist: q("narcoticPharmacist").value.trim(),
    notes: q("narcoticNotes").value.trim()
  };
  if (!payload.drugId || !payload.departmentId || !payload.patientName || !payload.fileNumber || !payload.prescriptionNumber || !payload.doctorName || !payload.dose || !payload.pharmacist || payload.dispensedUnits <= 0) {
    showActionModal("Validation", "Please complete all required narcotic prescription fields.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const stockRow = narcoticDeptStockRow(payload.departmentId, payload.drugId);
  if (!stockRow || !isAssignedDepartmentDrug(stockRow) || Number(stockRow.availableStockUnits || 0) < Number(payload.dispensedUnits || 0)) {
    showActionModal("Stock Validation", "No available stock in the selected department for this narcotic drug.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const drug = narcoticDrugById(payload.drugId);
  const dept = narcoticDeptById(payload.departmentId);
  showActionModal("Register Narcotic Prescription", "Please wait while the narcotic prescription is being saved...");
  const batch = writeBatch(db);
  batch.set(doc(collection(db, "narcotic_prescriptions")), {
    ...payload,
    drugName: drug?.tradeName || "",
    departmentName: dept?.departmentName || "",
    drugName: drug?.tradeName || "",
    departmentName: dept?.departmentName || "",
    status: "Registered",
    dateTime: jordanNowIso(),
    createdBy: actorDisplayName(),
    updatedAt: jordanNowIso()
  });
  batch.update(doc(db, "narcotic_department_stock", stockRow.id), {
    availableStockUnits: Math.max(0, Number(stockRow.availableStockUnits || 0) - Number(payload.dispensedUnits || 0)),
    updatedAt: serverTimestamp()
  });
  batch.set(doc(collection(db, "narcotic_order_movements")), {
    dateTime: jordanNowIso(),
    type: "Register",
    departmentId: payload.departmentId,
    departmentName: dept?.departmentName || "",
    drugId: payload.drugId,
    drugName: drug?.tradeName || "",
    patientName: payload.patientName || "",
    fileNumber: payload.fileNumber || "",
    prescriptionNumber: payload.prescriptionNumber || "",
    dispensedUnits: Number(payload.dispensedUnits || 0),
    emptyAmpoulesReceived: 0,
    quantitySent: 0,
    performedBy: payload.pharmacist,
    notes: `Prescription ${payload.prescriptionNumber} registered`
  });
  await batch.commit();
  await refreshTablesImmediate(["narcotic_prescriptions", "narcotic_department_stock", "narcotic_order_movements"]);
  resetNarcoticEntryForm();
  finishActionModal(true, "Narcotic prescription registered successfully.");
}
function addNarcoticOrderRow() {
  const departmentId = q("narcoticOrderDepartment").value;
  const drugId = q("narcoticOrderDrug").value;
  const emptyAmpoulesReceived = Number(q("narcoticOrderEmpty").value || 0);
  const quantitySent = Number(q("narcoticOrderSent").value || 0);
  const performedBy = q("narcoticOrderPharmacist").value;
  const nurseName = toSmartTitleCase(q("narcoticOrderNurseName").value.trim());
  if (q("narcoticOrderNurseName")) q("narcoticOrderNurseName").value = nurseName;
  if (!departmentId || !drugId || !performedBy) return false;
  const stockRow = narcoticDeptStockRow(departmentId, drugId);
  if (!stockRow || !isAssignedDepartmentDrug(stockRow)) {
    showActionModal("Department Orders", "The selected drug is not assigned to the selected department. Order cannot be received.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  const dept = narcoticDeptById(departmentId);
  const drug = narcoticDrugById(drugId);
  APP.narcoticOrdersBatchRows = APP.narcoticOrdersBatchRows || [];
  APP.narcoticOrdersBatchRows.push({
    departmentId, departmentName: dept?.departmentName || "", drugId,
    drugLabel: `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim(),
    drugName: `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim(),
    emptyAmpoulesReceived, quantitySent, performedBy, nurseName, notes: q("narcoticOrderNotes").value.trim()
  });
  q("narcoticOrderEmpty").value = "0"; q("narcoticOrderSent").value = "0"; q("narcoticOrderNotes").value = ""; if (q("narcoticOrderNurseName")) q("narcoticOrderNurseName").value = "";
  renderNarcoticOrdersTable();
  return true;
}
async function commitNarcoticOrdersBatch(rows) {
  const batch = writeBatch(db);
  for (const row of rows) {
    const stockRow = narcoticDeptStockRow(row.departmentId, row.drugId);
    const internalStock = narcoticInternalStockRow(row.drugId);
    if (!stockRow || !isAssignedDepartmentDrug(stockRow)) throw new Error("The selected drug is not assigned to the selected department.");
    const nextAvailable = Number(stockRow?.availableStockUnits || 0) + Number(row.quantitySent || 0);
    if (stockRow?.id) {
      batch.update(doc(db, "narcotic_department_stock", stockRow.id), { availableStockUnits: nextAvailable, updatedAt: serverTimestamp() });
    }
    if (internalStock?.id) {
      batch.update(doc(db, "narcotic_internal_stock", internalStock.id), {
        availableStockUnits: Math.max(0, Number(internalStock.availableStockUnits || 0) - Number(row.quantitySent || 0)),
        updatedAt: serverTimestamp()
      });
    }
    batch.set(doc(collection(db, "narcotic_order_movements")), {
      ...row,
      type: "Department Order Movement",
      quantityReceived: 0,
      dateTime: jordanNowIso()
    });
  }
  await batch.commit();
  APP.narcoticOrdersBatchRows = [];
  renderNarcoticOrdersTable();
  await refreshTablesImmediate(["narcotic_department_stock", "narcotic_internal_stock", "narcotic_order_movements"]);
  finishActionModal(true, "Department orders saved successfully.");
}
function openNarcoticOverflowModal(row, stockRow) {
  APP.narcoticPendingOverflowRows = APP.narcoticOrdersBatchRows.slice();
  const nextAvailable = Number(stockRow?.availableStockUnits || 0) + Number(row.quantitySent || 0);
  q("narcoticStockOverflowText").innerHTML = `
    <strong class="narcotic-order-warning-strong">The available stock will become higher than the fixed stock.</strong>
    <div><strong>Department:</strong> ${esc(row.departmentName || "")}</div>
    <div><strong>Drug:</strong> ${esc(row.drugName || "")}</div>
    <div><strong>Fixed Stock:</strong> ${Number(stockRow?.fixedStockUnits || 0)}</div>
    <div><strong>Available Stock After Operation:</strong> ${nextAvailable}</div>`;
  openModal("narcoticStockOverflowModal");
}
async function submitNarcoticOrders() {
  if (!(APP.narcoticOrdersBatchRows || []).length && !addNarcoticOrderRow()) return;
  const rows = APP.narcoticOrdersBatchRows || [];
  for (const row of rows) {
    const stockRow = narcoticDeptStockRow(row.departmentId, row.drugId);
    const internalStock = narcoticInternalStockRow(row.drugId);
    const nextAvailable = Number(stockRow?.availableStockUnits || 0) + Number(row.quantitySent || 0);
    if (Number(row.quantitySent || 0) > Number(internalStock?.availableStockUnits || 0)) {
      showActionModal("Department Orders", "Insufficient In-Patient Pharmacy narcotic stock for the selected drug.", false);
      q("actionOkBtn").classList.remove("hidden");
      return;
    }
    if (stockRow && nextAvailable > Number(stockRow.fixedStockUnits || 0)) {
      openNarcoticOverflowModal(row, stockRow);
      return;
    }
  }
  showActionModal("Department Orders", "Please wait while order movements are being saved...");
  await commitNarcoticOrdersBatch(rows);
}
async function deleteNarcoticPrescription(id) {
  return openNarcoticConfirmActionModal("delete", id);
}
async function saveNarcoticDepartment() {
  if (APP.currentRole !== "ADMIN") return;
  const name = q("narcoticDepartmentName").value.trim();
  if (!name) return;
  const nextOrder = (APP.cache.narcoticDepartments.reduce((m,d)=>Math.max(m, Number(d.sortOrder || 0)),0) || 0) + 1;
  showActionModal("Department", "Please wait while the department is being saved...");
  await addDoc(collection(db, "narcotic_departments"), { departmentName: name, notes: q("narcoticDepartmentNotes").value.trim(), sortOrder: nextOrder, active: true, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });
  q("narcoticDepartmentName").value = ""; q("narcoticDepartmentNotes").value = "";
  await refreshTablesImmediate(["narcotic_departments"]);
  finishActionModal(true, "Department added successfully.");
}
async function deleteNarcoticDepartment(id) {
  if (APP.currentRole !== "ADMIN") return;
  showActionModal("Department", "Please wait while the department is being deleted...");
  await updateDoc(doc(db, "narcotic_departments", id), { active: false, updatedAt: serverTimestamp() });
  await refreshTablesImmediate(["narcotic_departments"]);
  finishActionModal(true, "Department deleted successfully.");
}
function setNarcoticTab(tab) {
  if (APP.currentRole !== "ADMIN" && tab !== "dispensing") tab = "dispensing";
  APP.narcoticTab = tab;
  document.querySelectorAll(".narcotic-tab-btn").forEach(btn => {
    if (btn.classList.contains("hidden")) return btn.classList.remove("active");
    btn.classList.toggle("active", btn.dataset.narcotictab === tab);
  });
  document.querySelectorAll(".narcotic-tab-content").forEach(el => {
    if (el.classList.contains("narcotic-admin-tab-content") && APP.currentRole !== "ADMIN") {
      el.classList.add("hidden");
      return;
    }
    el.classList.toggle("hidden", el.id !== `narcoticTab-${tab}`);
  });
}
function openNarcoticDepartmentModal(departmentId) {
  APP.narcoticOpenDepartmentId = departmentId;
  APP.narcoticDetailTab = "stock";
  const dept = narcoticDeptById(departmentId);
  q("narcoticDepartmentModalName").value = dept?.departmentName || "";
  if (q("narcoticDepartmentModalNotes")) q("narcoticDepartmentModalNotes").value = dept?.notes || "";
  if (q("narcoticDepartmentModalNameCard")) q("narcoticDepartmentModalNameCard").textContent = dept?.departmentName || "-";
  if (q("narcoticDeptModalFrom")) q("narcoticDeptModalFrom").value = "";
  if (q("narcoticDeptModalTo")) q("narcoticDeptModalTo").value = "";
  renderNarcoticStaticOptions();
  if (q("narcoticDeptModalDrug")) q("narcoticDeptModalDrug").selectedIndex = 0;
  renderNarcoticDepartmentFullStockSummary();
  renderNarcoticDepartmentManageTable();
  renderNarcoticDepartmentModal();
  setNarcoticDetailTab("stock");
  openModal("narcoticDepartmentModal");
}
function renderNarcoticDepartmentModal() {
  const departmentId = APP.narcoticOpenDepartmentId;
  const dept = narcoticDeptById(departmentId);
  const drugId = q("narcoticDeptModalDrug")?.value || "";
  renderNarcoticDepartmentFullStockSummary();
  renderNarcoticDepartmentManageTable();
  if (!drugId) {
    if (q("narcoticDeptSelectionHint")) q("narcoticDeptSelectionHint").classList.remove("hidden");
    if (q("narcoticDeptDetailsWrap")) q("narcoticDeptDetailsWrap").classList.add("hidden");
    if (q("narcoticDeptModalTbody")) q("narcoticDeptModalTbody").innerHTML = `<tr><td colspan="7" class="empty-state">Select a drug to show prescription history.</td></tr>`;
    return;
  }
  if (q("narcoticDeptSelectionHint")) q("narcoticDeptSelectionHint").classList.add("hidden");
  if (q("narcoticDeptDetailsWrap")) q("narcoticDeptDetailsWrap").classList.remove("hidden");

  let rows = APP.cache.narcoticPrescriptions.filter(r => r.departmentId === departmentId && r.drugId === drugId);
  const from = q("narcoticDeptModalFrom")?.value || "";
  const to = q("narcoticDeptModalTo")?.value || "";
  if (from || to) {
    rows = rows.filter(r => {
      const day = String(formatJordanDateTime(r.dateTime)).slice(0,10);
      if (from && day < from) return false;
      if (to && day > to) return false;
      return true;
    });
  } else {
    const today = jordanDateKey();
    rows = rows.filter(r => String(formatJordanDateTime(r.dateTime)).slice(0,10) === today);
  }
  rows.sort((a,b)=>String(b.dateTime||"").localeCompare(String(a.dateTime||"")));
  q("narcoticDeptModalTbody").innerHTML = rows.map(r => `<tr>
    <td>${esc(formatJordanDateTime(r.dateTime))}</td>
    <td>${esc(r.patientName || "")}</td>
    <td>${esc(r.fileNumber || "")}</td>
    <td>${esc(r.doctorName || "")}</td>
    <td>${esc(r.pharmacist || "")}</td>
    <td>${esc(r.discardDose || "-")}</td>
    <td>${narcoticActionMenu(r)}</td>
  </tr>`).join("") || `<tr><td colspan="7" class="empty-state">No prescriptions found for the selected criteria.</td></tr>`;
  const selectedStock = narcoticDeptStockRow(departmentId, drugId);
  const fixed = Number(selectedStock?.fixedStockUnits || 0);
  const avail = Number(selectedStock?.availableStockUnits || 0);
  q("narcoticDeptStockSummary").innerHTML = `
    <div class="narcotic-stock-chip"><span>Department</span><strong>${esc(dept?.departmentName || "")}</strong></div>
    <div class="narcotic-stock-chip"><span>Drug</span><strong>${esc(narcoticDrugLabel(narcoticDrugById(drugId)) || selectedStock?.drugName || "-")}</strong></div>
    <div class="narcotic-stock-chip"><span>Fixed Stock</span><strong>${fixed}</strong></div>
    <div class="narcotic-stock-chip"><span>Available Stock</span><strong>${avail}</strong></div>`;
  if (q("narcoticDeptFixedStock")) q("narcoticDeptFixedStock").value = fixed;
  if (q("narcoticDeptAvailableStock")) q("narcoticDeptAvailableStock").value = avail;
}
async function saveNarcoticDepartmentInfo() {
  if (APP.currentRole !== "ADMIN" || !APP.narcoticOpenDepartmentId) return;
  const departmentName = q("narcoticDepartmentModalName")?.value.trim() || "";
  const notes = q("narcoticDepartmentModalNotes")?.value.trim() || "";
  if (!departmentName) {
    showActionModal("Department Information", "Department name is required.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  showActionModal("Department Information", "Please wait while the department information is being updated...");
  await updateDoc(doc(db, "narcotic_departments", APP.narcoticOpenDepartmentId), {
    departmentName,
    notes,
    updatedAt: serverTimestamp()
  });
  if (q("narcoticDepartmentModalNameCard")) q("narcoticDepartmentModalNameCard").textContent = departmentName;
  await refreshTablesImmediate(["narcotic_departments"]);
  finishActionModal(true, "Department information updated successfully.");
}

async function saveNarcoticDepartmentStock() {
  if (APP.currentRole !== "ADMIN") return;
  const departmentId = APP.narcoticOpenDepartmentId;
  const drugId = q("narcoticDeptModalDrug")?.value || "";
  if (!departmentId || !drugId) {
    showActionModal("Validation", "Select a drug first to update department stock.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const stockRow = narcoticDeptStockRow(departmentId, drugId);
  if (!stockRow) return;
  const fixedStockUnits = Math.max(0, Number(q("narcoticDeptFixedStock")?.value || 0));
  const availableStockUnits = Math.max(0, Number(q("narcoticDeptAvailableStock")?.value || 0));
  showActionModal("Department Stock", "Please wait while the stock is being updated...");
  await updateDoc(doc(db, "narcotic_department_stock", stockRow.id), {
    fixedStockUnits,
    availableStockUnits,
    updatedAt: serverTimestamp()
  });
  await refreshTablesImmediate(["narcotic_department_stock"]);
  finishActionModal(true, "Department stock updated successfully.");
}

async function addNarcoticDrug() {
  if (APP.currentRole !== "ADMIN") return;
  const scientificName = q("narcoticManageScientificName")?.value.trim() || "";
  const tradeName = q("narcoticManageTradeName")?.value.trim() || "";
  const strength = q("narcoticManageStrength")?.value.trim() || "";
  const dosageForm = q("narcoticManageDosageForm")?.value.trim() || "";
  const unitsPerBox = Math.max(1, Number(q("narcoticManageUnitsPerBox")?.value || 1));
  if (!tradeName || !strength) {
    showActionModal("Validation", "Trade name and strength are required.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  showActionModal("Narcotic Drug", "Please wait while the drug is being added...");
  const newDrug = await addDoc(collection(db, "narcotic_drugs"), {
    scientificName,
    tradeName,
    strength,
    dosageForm,
    unitsPerBox,
    active: true,
    createdAt: serverTimestamp(),
    updatedAt: serverTimestamp()
  });
  const batch = writeBatch(db);
  APP.cache.narcoticDepartments.forEach(dept => {
    batch.set(doc(db, "narcotic_department_stock", `${dept.id}__${newDrug.id}`), {
      id: `${dept.id}__${newDrug.id}`,
      departmentId: dept.id,
      departmentName: dept.departmentName,
      drugId: newDrug.id,
      drugName: narcoticDrugLabel({ tradeName, strength }),
      fixedStockUnits: 0,
      availableStockUnits: 0,
      assigned: true,
      updatedAt: serverTimestamp()
    });
  });
  await batch.commit();
  ["narcoticManageScientificName","narcoticManageTradeName","narcoticManageStrength","narcoticManageDosageForm"].forEach(id => { if (q(id)) q(id).value = ""; });
  if (q("narcoticManageUnitsPerBox")) q("narcoticManageUnitsPerBox").value = 1;
  await refreshTablesImmediate(["narcotic_drugs", "narcotic_department_stock"]);
  finishActionModal(true, "Narcotic drug added successfully.");
}

async function deleteNarcoticDrug(id) {
  if (APP.currentRole !== "ADMIN") return;
  showActionModal("Narcotic Drug", "Please wait while the drug is being deleted...");
  await updateDoc(doc(db, "narcotic_drugs", id), { active: false, updatedAt: serverTimestamp() });
  await refreshTablesImmediate(["narcotic_drugs"]);
  finishActionModal(true, "Narcotic drug deleted successfully.");
}

function narcoticReportFilters() {
  return {
    departmentId: q("narcoticReportDepartment")?.value || "",
    drugId: q("narcoticReportDrug")?.value || "",
    from: q("narcoticReportFrom")?.value || "",
    to: q("narcoticReportTo")?.value || "",
    search: (q("narcoticReportSearch")?.value || "").toLowerCase().trim(),
    previewType: q("narcoticReportPreviewType")?.value || "dispensing"
  };
}

function filterNarcoticPrescriptionsForReport() {
  const {departmentId, drugId, from, to, search} = narcoticReportFilters();
  return APP.cache.narcoticPrescriptions.filter(r => {
    const day = String(formatJordanDateTime(r.dateTime)).slice(0,10);
    if (departmentId && r.departmentId !== departmentId) return false;
    if (drugId && r.drugId !== drugId) return false;
    if (from && day < from) return false;
    if (to && day > to) return false;
    if (search) {
      const haystack = `${r.patientName || ""} ${r.fileNumber || ""} ${r.doctorName || ""} ${r.pharmacist || ""} ${r.drugName || ""} ${r.prescriptionNumber || ""}`.toLowerCase();
      if (!haystack.includes(search)) return false;
    }
    return true;
  });
}

function filterNarcoticMovementsForReport() {
  const {departmentId, drugId, from, to, search} = narcoticReportFilters();
  return APP.cache.narcoticOrderMovements.filter(r => {
    if (r.type !== "Department Order Movement") return false;
    const day = String(formatJordanDateTime(r.dateTime)).slice(0,10);
    if (departmentId && r.departmentId !== departmentId) return false;
    if (drugId && r.drugId !== drugId) return false;
    if (from && day < from) return false;
    if (to && day > to) return false;
    if (search) {
      const haystack = `${r.drugName || ""} ${r.performedBy || ""} ${r.nurseName || ""} ${r.departmentName || ""} ${r.notes || ""}`.toLowerCase();
      if (!haystack.includes(search)) return false;
    }
    return true;
  });
}

function setNarcoticReportRange(preset) {
  const today = jordanDateKey();
  let from = "", to = "";
  if (preset === "today") {
    from = today; to = today;
  } else if (preset === "week") {
    const now = parseJordanDateTime(`${today} 00:00:00`) || new Date();
    const dow = (now.getDay() + 6) % 7;
    const start = new Date(now);
    start.setDate(now.getDate() - dow);
    from = formatJordanDateTime(start).slice(0,10);
    to = today;
  } else if (preset === "month") {
    from = today.slice(0,7) + "-01";
    to = today;
  }
  if (q("narcoticReportFrom")) q("narcoticReportFrom").value = from;
  if (q("narcoticReportTo")) q("narcoticReportTo").value = to;
  updateNarcoticReportPreview();
}

function renderNarcoticReportPreviewCards(rows, type) {
  if (!q("narcoticReportPreviewCards")) return;
  const filters = narcoticReportFilters();
  const dept = narcoticDeptById(filters.departmentId);
  const drug = narcoticDrugById(filters.drugId);
  let cards = [];
  if (type === "dispensing") {
    const totalUnits = rows.reduce((s, r) => s + Number(r.dispensedUnits || 0), 0);
    const totalDiscard = rows.reduce((s, r) => s + (parseFloat(String(r.discardDose || "0")) || 0), 0);
    cards = [
      ["Department", dept?.departmentName || "Not selected"],
      ["Drug", drug ? `${drug.tradeName || ""} ${drug.strength || ""}`.trim() : "All narcotic drugs"],
      ["Dispensed units", String(totalUnits)],
      ["Registered prescriptions", String(rows.length)],
      ["Discard total", String(totalDiscard || 0)]
    ];
  } else if (type === "movements") {
    const totalEmpty = rows.reduce((s, r) => s + Number(r.emptyAmpoulesReceived || 0), 0);
    const totalSent = rows.reduce((s, r) => s + Number(r.quantitySent || 0), 0);
    cards = [
      ["Department", dept?.departmentName || "Not selected"],
      ["Drug", drug ? `${drug.tradeName || ""} ${drug.strength || ""}`.trim() : "All narcotic drugs"],
      ["Movements", String(rows.length)],
      ["Empty ampoules", String(totalEmpty)],
      ["Sent ampoules", String(totalSent)]
    ];
  } else {
    const totalFixed = rows.reduce((s, r) => s + Number(r.fixedStockUnits || 0), 0);
    const totalAvail = rows.reduce((s, r) => s + Number(r.availableStockUnits || 0), 0);
    cards = [
      ["Department", dept?.departmentName || "Not selected"],
      ["Drug", drug ? `${drug.tradeName || ""} ${drug.strength || ""}`.trim() : "All narcotic drugs"],
      ["Drugs in report", String(rows.length)],
      ["Total fixed stock", String(totalFixed)],
      ["Total available stock", String(totalAvail)]
    ];
  }
  q("narcoticReportPreviewCards").innerHTML = cards.map(([label, value]) => `
    <div class="narcotic-report-card">
      <div class="narcotic-report-card-label">${esc(label)}</div>
      <div class="narcotic-report-card-value">${esc(value)}</div>
    </div>
  `).join("");
}

function updateNarcoticReportPreview() {
  if (!q("narcoticReportPreviewTbody") || !q("narcoticReportPreviewHead")) return;
  const { previewType, departmentId, drugId } = narcoticReportFilters();
  if (!departmentId) {
    q("narcoticReportPreviewCards").innerHTML = `<div class="empty-state">Select a department to preview narcotic reports.</div>`;
    q("narcoticReportPreviewHead").innerHTML = "";
    q("narcoticReportPreviewTbody").innerHTML = `<tr><td class="empty-state">Department selection is required.</td></tr>`;
    return;
  }
  if (previewType === "dispensing") {
    const rows = filterNarcoticPrescriptionsForReport();
    q("narcoticReportPreviewHead").innerHTML = `<tr><th>Date & Time</th><th>Patient</th><th>File No.</th><th>Dispensed Units</th><th>Dose</th><th>Discard</th><th>Doctor</th><th>Pharmacist</th></tr>`;
    q("narcoticReportPreviewTbody").innerHTML = rows.map(r => `<tr>
      <td>${esc(formatJordanDateTime(r.dateTime))}</td>
      <td>${esc(r.patientName || "")}</td>
      <td>${esc(r.fileNumber || "")}</td>
      <td>${Number(r.dispensedUnits || 0)}</td>
      <td>${esc(r.dose || "")}</td>
      <td>${esc(r.discardDose || "-")}</td>
      <td>${esc(r.doctorName || "")}</td>
      <td>${esc(r.pharmacist || "")}</td>
    </tr>`).join("") || `<tr><td colspan="8" class="empty-state">No dispensing rows match the current filters.</td></tr>`;
    renderNarcoticReportPreviewCards(rows, previewType);
    return;
  }
  if (previewType === "movements") {
    const rows = filterNarcoticMovementsForReport();
    q("narcoticReportPreviewHead").innerHTML = `<tr><th>Date & Time</th><th>Drug</th><th>Empty Ampoules</th><th>Sent Ampoules</th><th>Pharmacist</th><th>Nurse</th></tr>`;
    q("narcoticReportPreviewTbody").innerHTML = rows.map(r => `<tr>
      <td>${esc(formatJordanDateTime(r.dateTime))}</td>
      <td>${esc(r.drugName || "")}</td>
      <td>${Number(r.emptyAmpoulesReceived || 0)}</td>
      <td>${Number(r.quantitySent || 0)}</td>
      <td>${esc(r.performedBy || "")}</td>
      <td>${esc(r.nurseName || "")}</td>
    </tr>`).join("") || `<tr><td colspan="6" class="empty-state">No movement rows match the current filters.</td></tr>`;
    renderNarcoticReportPreviewCards(rows, previewType);
    return;
  }
  const rows = APP.cache.narcoticDepartmentStock.filter(r => r.departmentId === departmentId && (!drugId || r.drugId === drugId));
  q("narcoticReportPreviewHead").innerHTML = `<tr><th>Drug</th><th>Fixed Stock</th><th>Available Stock</th></tr>`;
  q("narcoticReportPreviewTbody").innerHTML = rows.map(r => `<tr>
    <td>${esc(r.drugName || "")}</td>
    <td>${Number(r.fixedStockUnits || 0)}</td>
    <td>${Number(r.availableStockUnits || 0)}</td>
  </tr>`).join("") || `<tr><td colspan="3" class="empty-state">No stock rows match the selected department/drug.</td></tr>`;
  renderNarcoticReportPreviewCards(rows, previewType);
}

function validateNarcoticReportDepartment() {
  if (!q("narcoticReportDepartment")?.value) {
    showActionModal("Validation", "Please choose a department first.", false);
    q("actionOkBtn").classList.remove("hidden");
    return false;
  }
  return true;
}
function printNarcoticDispensingReport() {
  if (!validateNarcoticReportDepartment()) return;
  const rows = filterNarcoticPrescriptionsForReport();
  const dept = narcoticDeptById(q("narcoticReportDepartment")?.value || "");
  const drug = narcoticDrugById(q("narcoticReportDrug")?.value || "");
  const from = q("narcoticReportFrom")?.value || "";
  const to = q("narcoticReportTo")?.value || "";
  const totalUnits = rows.reduce((s,r)=>s+Number(r.dispensedUnits||0),0);
  const body = `
    <div class="section-title">Narcotic Dispensing Report</div>
    <div class="sub"><strong>Drug:</strong> ${esc(drug ? `${drug.tradeName || ""} ${drug.strength || ""}`.trim() : "All narcotic drugs")} &nbsp; | &nbsp; <strong>Department:</strong> ${esc(dept?.departmentName || "")} &nbsp; | &nbsp; <strong>Date:</strong> ${esc(from || "-")} ${to ? `to ${esc(to)}` : ""}</div>
    <div class="section"><table><thead><tr><th>Date & Time</th><th>Patient</th><th>File No.</th><th>Dispensed Units</th><th>Dose</th><th>Discard</th><th>Doctor</th><th>Pharmacist</th></tr></thead>
    <tbody>${rows.map(r=>`<tr><td>${esc(formatJordanDateTime(r.dateTime))}</td><td>${esc(r.patientName||"")}</td><td>${esc(r.fileNumber||"")}</td><td>${Number(r.dispensedUnits||0)}</td><td>${esc(r.dose||"")}</td><td>${esc(r.discardDose||"-")}</td><td>${esc(r.doctorName||"")}</td><td>${esc(r.pharmacist||"")}</td></tr>`).join("") || `<tr><td colspan="8">No records found.</td></tr>`}</tbody></table></div>
    <div class="section"><table><thead><tr><th>Total Dispensed Units</th><th>Total Registered Prescriptions</th></tr></thead><tbody><tr><td>${totalUnits}</td><td>${rows.length}</td></tr></tbody></table></div>
    <div class="section" style="margin-top:42px;display:flex;justify-content:flex-end"><div style="min-width:280px;text-align:center"><div style="font-weight:800">Pharmacist Signature</div><div style="margin-top:38px;border-top:1px solid #1b2b44"></div></div></div>`;
  const w = window.open("", "_blank"); w.document.write(buildPrintShell("Narcotic Dispensing Report", dept?.departmentName || "Department", body)); w.document.close();
}
function printNarcoticMovementsReport() {
  if (!validateNarcoticReportDepartment()) return;
  const rows = filterNarcoticMovementsForReport();
  const dept = narcoticDeptById(q("narcoticReportDepartment")?.value || "");
  const from = q("narcoticReportFrom")?.value || "";
  const to = q("narcoticReportTo")?.value || "";
  const totalEmpty = rows.reduce((s,r)=>s+Number(r.emptyAmpoulesReceived||0),0);
  const totalSent = rows.reduce((s,r)=>s+Number(r.quantitySent||0),0);
  const body = `
    <div class="section-title">Department Order Movement Report</div>
    <div class="sub"><strong>Department:</strong> ${esc(dept?.departmentName || "")} &nbsp; | &nbsp; <strong>Date:</strong> ${esc(from || "-")} ${to ? `to ${esc(to)}` : ""}</div>
    <div class="section"><table><thead><tr><th>Date & Time</th><th>Drug</th><th>Empty Ampoules</th><th>Sent Ampoules</th><th>Pharmacist</th><th>Nurse</th></tr></thead>
    <tbody>${rows.map(r=>`<tr><td>${esc(formatJordanDateTime(r.dateTime))}</td><td>${esc(r.drugName||"")}</td><td>${Number(r.emptyAmpoulesReceived||0)}</td><td>${Number(r.quantitySent||0)}</td><td>${esc(r.performedBy||"")}</td><td>${esc(r.nurseName||"")}</td></tr>`).join("") || `<tr><td colspan="6">No movement records found.</td></tr>`}</tbody></table></div>
    <div class="section"><table><thead><tr><th>Total Empty Ampoules</th><th>Total Sent Ampoules</th></tr></thead><tbody><tr><td>${totalEmpty}</td><td>${totalSent}</td></tr></tbody></table></div>
    <div class="section" style="margin-top:42px;display:flex;justify-content:space-between;gap:36px">
      <div style="min-width:280px;text-align:center"><div style="font-weight:800">Pharmacist Signature</div><div style="margin-top:38px;border-top:1px solid #1b2b44"></div></div>
      <div style="min-width:280px;text-align:center"><div style="font-weight:800">Nurse Signature</div><div style="margin-top:38px;border-top:1px solid #1b2b44"></div></div>
    </div>`;
  const w = window.open("", "_blank"); w.document.write(buildPrintShell("Department Order Movement Report", dept?.departmentName || "Department", body)); w.document.close();
}
function printNarcoticStockReport() {
  if (!validateNarcoticReportDepartment()) return;
  const departmentId = q("narcoticReportDepartment")?.value || "";
  const drugId = q("narcoticReportDrug")?.value || "";
  const dept = narcoticDeptById(departmentId);
  const rows = narcoticAssignedStockRows(departmentId).filter(r => !drugId || r.drugId === drugId);
  const drug = narcoticDrugById(drugId);
  const body = `
    <div class="section-title">Department Stock Report</div>
    <div class="sub"><strong>Department:</strong> ${esc(dept?.departmentName || "")} &nbsp; | &nbsp; <strong>Drug:</strong> ${esc(drug ? `${drug.tradeName || ""} ${drug.strength || ""}`.trim() : "All drugs")}</div>
    <div class="section"><table><thead><tr><th>Drug</th><th>Fixed Stock</th><th>Available Stock</th></tr></thead>
    <tbody>${rows.map(r=>`<tr><td>${esc(r.drugName||"")}</td><td>${Number(r.fixedStockUnits||0)}</td><td>${Number(r.availableStockUnits||0)}</td></tr>`).join("") || `<tr><td colspan="3">No stock rows found.</td></tr>`}</tbody></table></div>
    <div class="section" style="margin-top:42px;display:flex;justify-content:flex-end"><div style="min-width:280px;text-align:center"><div style="font-weight:800">Pharmacist Signature</div><div style="margin-top:38px;border-top:1px solid #1b2b44"></div></div></div>`;
  const w = window.open("", "_blank"); w.document.write(buildPrintShell("Department Stock Report", dept?.departmentName || "Department", body)); w.document.close();
}

function printNarcoticInternalStockReport() {
  const rows = APP.cache.narcoticDrugs.map(drug => {
    const stock = narcoticInternalStockRow(drug.id);
    return {
      label: `${drug.tradeName || ''} ${drug.strength || ''}`.trim(),
      available: Number(stock?.availableStockUnits || 0),
      reorder: Number(stock?.reorderLevelUnits ?? drug.reorderLevelUnits ?? 0),
      dosageForm: drug.dosageForm || ''
    };
  }).sort((a, b) => a.label.localeCompare(b.label));
  const body = `
    <div class="section-title">In-Patient Pharmacy Narcotic Stock</div>
    <div class="sub"><strong>Printed at:</strong> ${esc(formatJordanDateTime(jordanNowIso(), true))}</div>
    <div class="section"><table><thead><tr><th>Drug</th><th>Dosage Form</th><th>Available Stock</th><th>Reorder Level</th><th>Status</th></tr></thead>
    <tbody>${rows.map(r => `<tr><td>${esc(r.label)}</td><td>${esc(r.dosageForm)}</td><td>${r.available}</td><td>${r.reorder}</td><td>${r.available <= r.reorder ? 'Below Reorder' : 'OK'}</td></tr>`).join('') || `<tr><td colspan="5">No internal narcotic stock rows found.</td></tr>`}</tbody></table></div>`;
  const w = window.open('', '_blank');
  w.document.write(buildPrintShell('In-Patient Pharmacy Narcotic Stock', 'Internal narcotic stock', body));
  w.document.close();
}

function openNarcoticConsumptionModal() {
  const today = jordanDateKey();
  if (q('narcoticConsumptionFrom') && !q('narcoticConsumptionFrom').value) q('narcoticConsumptionFrom').value = today;
  if (q('narcoticConsumptionTo') && !q('narcoticConsumptionTo').value) q('narcoticConsumptionTo').value = today;
  openModal('narcoticConsumptionModal');
}

function printNarcoticConsumptionReport() {
  const from = q('narcoticConsumptionFrom')?.value || '';
  const to = q('narcoticConsumptionTo')?.value || '';
  const drugId = q('narcoticConsumptionDrug')?.value || '';
  const effectiveFrom = from || (!to ? jordanDateKey() : '');
  const effectiveTo = to || (!from ? jordanDateKey() : '');
  const selectedDrug = narcoticDrugById(drugId);
  const rows = (APP.cache.narcoticPrescriptions || []).filter(r => {
    const day = String(formatJordanDateTime(r.dateTime)).slice(0, 10);
    if (effectiveFrom && day < effectiveFrom) return false;
    if (effectiveTo && day > effectiveTo) return false;
    if (drugId && String(r.drugId || '') !== String(drugId)) return false;
    return true;
  });

  const consumedByDrug = new Map();
  rows.forEach(r => {
    const key = String(r.drugId || '');
    consumedByDrug.set(key, Number(consumedByDrug.get(key) || 0) + Number(r.dispensedUnits || 0));
  });

  const drugsToShow = (drugId ? APP.cache.narcoticDrugs.filter(drug => String(drug.id) === String(drugId)) : APP.cache.narcoticDrugs)
    .slice()
    .sort((a, b) => narcoticDrugLabel(a).localeCompare(narcoticDrugLabel(b)));

  const tableRows = drugsToShow.map(drug => {
    const stock = narcoticInternalStockRow(drug.id);
    return {
      label: narcoticDrugLabel(drug),
      consumed: Number(consumedByDrug.get(String(drug.id)) || 0),
      available: Number(stock?.availableStockUnits || 0)
    };
  });

  const body = `
    <div class="section-title">Narcotic Consumption Report</div>
    <div class="sub"><strong>Drug:</strong> ${esc(selectedDrug ? narcoticDrugLabel(selectedDrug) : 'All narcotic drugs')} &nbsp; | &nbsp; <strong>Period:</strong> ${esc(effectiveFrom || '-')} ${effectiveTo ? `to ${esc(effectiveTo)}` : ''}</div>
    <div class="section"><table><thead><tr><th>Drug</th><th>Consumed Quantity</th><th>Available Quantity in In-Patient Internal Stock</th></tr></thead>
    <tbody>${tableRows.map(r => `<tr><td>${esc(r.label)}</td><td>${r.consumed}</td><td>${r.available}</td></tr>`).join('') || `<tr><td colspan="3">No narcotic drugs found.</td></tr>`}</tbody></table></div>`;
  closeModal('narcoticConsumptionModal');
  const w = window.open('', '_blank');
  w.document.write(buildPrintShell('Narcotic Consumption Report', 'All departments consumption summary', body));
  w.document.close();
}
function handleNarcoticDepartmentDragStart(event) {
  const card = event.target.closest("[data-narcotic-dept-card]");
  if (!card || APP.currentRole !== "ADMIN") return;
  event.dataTransfer.setData("text/plain", card.dataset.narcoticDeptCard);
  card.classList.add("dragging");
}
function handleNarcoticDepartmentDragEnd(event) {
  const card = event.target.closest("[data-narcotic-dept-card]");
  if (card) card.classList.remove("dragging");
}
async function handleNarcoticDepartmentDrop(event) {
  const target = event.target.closest("[data-narcotic-dept-card]");
  if (!target || APP.currentRole !== "ADMIN") return;
  event.preventDefault();
  const draggedId = event.dataTransfer.getData("text/plain");
  if (!draggedId || draggedId === target.dataset.narcoticDeptCard) return;
  const dragged = narcoticDeptById(draggedId), dropped = narcoticDeptById(target.dataset.narcoticDeptCard);
  if (!dragged || !dropped) return;
  await updateDoc(doc(db, "narcotic_departments", dragged.id), { sortOrder: Number(dropped.sortOrder || 0), updatedAt: serverTimestamp() });
  await updateDoc(doc(db, "narcotic_departments", dropped.id), { sortOrder: Number(dragged.sortOrder || 0), updatedAt: serverTimestamp() });
}




function renderNarcoticRecentRows() {
  if (!q("narcoticRecentModalTbody")) return;
  const from = q("narcoticRecentFrom")?.value || "";
  const to = q("narcoticRecentTo")?.value || "";
  const effectiveFrom = from || (!to ? jordanDateKey() : "");
  const effectiveTo = to || (!from ? jordanDateKey() : "");
  ensureArchiveCacheForSection('narcoticPrescriptions');
  const rows = getMergedNarcoticPrescriptionRows(getArchiveMode('narcoticPrescriptions')).filter(row => {
    const day = formatJordanDateTime(row.dateTime).slice(0, 10);
    if (effectiveFrom && day < effectiveFrom) return false;
    if (effectiveTo && day > effectiveTo) return false;
    return true;
  }).slice(0, 50);
  q("narcoticRecentModalTbody").innerHTML = rows.map(row => {
    const drug = narcoticDrugById(row.drugId);
    const dept = narcoticDeptById(row.departmentId);
    return `<tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc((drug?.tradeName || row.drugName || "") + " " + (drug?.strength || ""))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(dept?.departmentName || row.departmentName || "")}</td>
      <td>${Number(row.dispensedUnits || 0)}</td>
      <td>${esc(row.discardDose || "-")}</td>
      <td>${esc(row.pharmacist || "")}</td>
      <td>${isArchivedRow(row) ? '<span class="badge pending">Archived</span>' : narcoticActionMenu(row)}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="8" class="empty-state">No narcotic prescriptions found.</td></tr>`;
}

function openNarcoticRecentModal() {
  renderNarcoticRecentRows();
  openModal("narcoticRecentModal");
}
function openNarcoticDrugInfoModal(drugId) {
  const drug = narcoticDrugById(drugId);
  if (!drug) return;
  APP.narcoticOpenDrugId = drugId;
  const totalAvail = APP.cache.narcoticDepartmentStock.filter(r => r.drugId === drugId).reduce((s,r)=>s+Number(r.availableStockUnits||0),0);
  q("narcoticInfoScientificName").value = drug.scientificName || "";
  q("narcoticInfoTradeName").value = drug.tradeName || "";
  q("narcoticInfoStrength").value = drug.strength || "";
  q("narcoticInfoDosageForm").value = drug.dosageForm || "";
  q("narcoticInfoUnitsPerBox").value = Number(drug.unitsPerBox || 1);
  q("narcoticInfoTotalAvailable").value = totalAvail;
  const readOnly = APP.currentRole !== "ADMIN";
  ["narcoticInfoScientificName","narcoticInfoTradeName","narcoticInfoStrength","narcoticInfoDosageForm","narcoticInfoUnitsPerBox"].forEach(id => q(id).readOnly = readOnly);
  if (q("saveNarcoticDrugInfoBtn")) q("saveNarcoticDrugInfoBtn").classList.toggle("hidden", readOnly);
  if (q("deleteNarcoticDrugInfoBtn")) q("deleteNarcoticDrugInfoBtn").classList.toggle("hidden", readOnly);
  openModal("narcoticDrugInfoModal");
}
async function saveNarcoticDrugInfo() {
  if (APP.currentRole !== "ADMIN" || !APP.narcoticOpenDrugId) return;
  await updateDoc(doc(db, "narcotic_drugs", APP.narcoticOpenDrugId), {
    scientificName: q("narcoticInfoScientificName").value.trim(),
    tradeName: q("narcoticInfoTradeName").value.trim(),
    strength: q("narcoticInfoStrength").value.trim(),
    dosageForm: q("narcoticInfoDosageForm").value.trim(),
    unitsPerBox: Math.max(1, Number(q("narcoticInfoUnitsPerBox").value || 1)),
    updatedAt: serverTimestamp()
  });
  closeModal("narcoticDrugInfoModal");
}
async function deleteNarcoticDrugInfo() {
  if (APP.currentRole !== "ADMIN" || !APP.narcoticOpenDrugId) return;
  await deleteNarcoticDrug(APP.narcoticOpenDrugId);
  closeModal("narcoticDrugInfoModal");
}
function openNarcoticEditPrescription(id) {
  const row = APP.cache.narcoticPrescriptions.find(r => r.id === id);
  if (!row) return;
  APP.narcoticEditPrescriptionId = id;
  const drug = narcoticDrugById(row.drugId);
  q("narcoticEditDrugLabel").value = `${drug?.tradeName || row.drugName || ""} ${drug?.strength || ""}`.trim();
  q("narcoticEditPatientName").value = row.patientName || "";
  q("narcoticEditFileNumber").value = row.fileNumber || "";
  q("narcoticEditDoctorName").value = row.doctorName || "";
  q("narcoticEditPrescriptionNumber").value = row.prescriptionNumber || "";
  q("narcoticEditDose").value = row.dose || "";
  q("narcoticEditDispensedUnits").value = Number(row.dispensedUnits || 1);
  q("narcoticEditDiscardDose").value = row.discardDose || "";
  q("narcoticEditNotes").value = row.notes || "";
  q("narcoticEditPharmacist").innerHTML = q("narcoticPharmacist").innerHTML;
  q("narcoticEditPharmacist").value = currentActorName() || row.pharmacist || "";
  applyCurrentUserReadonlyFields();
  openModal("narcoticEditPrescriptionModal");
}
async function saveNarcoticEditPrescription() {
  const id = APP.narcoticEditPrescriptionId;
  const row = APP.cache.narcoticPrescriptions.find(r => r.id === id);
  if (!row) return;
  const newUnits = Math.max(1, Number(q("narcoticEditDispensedUnits").value || 1));
  const stockRow = narcoticDeptStockRow(row.departmentId, row.drugId);
  const delta = newUnits - Number(row.dispensedUnits || 0);
  if (delta > 0 && Number(stockRow?.availableStockUnits || 0) < delta) {
    showActionModal("Stock Validation", "Not enough available stock to increase dispensed units.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  showActionModal("Edit Narcotic Prescription", "Please wait while the narcotic prescription is being updated...");
  const batch = writeBatch(db);
  batch.update(doc(db, "narcotic_prescriptions", id), {
    patientName: q("narcoticEditPatientName").value.trim(),
    fileNumber: q("narcoticEditFileNumber").value.trim(),
    doctorName: q("narcoticEditDoctorName").value.trim(),
    prescriptionNumber: q("narcoticEditPrescriptionNumber").value.trim(),
    dose: q("narcoticEditDose").value.trim(),
    dispensedUnits: newUnits,
    discardDose: q("narcoticEditDiscardDose").value.trim(),
    pharmacist: (q("narcoticEditPharmacist").value || currentActorName()).trim(),
    notes: q("narcoticEditNotes").value.trim(),
    updatedAt: serverTimestamp(),
    updatedBy: APP.currentRole
  });
  if (stockRow?.id && delta !== 0) {
    batch.update(doc(db, "narcotic_department_stock", stockRow.id), {
      availableStockUnits: Math.max(0, Number(stockRow.availableStockUnits || 0) - delta),
      updatedAt: serverTimestamp()
    });
  }
  await batch.commit();
  closeModal("narcoticEditPrescriptionModal");
  finishActionModal(true, "Narcotic prescription updated successfully.");
}

document.addEventListener("click", event => {
  if (!event.target.closest(".rx-actions-wrap")) closeRxActionMenus();
  const roleCard = event.target.closest(".login-card");
  if (roleCard) {
    APP.pendingRole = roleCard.dataset.role;
    q("passwordModalRole").textContent = `${USERS[APP.pendingRole]?.displayName || "Login"} · ${APP.pendingRole === "ADMIN" ? "Admin Portal" : portalToScope(APP.pendingRole)}`;
    if (q("loginEmployeeNumber")) q("loginEmployeeNumber").value = "";
    q("loginPassword").value = "";
    openModal("passwordModal");
    setTimeout(() => q("loginEmployeeNumber")?.focus(), 0);
    return;
  }

  const nav = event.target.closest(".nav-link[data-page]");
  if (nav) return showPage(nav.dataset.page);

  const narcoticTabBtn = event.target.closest(".narcotic-tab-btn[data-narcotictab]");
  if (narcoticTabBtn) {
    setNarcoticTab(narcoticTabBtn.dataset.narcotictab);
    return;
  }

  const tab = event.target.closest(".tab-btn[data-audittab]");
  if (tab) {
    APP.auditTab = tab.dataset.audittab;
    document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.toggle("active", btn === tab));
    renderAudit();
    return;
  }

  const narcoticDeptCard = event.target.closest("[data-narcotic-dept-card]");
  if (narcoticDeptCard && !event.target.closest(".delete-narcotic-department-btn")) return openNarcoticDepartmentModal(narcoticDeptCard.dataset.narcoticDeptCard);

  const openDrugBtn = event.target.closest(".open-drug-btn");
  if (openDrugBtn) return openDrug(openDrugBtn.dataset.drugid);

  const drugCard = event.target.closest(".drug-card[data-drugid]");
  if (drugCard) return openDrug(drugCard.dataset.drugid);

  const auditMenuBtn = event.target.closest("[data-audit-menu]");
  if (auditMenuBtn) {
    openAuditMenuPortal(auditMenuBtn, auditMenuBtn.dataset.auditMenu);
    return;
  }

  const auditBtn = event.target.closest(".audit-btn");
  if (auditBtn) {
    closeAuditMenuPortal();
    return auditPrescription(auditBtn.dataset.id, auditBtn.dataset.status, q(auditBtn.dataset.note).value);
  }

  const auditExpandBtn = event.target.closest("[data-audit-expand]");
  if (auditExpandBtn) {
    APP.openAuditDetailId = String(APP.openAuditDetailId || '') === String(auditExpandBtn.dataset.auditExpand) ? null : auditExpandBtn.dataset.auditExpand;
    renderAudit();
    return;
  }

  const returnBtn = event.target.closest(".return-btn");
  if (returnBtn) return openConfirmActionModal("return", returnBtn.dataset.id);


  const patientToggleBtn = event.target.closest("[data-patient-toggle]");
  if (patientToggleBtn) {
    APP.openPatientPrescriptionId = String(APP.openPatientPrescriptionId || "") === String(patientToggleBtn.dataset.patientToggle) ? null : patientToggleBtn.dataset.patientToggle;
    renderPatientsPage();
    return;
  }

  const addDoseBtn = event.target.closest("[data-add-dose]");
  if (addDoseBtn) {
    openAddDoseModal(addDoseBtn.dataset.addDose);
    return;
  }

  const rxMenuToggle = event.target.closest("[data-rx-actions-toggle]");
  if (rxMenuToggle) {
    toggleRxActionsMenu(rxMenuToggle.dataset.rxActionsToggle);
    return;
  }

  const latestReturnBtn = event.target.closest(".latest-return-btn");
  if (latestReturnBtn) {
    const portalContext = latestReturnBtn.closest(".rx-actions-portal")?.dataset.context;
    closeRxActionMenus();
    if (portalContext === "narcotic" || latestReturnBtn.closest("#page-narcotic, #narcoticDepartmentModal, #narcoticRecentModal")) return openNarcoticConfirmActionModal("return", latestReturnBtn.dataset.id);
    return openConfirmActionModal("return", latestReturnBtn.dataset.id);
  }

  const editBtn = event.target.closest(".edit-rx-btn");
  if (editBtn) {
    closeAuditMenuPortal();
    return openEditPrescription(editBtn.dataset.id);
  }

  const latestEditBtn = event.target.closest(".latest-edit-btn");
  if (latestEditBtn) {
    const portalContext = latestEditBtn.closest(".rx-actions-portal")?.dataset.context;
    closeRxActionMenus();
    if (portalContext === "narcotic" || latestEditBtn.closest("#page-narcotic, #narcoticDepartmentModal, #narcoticRecentModal")) return openNarcoticEditPrescription(latestEditBtn.dataset.id);
    return openEditPrescription(latestEditBtn.dataset.id);
  }

  const deleteRxBtn = event.target.closest(".delete-rx-btn");
  if (deleteRxBtn) {
    const portalContext = deleteRxBtn.closest(".rx-actions-portal")?.dataset.context;
    closeAuditMenuPortal();
    closeRxActionMenus();
    if (portalContext === "narcotic" || deleteRxBtn.closest("#page-narcotic, #narcoticDepartmentModal, #narcoticRecentModal")) return deleteNarcoticPrescription(deleteRxBtn.dataset.id);
    return openConfirmActionModal("delete", deleteRxBtn.dataset.id);
  }

  const narcoticDrugCardBtn = event.target.closest("[data-narcotic-drug-card]");
  if (narcoticDrugCardBtn) {
    return openNarcoticDrugInfoModal(narcoticDrugCardBtn.dataset.narcoticDrugCard);
  }

  const adjustNarcoticInternalStockBtn = event.target.closest("[data-adjust-narcotic-internal-stock]");
  if (adjustNarcoticInternalStockBtn) return openNarcoticInternalStockAdjustModal(adjustNarcoticInternalStockBtn.dataset.adjustNarcoticInternalStock);

  const narcoticEditBtn = event.target.closest(".narcotic-edit-btn");
  if (narcoticEditBtn) {
    closeRxActionMenus();
    return openNarcoticEditPrescription(narcoticEditBtn.dataset.id);
  }

  const narcoticDeleteBtn = event.target.closest(".narcotic-delete-btn");
  if (narcoticDeleteBtn) {
    closeRxActionMenus();
    return deleteNarcoticPrescription(narcoticDeleteBtn.dataset.id);
  }

  const removeNarcoticOrderRowBtn = event.target.closest("[data-remove-narcotic-order-row]");
  if (removeNarcoticOrderRowBtn) {
    APP.narcoticOrdersBatchRows.splice(Number(removeNarcoticOrderRowBtn.dataset.removeNarcoticOrderRow), 1);
    renderNarcoticOrdersTable();
    return;
  }

  const removeShipmentRowBtn = event.target.closest("[data-remove-shipment-row]");
  if (removeShipmentRowBtn) {
    APP.shipmentBatchRows.splice(Number(removeShipmentRowBtn.dataset.removeShipmentRow), 1);
    renderShipmentBatchTable();
    return;
  }

  const removeTransferRowBtn = event.target.closest("[data-remove-transfer-row]");
  if (removeTransferRowBtn) {
    APP.transferBatchRows.splice(Number(removeTransferRowBtn.dataset.removeTransferRow), 1);
    renderTransferBatchTable();
    return;
  }

  const adjustStockBtn = event.target.closest("[data-adjust-stock]");
  if (adjustStockBtn) return openAdjustStockModal(adjustStockBtn.dataset.adjustStock);

  const editUserBtn = event.target.closest("[data-edit-user]");
  if (editUserBtn) return startEditUser(editUserBtn.dataset.editUser);

  const removeDeptDrugBtn = event.target.closest("[data-remove-dept-drug]");
  if (removeDeptDrugBtn) return removeDrugFromNarcoticDepartment(removeDeptDrugBtn.dataset.removeDeptDrug);

  const deleteNarcoticDepartmentBtn = event.target.closest(".delete-narcotic-department-btn");
  if (deleteNarcoticDepartmentBtn) return deleteNarcoticDepartment(deleteNarcoticDepartmentBtn.dataset.id);

  const deleteNarcoticDrugBtn = event.target.closest(".delete-narcotic-drug-btn");
  if (deleteNarcoticDrugBtn) return deleteNarcoticDrug(deleteNarcoticDrugBtn.dataset.id);


  if (event.target.id === "userMenuBtn") {
    q("userMenuDropdown").classList.toggle("hidden");
    return;
  }

  if (!event.target.closest(".user-menu-wrap") && !q("userMenuDropdown").classList.contains("hidden")) {
    q("userMenuDropdown").classList.add("hidden");
  }

  if (!event.target.closest(".audit-menu-wrap")) {
    closeAuditMenuPortal();
  }

  if (!event.target.closest(".rx-actions-wrap")) {
    closeRxActionMenus();
  }

  if (event.target.dataset.close) return closeModal(event.target.dataset.close);
});


q("narcoticDrug").onchange = updateNarcoticAvailableStock;
q("narcoticDepartment").onchange = updateNarcoticAvailableStock;
q("narcoticDrugSearch").oninput = renderNarcoticStaticOptions;
q("narcoticClearBtn").onclick = resetNarcoticEntryForm;
if (q("openNarcoticRecentModalBtn")) q("openNarcoticRecentModalBtn").onclick = openNarcoticRecentModal;
if (q("narcoticRecentFrom")) q("narcoticRecentFrom").onchange = renderNarcoticRecentRows;
if (q("narcoticRecentTo")) q("narcoticRecentTo").onchange = renderNarcoticRecentRows;
q("registerNarcoticBtn").onclick = registerNarcoticPrescription;
q("narcoticDepartmentSearch").oninput = renderNarcoticDepartmentCards;
q("addNarcoticOrderRowBtn").onclick = addNarcoticOrderRow;
q("submitNarcoticOrdersBtn").onclick = submitNarcoticOrders;
q("saveNarcoticDepartmentBtn").onclick = saveNarcoticDepartment;
q("addNarcoticDrugBtn").onclick = addNarcoticDrug;
q("saveNarcoticDeptStockBtn").onclick = saveNarcoticDepartmentStock;
if (q("saveNarcoticDepartmentInfoBtn")) q("saveNarcoticDepartmentInfoBtn").onclick = saveNarcoticDepartmentInfo;
if (q("saveNarcoticInternalStockAdjustBtn")) q("saveNarcoticInternalStockAdjustBtn").onclick = saveNarcoticInternalStockAdjust;
q("narcoticDeptModalDrug").onchange = renderNarcoticDepartmentModal;
q("narcoticDeptModalFrom").onchange = renderNarcoticDepartmentModal;
q("narcoticDeptModalTo").onchange = renderNarcoticDepartmentModal;
q("printNarcoticDispensingReportBtn").onclick = printNarcoticDispensingReport;
q("printNarcoticMovementsReportBtn").onclick = printNarcoticMovementsReport;
q("printNarcoticStockReportBtn").onclick = printNarcoticStockReport;
["narcoticReportDepartment","narcoticReportDrug","narcoticReportFrom","narcoticReportTo","narcoticReportSearch","narcoticReportPreviewType"].forEach(id => {
  if (q(id)) {
    q(id).oninput = updateNarcoticReportPreview;
    q(id).onchange = updateNarcoticReportPreview;
  }
});
if (q("narcoticReportTodayBtn")) q("narcoticReportTodayBtn").onclick = () => setNarcoticReportRange("today");
if (q("narcoticReportWeekBtn")) q("narcoticReportWeekBtn").onclick = () => setNarcoticReportRange("week");
if (q("narcoticReportMonthBtn")) q("narcoticReportMonthBtn").onclick = () => setNarcoticReportRange("month");
if (q("narcoticReportClearBtn")) q("narcoticReportClearBtn").onclick = () => {
  if (q("narcoticReportFrom")) q("narcoticReportFrom").value = "";
  if (q("narcoticReportTo")) q("narcoticReportTo").value = "";
  updateNarcoticReportPreview();
};
q("saveNarcoticDrugInfoBtn").onclick = saveNarcoticDrugInfo;
q("deleteNarcoticDrugInfoBtn").onclick = deleteNarcoticDrugInfo;
q("saveNarcoticEditPrescriptionBtn").onclick = saveNarcoticEditPrescription;
q("cancelNarcoticOverflowBtn").onclick = () => { APP.narcoticPendingOverflowRows = null; closeModal("narcoticStockOverflowModal"); };
q("confirmNarcoticOverflowBtn").onclick = async () => { const rows = APP.narcoticPendingOverflowRows || []; closeModal("narcoticStockOverflowModal"); if (!rows.length) return; showActionModal("Department Orders", "Please wait while order movements are being saved..."); await commitNarcoticOrdersBatch(rows); APP.narcoticPendingOverflowRows = null; };
document.addEventListener("dragstart", handleNarcoticDepartmentDragStart);
document.addEventListener("dragend", handleNarcoticDepartmentDragEnd);
document.addEventListener("dragover", event => { if (event.target.closest("[data-narcotic-dept-card]")) event.preventDefault(); });
document.addEventListener("drop", handleNarcoticDepartmentDrop);

q("passwordCancelBtn").onclick = () => closeModal("passwordModal");
q("passwordLoginBtn").onclick = () => doLogin(APP.pendingRole, q("loginEmployeeNumber")?.value, q("loginPassword").value);
q("actionOkBtn").onclick = () => closeModal("actionModal");
q("changePasswordBtn").onclick = () => { q("userMenuDropdown").classList.add("hidden"); openModal("changePasswordModal"); };
q("savePasswordBtn").onclick = savePassword;
q("logoutBtn").onclick = () => {
  q("userMenuDropdown").classList.add("hidden");
  APP.listeners.forEach(unsub => unsub && unsub());
  APP.listeners = [];
  localStorage.removeItem("cdms_session_role");
  sessionStorage.removeItem("cdms_session_user_id");
  sessionStorage.removeItem("cdms_session_portal");
  sessionStorage.removeItem("cdms_session_scope");
  sessionStorage.removeItem("cdms_session_login_method");
  APP.currentRole = null;
  APP.currentUser = null;
  APP.currentUserDocId = null;
  APP.currentPortal = null;
  APP.currentPharmacyScope = null;
  APP.loginMethod = null;
  APP.archiveView = { dashboard: "all", patients: "all", prescriptions: "all", transactions: "all", narcoticPrescriptions: "all", narcoticMovements: "all" };
  APP.archiveCache = { prescriptions: { rows: [], loaded: false, loading: null }, transactions: { rows: [], loaded: false, loading: null }, narcoticPrescriptions: { rows: [], loaded: false, loading: null }, narcoticOrderMovements: { rows: [], loaded: false, loading: null } };
  q("appShell").classList.add("hidden");
  q("loginScreen").classList.remove("hidden");
  updateLayoutMode();
};
q("themeToggle").onclick = () => { q("userMenuDropdown").classList.add("hidden"); openModal("themeModal"); };
q("registerQuickBtn").onclick = registerQuickPrescription;
q("saveEditPrescriptionBtn").onclick = saveEditedPrescription;
q("saveAdjustStockBtn").onclick = saveAdjustedStock;
if (q("saveSettingsBtn")) q("saveSettingsBtn").onclick = saveSettings;
if (q("resetPasswordOpenBtn")) q("resetPasswordOpenBtn").onclick = openResetPasswordModal;
q("confirmResetPasswordBtn").onclick = resetSelectedPassword;
if (q("settingsPharmacy")) q("settingsPharmacy").onchange = () => {
  if (APP.currentRole !== "ADMIN") return;
  APP.cache.settings = { ...(APP.cache.settings || {}), pharmacyType: q("settingsPharmacy").value };
  renderAll();
};
if (q("saveUserBtn")) q("saveUserBtn").onclick = saveUser;
if (q("saveUserBtnModal")) q("saveUserBtnModal").onclick = saveUser;
if (q("cancelUserEditBtn")) q("cancelUserEditBtn").onclick = () => resetUserForm(true);
if (q("cancelUserEditBtnModal")) q("cancelUserEditBtnModal").onclick = () => resetUserForm(true);
if (q("toggleUserActiveBtn")) q("toggleUserActiveBtn").onclick = () => { const id = q("toggleUserActiveBtn").dataset.id; if (id) toggleUserActive(id); };
if (q("toggleUserActiveBtnModal")) q("toggleUserActiveBtnModal").onclick = () => { const id = q("toggleUserActiveBtnModal").dataset.id; if (id) toggleUserActive(id); };
if (q("openUserModalBtn")) q("openUserModalBtn").onclick = () => { resetUserForm(false); openModal("userEditorModal"); };
q("saveDrugInfoBtn").onclick = saveDrugInfo;
q("deleteDrugBtn").onclick = deleteDrug;
q("openShipmentModalBtn").onclick = () => { refreshScopedSelectors(); APP.shipmentBatchRows = []; renderShipmentBatchTable(); clearShipmentInputs(); openModal("shipmentModal"); };
q("openShipmentModalBtn2").onclick = () => { refreshScopedSelectors(); APP.shipmentBatchRows = []; renderShipmentBatchTable(); clearShipmentInputs(); openModal("shipmentModal"); };
q("openTransferModalBtn").onclick = () => { refreshScopedSelectors(); APP.transferBatchRows = []; renderTransferBatchTable(); clearTransferInputs(); openModal("transferModal"); };
q("openTransferModalBtn2").onclick = () => { refreshScopedSelectors(); APP.transferBatchRows = []; renderTransferBatchTable(); clearTransferInputs(); openModal("transferModal"); };
if (q("transferFrom")) q("transferFrom").addEventListener("change", () => syncTransferToOptions());
q("openAddDrugModalBtn").onclick = () => openModal("addDrugModal");
q("openAddDrugModalBtn2").onclick = () => openModal("addDrugModal");
q("addShipmentRowBtn").onclick = addShipmentBatchRow;
q("addTransferRowBtn").onclick = addTransferBatchRow;
q("saveShipmentBtn").onclick = receiveShipment;
q("saveTransferBtn").onclick = transferStock;
q("saveNewDrugBtn").onclick = addDrug;
q("printDrugReportBtn").onclick = printDrugReport;
q("printComprehensiveReportBtn").onclick = printComprehensiveReport;
q("inventorySearch").oninput = renderInventory;
q("inventoryLocationFilter").onchange = () => { renderInventory(); updateQuickAvailableStock(); };
q("reportPharmacy").onchange = () => { renderReports?.(); };
q("auditPharmacy").onchange = renderAudit;
if (q("auditFromDate")) q("auditFromDate").onchange = renderAudit;
if (q("auditToDate")) q("auditToDate").onchange = renderAudit;
q("transactionsSearch").oninput = renderTransactions;
q("drugCardsSearch").oninput = () => { APP.drugCardsPage = 1; renderDashboard(); };
q("auditSearch").oninput = renderAudit;
q("quickDrug").onchange = updateQuickAvailableStock;
q("confirmActionCancelBtn").onclick = () => { APP.confirmAction = null; closeModal("confirmActionModal"); };
q("confirmActionOkBtn").onclick = submitConfirmedAction;
q("duplicateWarningCancelBtn").onclick = () => { APP.pendingQuickPayload = null; closeModal("duplicateWarningModal"); };
q("duplicateWarningContinueBtn").onclick = async () => { const payload = APP.pendingQuickPayload; closeModal("duplicateWarningModal"); if (payload) await continueQuickRegistration(payload); };
q("viewPatientHistoryBtn").onclick = openPatientHistoryModal;
q("closePatientHistoryBtn").onclick = () => closeModal("patientHistoryModal");
document.querySelectorAll("[data-close]").forEach(btn => { btn.onclick = () => closeModal(btn.dataset.close); });
q("actionOkBtn").onclick = () => closeModal("actionModal");

if (APP.liveTimer) clearInterval(APP.liveTimer);
APP.liveTimer = setInterval(renderLiveClocks, 1000);
renderLiveClocks();


q("quickDrugSearch").oninput = filterQuickDrugOptions;
if (q("quickDrugSearch")) {
  q("quickDrugSearch").addEventListener("keydown", event => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      q("quickDrug")?.focus();
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const visibleOption = [...q("quickDrug").options].find((opt, index) => index > 0 && !opt.hidden);
      if (visibleOption) q("quickDrug").value = visibleOption.value;
      updateQuickAvailableStock();
      q("quickDrug")?.focus();
    }
  });
}
q("openRecentPrescriptionsBtn").onclick = openRecentPrescriptionsModal;
q("recentPrescriptionsSearch").oninput = renderRecentPrescriptionsModal;

["patientsFileFilter","patientsDrugFilter","patientsPharmacyFilter","patientsStatusFilter","patientsFromFilter","patientsToFilter","patientsSearchFilter"].forEach(id => {
  if (q(id)) {
    q(id).oninput = renderPatientsPage;
    q(id).onchange = renderPatientsPage;
  }
});
if (q("patientsTodayBtn")) q("patientsTodayBtn").onclick = () => {
  const today = jordanDateKey();
  if (q("patientsFromFilter")) q("patientsFromFilter").value = today;
  if (q("patientsToFilter")) q("patientsToFilter").value = today;
  renderPatientsPage();
};
if (q("patientsClearFiltersBtn")) q("patientsClearFiltersBtn").onclick = () => {
  ["patientsFileFilter","patientsFromFilter","patientsToFilter","patientsSearchFilter"].forEach(id => { if (q(id)) q(id).value = ""; });
  ["patientsDrugFilter","patientsPharmacyFilter","patientsStatusFilter"].forEach(id => { if (q(id)) q(id).value = ""; });
  renderPatientsPage();
};
["doseUnitsPerDose","doseFrequency","doseDurationValue","doseDurationUnit","doseStartDate"].forEach(id => {
  if (q(id)) {
    q(id).oninput = updateDosePreview;
    q(id).onchange = updateDosePreview;
  }
});
if (q("saveDoseBtn")) q("saveDoseBtn").onclick = saveDoseForPrescription;
if (q("openPatientProfileBtn")) q("openPatientProfileBtn").onclick = openPatientProfileFromDuplicate;
if (q("printPatientMedicationHistoryBtn")) q("printPatientMedicationHistoryBtn").onclick = printPatientMedicationHistory;
if (q("printPatientMedicationHistoryModalBtn")) q("printPatientMedicationHistoryModalBtn").onclick = printPatientMedicationHistory;

if (q('prescriptionsPrintLabelBtn')) q('prescriptionsPrintLabelBtn').onclick = openPrescriptionLabelModal;
if (q('closePrescriptionLabelModalBtn')) q('closePrescriptionLabelModalBtn').onclick = () => closeModal('prescriptionLabelModal');
if (q('printPrescriptionLabelConfirmBtn')) q('printPrescriptionLabelConfirmBtn').onclick = printPrescriptionLabels;

if (q("viewPatientHistoryBtn")) q("viewPatientHistoryBtn").onclick = openPatientHistoryFromDuplicate;

if (q("drugRxFromDate")) q("drugRxFromDate").onchange = renderDrugRows;
if (q("drugRxToDate")) q("drugRxToDate").onchange = renderDrugRows;
if (q("drugRxTodayBtn")) q("drugRxTodayBtn").onclick = () => {
  const today = jordanDateKey();
  q("drugRxFromDate").value = today;
  q("drugRxToDate").value = today;
  renderDrugRows();
};
if (q("drugRxClearBtn")) q("drugRxClearBtn").onclick = () => {
  q("drugRxFromDate").value = "";
  q("drugRxToDate").value = "";
  renderDrugRows();
};
if (q("recentPrescriptionsFrom")) q("recentPrescriptionsFrom").onchange = renderRecentPrescriptionsModal;
if (q("recentPrescriptionsTo")) q("recentPrescriptionsTo").onchange = renderRecentPrescriptionsModal;
q("closeRecentPrescriptionsBtn").onclick = () => { closeRxActionMenus(); closeModal("recentPrescriptionsModal"); };
q("viewPatientHistoryBtn").onclick = openPatientHistoryModal;
document.querySelectorAll("[data-theme-choice]").forEach(btn => {
  btn.onclick = () => {
    applyTheme(btn.dataset.themeChoice);
    closeModal("themeModal");
  };
});
q("transactionsTypeFilter").onchange = renderTransactions;
q("transactionsFromDate").onchange = renderTransactions;
q("transactionsToDate").onchange = renderTransactions;
q("transactionsTodayBtn").onclick = () => {
  const today = jordanDateKey();
  q("transactionsFromDate").value = today;
  q("transactionsToDate").value = today;
  renderTransactions();
};
q("transactionsMonthBtn").onclick = () => {
  const month = jordanNowIso().slice(0, 7);
  q("transactionsFromDate").value = `${month}-01`;
  q("transactionsToDate").value = jordanDateKey();
  renderTransactions();
};
if (q("narcoticInternalStockSearch")) q("narcoticInternalStockSearch").oninput = renderNarcoticInternalStockTable;
q("transactionsAllBtn").onclick = () => {
  q("transactionsTypeFilter").value = "";
  q("transactionsFromDate").value = "";
  q("transactionsToDate").value = "";
  q("transactionsSearch").value = "";
  renderTransactions();
};
q("printTransactionsBtn").onclick = printTransactionsPage;
q("printInventoryBtn").onclick = printInventoryPage;
if (q("printInventoryAuditBtn")) q("printInventoryAuditBtn").onclick = openInventoryAuditModal;
if (q("closeInventoryAuditModalBtn")) q("closeInventoryAuditModalBtn").onclick = () => closeModal("inventoryAuditModal");
if (q("printInventoryAuditConfirmBtn")) q("printInventoryAuditConfirmBtn").onclick = printInventoryAuditReport;
if (q("receiveNarcoticInternalShipmentBtn")) q("receiveNarcoticInternalShipmentBtn").onclick = receiveNarcoticInternalShipment;
if (q("printNarcoticInternalStockBtn")) q("printNarcoticInternalStockBtn").onclick = printNarcoticInternalStockReport;
if (q("openNarcoticConsumptionBtn")) q("openNarcoticConsumptionBtn").onclick = openNarcoticConsumptionModal;
if (q("printNarcoticConsumptionBtn")) q("printNarcoticConsumptionBtn").onclick = printNarcoticConsumptionReport;
if (q("printNarcoticTransactionsBtn")) q("printNarcoticTransactionsBtn").onclick = printNarcoticTransactionsPage;
if (q("narcoticTransactionsDrug")) q("narcoticTransactionsDrug").onchange = renderNarcoticTransactionsTable;
if (q("narcoticTransactionsType")) q("narcoticTransactionsType").onchange = renderNarcoticTransactionsTable;
if (q("narcoticTransactionsFrom")) q("narcoticTransactionsFrom").onchange = renderNarcoticTransactionsTable;
if (q("narcoticTransactionsTo")) q("narcoticTransactionsTo").onchange = renderNarcoticTransactionsTable;
if (q("narcoticTransactionsSearch")) q("narcoticTransactionsSearch").oninput = renderNarcoticTransactionsTable;
if (q("narcoticTransactionsTodayBtn")) q("narcoticTransactionsTodayBtn").onclick = () => {
  const today = jordanDateKey();
  q("narcoticTransactionsFrom").value = today;
  q("narcoticTransactionsTo").value = today;
  renderNarcoticTransactionsTable();
};
if (q("narcoticTransactionsClearBtn")) q("narcoticTransactionsClearBtn").onclick = () => {
  q("narcoticTransactionsFrom").value = "";
  q("narcoticTransactionsTo").value = "";
  renderNarcoticTransactionsTable();
};
document.addEventListener("click", event => {
  const detailBtn = event.target.closest(".transaction-details-btn");
  if (detailBtn) openTransactionDetails(detailBtn.dataset.scope || "normal", detailBtn.dataset.id);
});
q("drugCardsPrevBtn").onclick = () => { APP.drugCardsPage = Math.max(1, APP.drugCardsPage - 1); renderDashboard(); };
q("drugCardsNextBtn").onclick = () => { APP.drugCardsPage += 1; renderDashboard(); };


if (q("quickAddDoseToggle")) q("quickAddDoseToggle").onchange = () => {
  q("quickDoseFields")?.classList.toggle("hidden", !q("quickAddDoseToggle").checked);
  if (q("quickAddDoseToggle").checked && q("quickDoseStartDate") && !q("quickDoseStartDate").value) q("quickDoseStartDate").value = jordanDateKey();
};
["prescriptionsSearch","prescriptionsFromDate","prescriptionsToDate","prescriptionsDrugFilter","prescriptionsPharmacyFilter"].forEach(id => { if (q(id)) q(id).oninput = q(id).onchange = renderPrescriptions; });
if (q("prescriptionsTodayBtn")) q("prescriptionsTodayBtn").onclick = () => { const t = jordanDateKey(); q("prescriptionsFromDate").value = t; q("prescriptionsToDate").value = t; renderPrescriptions(); };
if (q("prescriptionsClearBtn")) q("prescriptionsClearBtn").onclick = () => { ["prescriptionsFromDate","prescriptionsToDate","prescriptionsSearch"].forEach(id => { if (q(id)) q(id).value = ""; }); if (q("prescriptionsDrugFilter")) q("prescriptionsDrugFilter").value = ""; if (q("prescriptionsPharmacyFilter")) q("prescriptionsPharmacyFilter").value = ""; renderPrescriptions(); };

bindAutoTitleCaseInput([
  "quickPatientName","quickDoctor","editPatientName","editDoctor",
  "narcoticPatientName","narcoticDoctorName","narcoticOrderNurseName",
  "transferReceiverPharmacist"
]);

//enableAutoFocusFlow(
 // ["quickPatient","quickFile","quickPrescriptionType","quickDrugSearch","quickDrug","quickBoxes","quickUnits","quickDoctor","quickPharmacist"],
 // () => registerQuickPrescription()
//);

themeInit();
ensureTransactionDetailsModal();
await bootstrapIfNeeded();
await tryRestoreSession();




["narcoticChartDrug","narcoticChartDepartment","narcoticDeptModalDrug","narcoticDeptModalFrom","narcoticDeptModalTo"].forEach(id => {
  if (q(id)) q(id).addEventListener("change", () => {
    if (id.startsWith("narcoticChart")) renderNarcoticDispensingChart();
    else renderNarcoticDepartmentModal();
  });
});
if (q("narcoticDepartment")) q("narcoticDepartment").addEventListener("change", () => { if (q("narcoticDrug")) q("narcoticDrug").value = ""; renderNarcoticStaticOptions(); updateNarcoticAvailableStock(); });
if (q("narcoticOrderDepartment")) q("narcoticOrderDepartment").addEventListener("change", () => { renderNarcoticStaticOptions(); });
if (q("narcoticDrugSearch")) q("narcoticDrugSearch").addEventListener("input", renderNarcoticStaticOptions);
if (q("addNarcoticDeptDrugBtn")) q("addNarcoticDeptDrugBtn").onclick = addDrugToNarcoticDepartment;
document.querySelectorAll(".narcotic-inner-tab-btn").forEach(btn => btn.addEventListener("click", () => setNarcoticDetailTab(btn.dataset.narcoticdetailtab)));

if (q("loginEmployeeNumber")) q("loginEmployeeNumber").addEventListener("keydown", e => { if (e.key === "Enter") doLogin(APP.pendingRole, q("loginEmployeeNumber")?.value, q("loginPassword").value); });
if (q("loginPassword")) q("loginPassword").addEventListener("keydown", e => { if (e.key === "Enter") doLogin(APP.pendingRole, q("loginEmployeeNumber")?.value, q("loginPassword").value); });

window.archiveOldDataNow = archiveOldDataNow;
window.refreshCrossUserSyncNow = refreshCrossUserSyncNow;
window.runAutomaticArchiveIfDue = runAutomaticArchiveIfDue;
window.loadArchiveStatus = loadArchiveStatus;

updateLayoutMode();
