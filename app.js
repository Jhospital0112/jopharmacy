
const API_URL =
  window.CDMS_CONFIG?.APPS_SCRIPT_WEB_APP_URL ||
  window.APP_CONFIG?.APPS_SCRIPT_WEB_APP_URL ||
  "";

if (!API_URL) {
  alert("Google Sheets Web App URL is missing. Open config.js and set APPS_SCRIPT_WEB_APP_URL first.");
}

const db = { kind: "gsheets" };

function cleanData(obj) {
  return Object.fromEntries(Object.entries(obj || {}).filter(([, v]) => v !== undefined));
}

function makeDocSnapshot(row) {
  return {
    exists: () => !!row,
    data: () => row ? { ...row } : undefined
  };
}

function makeQuerySnapshot(rows) {
  return {
    docs: (rows || []).map(row => ({
      id: row.id,
      data: () => ({ ...row })
    }))
  };
}

function collection(_db, table) {
  return { kind: "collection", table };
}

function where(field, op, value) {
  return { field, op, value };
}

function query(collectionRef, ...filters) {
  return { kind: "query", table: collectionRef.table, filters };
}

function doc(a, b, c) {
  if (a && a.kind === "collection") {
    return { kind: "doc", table: a.table, id: b || crypto.randomUUID() };
  }
  return { kind: "doc", table: b, id: c };
}

async function apiRequest(action, payload = {}) {
  const res = await fetch(API_URL, {
    method: "POST",
    body: JSON.stringify({ action, ...payload })
  });
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || "Google Sheets API Error");
  return data;
}

function applyFiltersToRows(rows, filters = []) {
  let result = [...(rows || [])];
  for (const f of filters) {
    if (!f) continue;
    if (f.op === "==" || f.op === "eq") {
      result = result.filter(row => String(row?.[f.field] ?? "") === String(f.value ?? ""));
    }
  }
  return result;
}

async function getDoc(docRef) {
  const { data } = await apiRequest("getDoc", { table: docRef.table, id: docRef.id });
  return makeDocSnapshot(data || null);
}

async function fetchRows(ref) {
  const { data } = await apiRequest("listDocs", { table: ref.table });
  const rows = data || [];
  if (ref.kind === "query") return applyFiltersToRows(rows, ref.filters);
  return rows;
}

function onSnapshot(ref, callback) {
  let active = true;
  let lastPayload = "";

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

  emit();
  const timer = setInterval(emit, 2000);

  return () => {
    active = false;
    clearInterval(timer);
  };
}

async function setDoc(docRef, payload, options = {}) {
  const row = cleanData({ id: docRef.id, ...payload });
  if (options && options.merge) {
    const currentSnap = await getDoc(docRef);
    const merged = { ...(currentSnap.exists() ? currentSnap.data() : {}), ...row, id: docRef.id };
    await apiRequest("setDoc", { table: docRef.table, id: docRef.id, data: merged });
    return;
  }
  await apiRequest("setDoc", { table: docRef.table, id: docRef.id, data: row });
}

async function updateDoc(docRef, payload) {
  await apiRequest("updateDoc", { table: docRef.table, id: docRef.id, data: cleanData(payload) });
}

async function addDoc(collectionRef, payload) {
  const id = crypto.randomUUID();
  await apiRequest("setDoc", { table: collectionRef.table, id, data: cleanData({ id, ...payload }) });
  return { id };
}

async function deleteDoc(docRef) {
  await apiRequest("deleteDoc", { table: docRef.table, id: docRef.id });
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
      await apiRequest("batch", {
        operations: ops.map(op => ({
          type: op.type,
          table: op.ref.table,
          id: op.ref.id,
          data: op.data
        }))
      });
    }
  };
}

const DEFAULT_PASSWORD = "111111";
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
  pharmacistEditId: null,
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
  cache: {
    drugs: [],
    inventory: [],
    prescriptions: [],
    transactions: [],
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

const q = id => document.getElementById(id);
const esc = value => String(value ?? "").replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch]));
const todayKey = () => jordanNowIso().slice(0, 10);
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
  const p = jordanDateParts();
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

function jordanDateKey() {
  return jordanNowIso().slice(0, 10);
}

function formatJordanDateTime(value, withSeconds = false) {
  if (!value) return "-";
  const s = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2})?$/.test(s)) {
    return s.replace("T", " ").slice(0, withSeconds ? 19 : 16);
  }
  const date = new Date(s);
  if (Number.isNaN(date.getTime())) return s.replace("T", " ").slice(0, withSeconds ? 19 : 16);
  const p = jordanDateParts(date);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}${withSeconds ? `:${p.second}` : ""}`;
}

function parseJordanDateTime(value) {
  if (!value) return null;
  const s = String(value).trim();
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (m) {
    const [,y,mo,d,h,mi,se='00'] = m;
    return new Date(`${y}-${mo}-${d}T${h}:${mi}:${se}+03:00`);
  }
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function canEditPrescription(rx) {
  return !!rx && rx.status !== "Returned";
}

function pharmacistWorksInScope(pharmacist, scope) {
  const pharmacies = Array.isArray(pharmacist?.pharmacies) && pharmacist.pharmacies.length ? pharmacist.pharmacies : [pharmacist?.workplace].filter(Boolean);
  return pharmacies.includes(scope);
}

function getScopePharmacists(scope, opts = {}) {
  return APP.cache.pharmacists.filter(p => p.active !== false && pharmacistWorksInScope(p, scope) && (!opts.canAuditOnly || p.canAudit));
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

function currentScopePharmacy() {
  if (APP.currentRole === "ADMIN") return APP.cache.settings.pharmacyType || "In-Patient Pharmacy";
  return APP.currentUser?.pharmacyScope?.[0] || "In-Patient Pharmacy";
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
  const scope = currentScopePharmacy();
  return APP.cache.prescriptions.filter(row => row.pharmacy === scope);
}

function transactionScopeRows() {
  const scope = currentScopePharmacy();
  return APP.cache.transactions.filter(row => row.pharmacy === scope || String(row.pharmacy || "").includes(scope));
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

  const transferFromOptions = isAdmin ? PHARMACIES : [scope];
  setSelectOptions("transferFrom", transferFromOptions, q("transferFrom")?.value || scope, !isAdmin && transferFromOptions.length === 1);
  syncTransferToOptions(q("transferTo")?.value || (isAdmin ? PHARMACIES.find(name => name !== (q("transferFrom")?.value || scope)) : PHARMACIES.find(name => name !== scope)));
}


async function bootstrapIfNeeded() {
  const marker = await getDoc(doc(db, "meta", "bootstrap"));
  if (marker.exists()) return;

  showActionModal("First Setup", "Preparing Google Sheets data...");
  const defaultHash = await sha256(DEFAULT_PASSWORD);

  for (const [id, data] of Object.entries(USERS)) {
    await setDoc(doc(db, "users", id), {
      ...data,
      id,
      passwordHash: defaultHash,
      mustChangePassword: true,
      active: true,
      createdAt: serverTimestamp(),
      assigned: true,
      updatedAt: serverTimestamp()
    });
  }

  await setDoc(doc(db, "settings", "main"), {
    pharmacyType: "In-Patient Pharmacy",
    month: MONTHS[new Date().getMonth()],
    year: new Date().getFullYear(),
    updatedAt: serverTimestamp()
  });

  await setDoc(doc(db, "pharmacists", "p1"), { name: "Noor", workplace: "In-Patient Pharmacy", pharmacies: ["In-Patient Pharmacy"], canAudit: true, active: true, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });
  await setDoc(doc(db, "pharmacists", "p2"), { name: "Ahmad", workplace: "Out-Patient Pharmacy", pharmacies: ["Out-Patient Pharmacy"], canAudit: false, active: true, createdAt: serverTimestamp(), updatedAt: serverTimestamp() });

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


  const narcoticSeedMarker = await getDoc(doc(db, "meta", "narcotic_bootstrap"));
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
    await setDoc(doc(db, "meta", "narcotic_bootstrap"), { createdAt: serverTimestamp() });
  }


  await setDoc(doc(db, "meta", "bootstrap"), { createdAt: serverTimestamp() });
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

  APP.listeners.push(onSnapshot(collection(db, "pharmacists"), snap => {
    APP.cache.pharmacists = snap.docs.map(d => ({ id: d.id, ...d.data() })).filter(p => p.active !== false);
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
    APP.cache.narcoticInternalStock = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    renderNarcoticPage();
  }));

  APP.listeners.push(onSnapshot(doc(db, "settings", "main"), snap => {
    APP.cache.settings = snap.exists() ? snap.data() : {};
    renderStaticOptions();
    renderAll();
  }));
}

function applyRoleUI() {
  const isAdmin = APP.currentRole === "ADMIN";
  const canSeeNarcotic = isAdmin || APP.currentRole === "IN_PATIENT_USER";
  document.querySelectorAll(".admin-only").forEach(el => el.classList.toggle("hidden", !isAdmin));
  document.querySelectorAll(".admin-only-block").forEach(el => el.classList.toggle("hidden", !isAdmin));
  document.querySelectorAll(".narcotic-nav-link").forEach(el => el.classList.toggle("hidden", !canSeeNarcotic));
  document.querySelectorAll(".narcotic-admin-tab").forEach(el => el.classList.toggle("hidden", !isAdmin));
  document.querySelectorAll(".narcotic-admin-tab-content").forEach(el => el.classList.toggle("hidden", !isAdmin));
  if (!isAdmin && APP.narcoticTab !== "dispensing") APP.narcoticTab = "dispensing";
  document.body.classList.toggle("allow-narcotic", canSeeNarcotic);
}

async function tryRestoreSession() {
  const role = localStorage.getItem("cdms_session_role");
  if (!role) return;
  const snap = await getDoc(doc(db, "users", role));
  if (!snap.exists()) return;
  APP.currentRole = role;
  APP.currentUser = snap.data();
  bindListeners();
  applyRoleUI();
  q("loginScreen").classList.add("hidden");
  q("appShell").classList.remove("hidden");
  showPage("dashboard");
}

function renderStaticOptions() {
  const drugOptions = APP.cache.drugs.map(d => `<option value="${esc(d.id)}">${esc(d.tradeName)} ${esc(d.strength)}</option>`).join("");
  ["quickDrug","shipmentDrug","transferDrug","reportDrug"].forEach(id => {
    if (q(id)) q(id).innerHTML = `<option value="">Select Drug</option>${drugOptions}`;
  });
  filterQuickDrugOptions();

  const scope = currentScopePharmacy();
  const pharmacists = getScopePharmacists(scope);
  q("quickPharmacist").innerHTML = `<option value="">Select Pharmacist</option>` + pharmacists.map(p => `<option value="${esc(p.name)}">${esc(p.name)}</option>`).join("");
  if (q("editPharmacist")) q("editPharmacist").innerHTML = `<option value="">Select Pharmacist</option>` + pharmacists.map(p => `<option value="${esc(p.name)}">${esc(p.name)}</option>`).join("");
  q("auditAuditor").innerHTML = `<option value="">Select Auditor</option>` + APP.cache.pharmacists.filter(p => p.canAudit && p.active !== false).map(p => `<option value="${esc(p.name)}">${esc(p.name)}</option>`).join("");
  q("doctorList").innerHTML = [...new Set(APP.cache.prescriptions.map(p => p.doctorName).filter(Boolean))].sort().map(name => `<option value="${esc(name)}"></option>`).join("");

  const pharmacyOptions = PHARMACIES.map(name => `<option>${esc(name)}</option>`).join("");
  ["shipmentLocation","transferFrom","transferTo","inventoryLocationFilter","reportPharmacy"].forEach(id => {
    if (q(id)) q(id).innerHTML = pharmacyOptions;
  });

  q("settingsPharmacy").innerHTML = WORK_PHARMACIES.map(name => `<option>${esc(name)}</option>`).join("");
  q("pharmacistPharmacyCheckboxes").innerHTML = WORK_PHARMACIES.map(name => `<label class="checkbox-item"><input type="checkbox" class="pharmacy-checkbox" value="${esc(name)}"> <span>${esc(name)}</span></label>`).join("");
  q("settingsMonth").innerHTML = MONTHS.map(name => `<option>${esc(name)}</option>`).join("");

  refreshScopedSelectors();
  updateQuickAvailableStock();
  renderNarcoticStaticOptions();
  renderLiveClocks();
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
  renderNarcoticPage();
  updateQuickAvailableStock();
  updateNarcoticAvailableStock();
  renderNarcoticStaticOptions();
  renderLiveClocks();
}

function renderControlPanel() {
  q("controlUser").textContent = APP.currentRole === "ADMIN" ? "admin@demo.local" : APP.currentUser.displayName;
  q("controlRole").textContent = APP.currentUser.role?.replaceAll("_", " ") || APP.currentRole;
  q("controlPharmacy").textContent = currentScopePharmacy();
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
  const rows = prescriptionScopeRows()
    .filter(row => !term || `${row.patientName} ${row.fileNumber} ${row.doctorName} ${row.pharmacistName}`.toLowerCase().includes(term) || `${APP.cache.drugs.find(d => d.id === row.drugId)?.tradeName || ""} ${APP.cache.drugs.find(d => d.id === row.drugId)?.strength || ""}`.toLowerCase().includes(term))
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
        <td class="rx-actions-cell">${buildPrescriptionActionsDropdown(row, { prefix: "recent" })}</td>
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

  return transactionScopeRows().filter(row => {
    const haystack = `${row.type} ${row.tradeName} ${row.pharmacy} ${row.performedBy} ${row.note}`.toLowerCase();
    const day = formatJordanDateTime(row.dateTime).slice(0, 10);
    if (term && !haystack.includes(term)) return false;
    if (type && String(row.type || "") !== type) return false;
    if (from && day < from) return false;
    if (to && day > to) return false;
    return true;
  });
}

function printTransactionsPage() {
  const rows = getFilteredTransactionsRows();
  const body = `
    <div class="section-title">Transactions</div>
    <div class="sub"><strong>Rows:</strong> ${rows.length}</div>
    <div class="section">
      <table>
        <thead><tr><th>Date & Time</th><th>Type</th><th>Drug</th><th>Boxes</th><th>Units</th><th>By</th><th>Note</th></tr></thead>
        <tbody>
          ${rows.map(row => `
            <tr>
              <td>${esc(formatJordanDateTime(row.dateTime))}</td>
              <td>${esc(row.type || "")}</td>
              <td>${esc(row.tradeName || "")}</td>
              <td>${Number(row.qtyBoxes || 0)}</td>
              <td>${Number(row.qtyUnits || 0)}</td>
              <td>${esc(row.performedBy || "")}</td>
              <td>${esc(row.note || "")}</td>
            </tr>`).join("")}
        </tbody>
      </table>
    </div>`;
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


function renderDashboard() {
  const scope = currentScopePharmacy();
  const scopedPrescriptions = prescriptionScopeRows();
  const cardSearch = (q("drugCardsSearch").value || "").toLowerCase();

  const today = jordanDateKey();
  q("metricRegistered").textContent = scopedPrescriptions.filter(p => formatJordanDateTime(p.dateTime).slice(0,10) === today).length;
  q("metricPending").textContent = scopedPrescriptions.filter(p => (p.status || "") === "Pending" && formatJordanDateTime(p.dateTime).slice(0,10) === today).length;
  q("metricReturned").textContent = scopedPrescriptions.filter(p => (p.status || "") === "Returned" && formatJordanDateTime(p.dateTime).slice(0,10) === today).length;

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
          ${p.status === "Returned" ? statusBadge("Returned") : buildPrescriptionActionsDropdown(p, { prefix: "latest" })}
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

function renderTransactions() {
  const rows = getFilteredTransactionsRows();
  q("transactionsTbody").innerHTML = rows.map(row => `
    <tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc(row.type || "")}</td>
      <td>${esc(row.tradeName || "")}</td>
      <td>${Number(row.qtyBoxes || 0)}</td>
      <td>${Number(row.qtyUnits || 0)}</td>
      <td>${esc(row.performedBy || "")}</td>
      <td>${esc(row.note || "")}</td>
    </tr>`).join("") || `<tr><td colspan="8" class="empty-state">No transactions found.</td></tr>`;
}

function renderReports() {
  refreshScopedSelectors();
}

function renderAudit() {
  if (APP.currentRole !== "ADMIN") return;
  const term = (q("auditSearch").value || "").toLowerCase();
  const scope = currentAuditPharmacy();
  const auditTableShell = q("auditTbody").closest(".table-shell");
  if (auditTableShell) auditTableShell.classList.add("audit-shell");

  const rows = scopedPrescriptionRowsByPharmacy(scope).filter(row => {
    if (APP.auditTab === "new") return (row.status || "New") === "New";
    if (APP.auditTab === "pending") return row.status === "Pending";
    return row.status === "Verified";
  }).filter(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    return `${row.patientName} ${row.fileNumber} ${row.doctorName} ${drug?.tradeName || ""} ${drug?.strength || ""}`.toLowerCase().includes(term);
  }).sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));

  q("auditTbody").innerHTML = rows.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    const noteId = `audit_note_${row.id}`;

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
      <tr>
        <td>${esc(formatJordanDateTime(row.dateTime))}</td>
        <td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td>
        <td>${esc(row.patientName || "")}</td>
        <td>${esc(row.fileNumber || "")}</td>
        <td>${Number(row.qtyBoxes || 0)}</td>
        <td>${Number(row.qtyUnits || 0)}</td>
        <td>${esc(row.doctorName || "")}</td>
        <td><input id="${noteId}" class="audit-note" value="${esc(row.auditNote || "")}"></td>
        <td>${actionCell}</td>
      </tr>`;
  }).join("") || `<tr><td colspan="9" class="empty-state">No prescriptions found in this audit tab.</td></tr>`;
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
  closeModal("adjustStockModal");
  finishActionModal(true, "Available stock updated successfully.");
}

function openConfirmActionModal(type, id) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx) return;
  APP.confirmAction = { type, id };
  const scope = rx.pharmacy;
  const list = getScopePharmacists(scope, { canAuditOnly: APP.currentRole === "ADMIN" }).map(p => p.name);
  q("confirmActionTitle").textContent = type === "delete" ? "Delete Prescription" : "Return Prescription";
  q("confirmActionText").textContent = type === "delete" ? "Please confirm deleting this prescription." : "Please confirm returning this prescription.";
  q("confirmActionPharmacist").innerHTML = `<option value="">Select Pharmacist</option>` + list.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
  if (q("confirmActionPharmacist").parentElement) q("confirmActionPharmacist").parentElement.classList.remove("hidden");
  openModal("confirmActionModal");
}

async function submitConfirmedAction() {
  if (!APP.confirmAction) return closeModal("confirmActionModal");
  const action = APP.confirmAction;
  const pharmacistName = q("confirmActionPharmacist")?.value || "";
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
  batch.update(doc(db, "narcotic_prescriptions", id), { status: "Returned", returnPharmacist: pharmacistName, updatedAt: serverTimestamp(), updatedBy: APP.currentRole });
  if (stockRow?.id) {
    batch.update(doc(db, "narcotic_department_stock", stockRow.id), {
      availableStockUnits: Number(stockRow.availableStockUnits || 0) + Number(row.dispensedUnits || 0),
      updatedAt: serverTimestamp()
    });
  }
  showActionModal("Return Narcotic Prescription", "Please wait while the prescription is being returned...");
  await batch.commit();
  finishActionModal(true, "Prescription returned successfully.");
}

async function performDeleteNarcoticPrescription(id, pharmacistName) {
  const row = narcoticPrescriptionById(id);
  if (!row) return;
  const stockRow = narcoticDeptStockRow(row.departmentId, row.drugId);
  const batch = writeBatch(db);
  batch.set(doc(collection(db, "narcotic_order_movements")), {
    dateTime: jordanNowIso(),
    type: "Delete Prescription",
    departmentId: row.departmentId,
    departmentName: row.departmentName || narcoticDeptById(row.departmentId)?.departmentName || "",
    drugId: row.drugId,
    drugName: row.drugName || narcoticDrugById(row.drugId)?.tradeName || "",
    quantitySent: Number(row.dispensedUnits || 0),
    performedBy: pharmacistName,
    notes: `Prescription deleted for ${row.patientName || ""}`
  });
  batch.delete(doc(db, "narcotic_prescriptions", id));
  if (stockRow?.id) {
    batch.update(doc(db, "narcotic_department_stock", stockRow.id), {
      availableStockUnits: Number(stockRow.availableStockUnits || 0) + Number(row.dispensedUnits || 0),
      updatedAt: serverTimestamp()
    });
  }
  showActionModal("Delete Narcotic Prescription", "Please wait while the prescription is being deleted...");
  await batch.commit();
  finishActionModal(true, "Prescription deleted successfully.");
}

async function performDeletePrescription(id, pharmacistName) {
  if (APP.currentRole !== "ADMIN") return;
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx) return;
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
    type: "Delete Prescription", drugId: rx.drugId, tradeName: drug?.tradeName || "", pharmacy: rx.pharmacy, qtyBoxes: rx.qtyBoxes || 0, qtyUnits: rx.qtyUnits || 0, performedBy: pharmacistName, note: `Prescription deleted for ${rx.patientName || ""}`, dateTime: jordanNowIso()
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
  batch.update(doc(db, "prescriptions", id), { status: "Returned", returnBy: pharmacistName, returnDateTime: jordanNowIso(), updatedBy: APP.currentRole });
  batch.update(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
  batch.set(doc(collection(db, "transactions")), { type: "Return", drugId: rx.drugId, tradeName: drug.tradeName, pharmacy: rx.pharmacy, qtyBoxes: Number(rx.qtyBoxes || 0), qtyUnits: Number(rx.qtyUnits || 0), performedBy: pharmacistName, note: `Returned prescription for ${rx.patientName}`, dateTime: jordanNowIso() });
  await batch.commit();
  finishActionModal(true, "Prescription returned successfully.");
}

function renderSettings() {
  if (APP.currentRole !== "ADMIN") return;
  q("settingsPharmacy").value = APP.cache.settings.pharmacyType || "In-Patient Pharmacy";
  q("settingsMonth").value = APP.cache.settings.month || MONTHS[new Date().getMonth()];
  q("settingsYear").value = APP.cache.settings.year || new Date().getFullYear();
  q("savePharmacistBtn").textContent = APP.pharmacistEditId ? "Save Changes" : "Add Pharmacist";
  if (q("pharmacistCanNarcotic")) q("pharmacistCanNarcotic").value = q("pharmacistCanNarcotic").value || "false";
  q("cancelPharmacistEditBtn").classList.toggle("hidden", !APP.pharmacistEditId);
  q("pharmacistFormMode").textContent = APP.pharmacistEditId ? "Edit pharmacist" : "Add new pharmacist";
  q("pharmacistsTbody").innerHTML = APP.cache.pharmacists.map(p => {
    const pharmacies = Array.isArray(p.pharmacies) && p.pharmacies.length ? p.pharmacies : [p.workplace].filter(Boolean);
    return `
    <tr>
      <td>${esc(p.name)}</td>
      <td>${esc(p.jobNumber || "-")}</td>
      <td>${esc(pharmacies.join(", "))}</td>
      <td>${p.canAudit ? "Yes" : "No"}</td>
      <td>${p.canManageNarcotic ? "Yes" : "No"}</td>
      <td>${p.active !== false ? "Yes" : "No"}</td>
      <td>
        <div class="button-inline-group">
          <button class="soft-btn mini-btn edit-pharmacist-btn" data-id="${p.id}">Edit</button>
          <button class="soft-btn mini-btn delete-pharmacist-btn" data-id="${p.id}">Delete</button>
        </div>
      </td>
    </tr>`;
  }).join("") || `<tr><td colspan="7" class="empty-state">No pharmacists found.</td></tr>`;
}


function showPage(page) {
  document.querySelectorAll(".page-block").forEach(block => block.classList.add("hidden"));
  q(`page-${page}`).classList.remove("hidden");
  document.querySelectorAll(".nav-link[data-page]").forEach(btn => btn.classList.toggle("active", btn.dataset.page === page));
}

async function doLogin(role, password) {
  showActionModal("Signing In", "Please wait while the system signs you in...");
  const snap = await getDoc(doc(db, "users", role));
  if (!snap.exists()) return finishActionModal(false, "User not found.");
  const user = snap.data();
  const ok = await sha256(password) === user.passwordHash;
  if (!ok) return finishActionModal(false, "Invalid password.");

  APP.currentRole = role;
  APP.currentUser = user;
  localStorage.setItem("cdms_session_role", role);
  bindListeners();
  applyRoleUI();
  q("loginScreen").classList.add("hidden");
  q("appShell").classList.remove("hidden");
  showPage("dashboard");

  if (user.mustChangePassword) {
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
  if (!history.length) {
    return {
      exactDrugDuplicates: [],
      smartDuplicates: [],
      allHistory: []
    };
  }

  const exactDrugDuplicates = history.filter(row => row.drugId === payload.drugId);
  const smartDuplicates = history.filter(row => row.drugId !== payload.drugId);

  return {
    exactDrugDuplicates,
    smartDuplicates,
    allHistory: history
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

  let msg = "";
  if (hasExact && hasSmart) {
    msg = "This file number has repeated prescriptions within the last 30 days, including the same drug and other drugs.";
  } else if (hasExact) {
    msg = "This file number has repeated prescriptions for the same drug within the last 30 days.";
  } else if (hasSmart) {
    msg = "This file number already has other prescriptions within the last 30 days, even if the drug is different.";
  } else {
    msg = "This file number has previous prescriptions within the last 30 days.";
  }

  q("duplicateWarningText").textContent = msg;

  const exactRows = duplicateResult.exactDrugDuplicates || [];
  const smartRows = duplicateResult.smartDuplicates || [];
  const allRows = [...exactRows, ...smartRows]
    .sort((a, b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));

  q("duplicateWarningTbody").innerHTML = allRows.length
    ? renderDuplicateRows(allRows)
    : `<tr><td colspan="5" class="empty-state">No duplicate prescriptions found.</td></tr>`;

  if (q("viewPatientHistoryBtn")) {
    q("viewPatientHistoryBtn").classList.toggle("hidden", !(duplicateResult.allHistory || []).length);
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

  showActionModal("Register Prescription", "Please wait while the prescription is being registered...");
  try {
    const prescriptionRef = doc(collection(db, "prescriptions"));
    await setDoc(prescriptionRef, {
      drugId, pharmacy, patientName, fileNumber, doctorName, pharmacistName, prescriptionType: prescriptionType || "", qtyBoxes, qtyUnits, status: "New", auditBy: "", auditDateTime: null, auditNote: "", returnBy: "", returnDateTime: null, returnNote: "", dateTime: jordanNowIso(), createdBy: APP.currentRole, updatedBy: APP.currentRole
    });
    const txRef = doc(collection(db, "transactions"));
    await setDoc(txRef, { type: "Dispense", drugId, tradeName: drug.tradeName, pharmacy, qtyBoxes, qtyUnits, performedBy: pharmacistName, note: `Prescription: ${patientName}`, dateTime: jordanNowIso() });
    if (!inv?.id) throw new Error("Inventory record was not found for the selected drug and pharmacy.");
    await updateDoc(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
    ["quickPatientName","quickPatientFile","quickDoctor"].forEach(id => q(id).value = "");
    if (q("quickPrescriptionType")) q("quickPrescriptionType").value = "";
    ["quickBoxes","quickUnits"].forEach(id => q(id).value = "0");
    APP.pendingQuickPayload = null;
    updateQuickAvailableStock();
    finishActionModal(true, "Prescription registered successfully.");
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
    patientName: q("quickPatientName").value.trim(),
    fileNumber: q("quickPatientFile").value.trim(),
    doctorName: q("quickDoctor").value.trim(),
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
  if (payload.fileNumber !== "999444" && (duplicateResult.exactDrugDuplicates.length || duplicateResult.smartDuplicates.length)) {
    openDuplicateWarningModal(payload, duplicateResult);
    return;
  }
  await continueQuickRegistration(payload);
}

async function auditPrescription(id, status, note) {
  const auditor = q("auditAuditor").value;
  if (!auditor) {
    showActionModal("Audit", "Please select an auditor first.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const rx = APP.cache.prescriptions.find(p => p.id === id);
  if (!rx) return;

  showActionModal("Audit Update", "Please wait while the prescription is being updated...");
  await updateDoc(doc(db, "prescriptions", id), {
    status,
    auditBy: auditor,
    auditDateTime: jordanNowIso(),
    auditNote: note || "",
    updatedBy: "ADMIN"
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
  await setDoc(doc(db, "settings", "main"), nextSettings, { merge: true });
  APP.cache.settings = { ...(APP.cache.settings || {}), ...nextSettings };
  renderAll();
  showPage("dashboard");
  finishActionModal(true, "Settings saved successfully.");
}

function selectedPharmacistPharmacies() {
  return [...document.querySelectorAll('.pharmacy-checkbox:checked')].map(el => el.value);
}

function resetPharmacistForm() {
  APP.pharmacistEditId = null;
  q("pharmacistName").value = "";
  q("pharmacistJobNumber").value = "";
  q("pharmacistCanAudit").value = "false";
  if (q("pharmacistCanNarcotic")) q("pharmacistCanNarcotic").value = "false";
  document.querySelectorAll('.pharmacy-checkbox').forEach(el => el.checked = false);
  renderSettings();
}

function startEditPharmacist(id) {
  const pharmacist = APP.cache.pharmacists.find(p => p.id === id);
  if (!pharmacist) return;
  APP.pharmacistEditId = id;
  q("pharmacistName").value = pharmacist.name || "";
  q("pharmacistJobNumber").value = pharmacist.jobNumber || "";
  q("pharmacistCanAudit").value = pharmacist.canAudit ? "true" : "false";
  if (q("pharmacistCanNarcotic")) q("pharmacistCanNarcotic").value = pharmacist.canManageNarcotic ? "true" : "false";
  const pharmacies = Array.isArray(pharmacist.pharmacies) && pharmacist.pharmacies.length ? pharmacist.pharmacies : [pharmacist.workplace].filter(Boolean);
  document.querySelectorAll('.pharmacy-checkbox').forEach(el => el.checked = pharmacies.includes(el.value));
  renderSettings();
}


let savingPharmacistNow = false;

async function savePharmacist() {
  if (savingPharmacistNow) return;

  const name = q("pharmacistName").value.trim();
  const jobNumber = q("pharmacistJobNumber").value.trim();
  const pharmacies = selectedPharmacistPharmacies();

  if (!name || !jobNumber || !pharmacies.length) {
    showActionModal("Validation", "Please enter pharmacist name, job number, and select at least one pharmacy.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  const duplicateInCache = APP.cache.pharmacists.find(
    p => p.active !== false && String(p.jobNumber).trim() === jobNumber && p.id !== APP.pharmacistEditId
  );
  if (duplicateInCache) {
    showActionModal("Validation", "This job number already exists. Pharmacist was not saved.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }

  savingPharmacistNow = true;
  q("savePharmacistBtn").disabled = true;

  try {
    showActionModal(
      APP.pharmacistEditId ? "Update Pharmacist" : "Save Pharmacist",
      "Please wait while the pharmacist record is being saved..."
    );

    const liveRows = await fetchRows(collection(db, "pharmacists"));
    const duplicateLive = liveRows.find(
      p => p.active !== false && String(p.jobNumber || "").trim() === jobNumber && p.id !== APP.pharmacistEditId
    );

    if (duplicateLive) {
      showActionModal("Validation", "This job number already exists. Pharmacist was not saved.", false);
      q("actionOkBtn").classList.remove("hidden");
      return;
    }

    const payload = {
      name,
      jobNumber,
      workplace: pharmacies[0],
      pharmacies,
      canAudit: q("pharmacistCanAudit").value === "true",
      canManageNarcotic: q("pharmacistCanNarcotic")?.value === "true",
      active: true,
      updatedAt: serverTimestamp()
    };

    if (APP.pharmacistEditId) {
      await updateDoc(doc(db, "pharmacists", APP.pharmacistEditId), payload);
    } else {
      await addDoc(collection(db, "pharmacists"), {
        ...payload,
        createdAt: serverTimestamp()
      });
    }

    const wasEditing = !!APP.pharmacistEditId;
    resetPharmacistForm();
    finishActionModal(true, wasEditing ? "Pharmacist updated successfully." : "Pharmacist saved successfully.");
  } catch (error) {
    console.error("Save Pharmacist Error:", error);
    showActionModal("Save Pharmacist Error", error?.message || "Unexpected error while saving pharmacist.", false);
    q("actionOkBtn").classList.remove("hidden");
  } finally {
    savingPharmacistNow = false;
    q("savePharmacistBtn").disabled = false;
  }
}

async function deletePharmacist(id) {
  if (APP.currentRole !== "ADMIN") return;
  const pharmacist = APP.cache.pharmacists.find(p => p.id === id);
  if (!pharmacist) return;
  showActionModal("Delete Pharmacist", "Please wait while the pharmacist is being deleted...");
  await updateDoc(doc(db, "pharmacists", id), { active: false, updatedAt: serverTimestamp() });
  finishActionModal(true, `Pharmacist ${pharmacist.name} deleted successfully.`);
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
  await updateDoc(doc(db, "users", APP.currentRole), { passwordHash: hash, mustChangePassword: false, updatedAt: serverTimestamp() });
  APP.currentUser.passwordHash = hash;
  APP.currentUser.mustChangePassword = false;
  closeModal("changePasswordModal");
  finishActionModal(true, "Password changed successfully.");
}

async function openResetPasswordModal() {
  if (APP.currentRole !== "ADMIN") return;
  q("resetPasswordRole").innerHTML = `
    <option value="IN_PATIENT_USER">In-Patient Pharmacy</option>
    <option value="OUT_PATIENT_USER">Out-Patient Pharmacy</option>
    <option value="MEDICAL_CENTER_USER">Medical Center Pharmacy</option>
  `;
  q("resetPasswordRole").value = "IN_PATIENT_USER";
  q("resetPasswordHint").textContent = "Selected pharmacy password will be reset to 111111";
  openModal("resetPasswordModal");
}

async function resetSelectedPassword() {
  if (APP.currentRole !== "ADMIN") return;
  const role = q("resetPasswordRole").value;
  if (!role || !USERS[role]) return;
  showActionModal("Reset Password", "Please wait while the password is being reset...");
  const hash = await sha256(DEFAULT_PASSWORD);
  await updateDoc(doc(db, "users", role), { passwordHash: hash, mustChangePassword: false, updatedAt: serverTimestamp() });
  closeModal("resetPasswordModal");
  finishActionModal(true, `${USERS[role].displayName} password has been reset to 111111`);
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
  [q("saveDrugInfoBtn"), q("deleteDrugBtn")].forEach(el => el.classList.toggle("hidden", APP.currentRole !== "ADMIN"));
  renderDrugRows();
  openModal("drugModal");
}


function renderDrugRows() {
  if (!APP.selectedDrugId) return;
  q("drugRxTbody").innerHTML = prescriptionScopeRows().filter(row => row.drugId === APP.selectedDrugId).map(row => {
    return `
    <tr>
      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
      <td>${esc(row.patientName || "")}</td>
      <td>${esc(row.fileNumber || "")}</td>
      <td>${Number(row.qtyBoxes || 0)}</td>
      <td>${Number(row.qtyUnits || 0)}</td>
      <td>${esc(row.doctorName || "")}</td>
      <td>${esc(row.pharmacistName || "")}</td>
      <td>${esc(row.status || "")}</td>
      <td class="rx-actions-cell">${buildPrescriptionActionsDropdown(row, { prefix: "drug" })}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="9" class="empty-state">No prescriptions for this drug.</td></tr>`;
  renderLiveClocks();
}

function openEditPrescription(id) {
  const rx = APP.cache.prescriptions.find(row => row.id === id);
  if (!rx) return;
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
  q("editPharmacist").value = rx.pharmacistName || "";
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
  if (!rx) return;
  const patientName = q("editPatientName").value.trim();
  const fileNumber = q("editPatientFile").value.trim();
  const doctorName = q("editDoctor").value.trim();
  const pharmacistName = q("editPharmacist").value.trim();
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
    performedBy: APP.currentRole,
    note: `Prescription edited for ${patientName}`,
    dateTime: jordanNowIso()
  });
  await batch.commit();
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
  closeModal("addDrugModal");
  finishActionModal(true, "Drug added successfully.");
}

function buildPrescriptionActionsDropdown(row, { prefix = "rx" } = {}) {
  if (!row || row.status === "Returned") return `<span class="rx-returned-label">Returned</span>`;
  const allowed = canEditPrescription(row);
  const menuId = `rx_actions_${prefix}_${row.id}`;
  return `
    <div class="rx-actions-wrap">
      <button class="soft-btn mini-btn rx-actions-btn" data-rx-actions-toggle="${menuId}">Actions ▾</button>
      <div class="rx-actions-menu hidden" id="${menuId}">
        <button class="soft-btn mini-btn latest-return-btn" data-id="${row.id}">Return</button>
        <button class="primary-btn mini-btn latest-edit-btn ${allowed ? "" : "hidden"}" data-id="${row.id}">Edit</button>
        ${APP.currentRole === "ADMIN" ? `<button class="mini-danger-btn mini-btn delete-rx-btn" data-id="${row.id}">Delete</button>` : ""}
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
      <td><button class="soft-btn mini-btn" data-remove-shipment-row="${index}">Remove</button></td>
    </tr>`).join("") || `<tr><td colspan="5" class="empty-state">No shipment rows added yet.</td></tr>`;
}

function renderTransferBatchTable() {
  const rows = APP.transferBatchRows || [];
  q("transferBatchTbody").innerHTML = rows.map((row, index) => `
    <tr>
      <td>${esc(row.tradeLabel || "")}</td>
      <td>${esc(row.from || "")}</td>
      <td>${esc(row.to || "")}</td>
      <td>${Number(row.boxes || 0)}</td>
      <td>${Number(row.units || 0)}</td>
      <td><button class="soft-btn mini-btn" data-remove-transfer-row="${index}">Remove</button></td>
    </tr>`).join("") || `<tr><td colspan="6" class="empty-state">No transfer rows added yet.</td></tr>`;
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
  APP.shipmentBatchRows.push({ drugId, pharmacy, boxes, units, tradeLabel: `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim() });
  renderShipmentBatchTable();
  clearShipmentInputs();
  return true;
}

function addTransferBatchRow() {
  const drugId = q("transferDrug").value;
  const boxes = Number(q("transferBoxes").value || 0);
  const units = Number(q("transferUnits").value || 0);
  const from = q("transferFrom").value;
  const to = q("transferTo").value;
  if (!drugId) {
    showActionModal("Transfer Stock", "Please select a drug.", false);
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
  APP.transferBatchRows.push({ drugId, from, to, boxes, units, tradeLabel: `${drug?.tradeName || ""} ${drug?.strength || ""}`.trim() });
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

    for (const item of batchRows) {
      const { drugId, pharmacy, boxes, units } = item;
      const drug = APP.cache.drugs.find(d => d.id === drugId);
      if (!drug) continue;
      const unitsPerBox = Number(drug.unitsPerBox || 1);
      let inv = inventoryMap.get(`${drugId}__${String(pharmacy).replace(/\s+/g, "_")}`) || invRow(drugId, pharmacy);
      if (!inv) {
        inv = { id: `${drugId}__${String(pharmacy).replace(/\s+/g, "_")}`, drugId, pharmacy, boxes: 0, units: 0, totalUnits: 0 };
        operations.push({ type: "set", table: "inventory", id: inv.id, data: { ...inv, updatedAt: jordanNowIso() } });
      }
      const updated = normalizeInventory(Number(inv.boxes || 0) + Number(boxes || 0), Number(inv.units || 0) + Number(units || 0), unitsPerBox);
      operations.push({ type: "update", table: "inventory", id: inv.id, data: { boxes: updated.boxes, units: updated.units, totalUnits: updated.totalUnits, updatedAt: jordanNowIso() } });
      operations.push({ type: "set", table: "transactions", id: crypto.randomUUID(), data: { type: "Receive Shipment", drugId, tradeName: drug.tradeName || "", pharmacy, qtyBoxes: Number(boxes || 0), qtyUnits: Number(units || 0), performedBy: APP.currentRole || "", note: "Shipment received", dateTime: jordanNowIso() } });
      inventoryMap.set(inv.id, { ...inv, ...updated, updatedAt: jordanNowIso() });
    }

    await apiRequest("batch", { operations });
    APP.cache.inventory = [...inventoryMap.values()];
    APP.shipmentBatchRows = [];
    renderShipmentBatchTable();
    clearShipmentInputs();
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
  if (!(APP.transferBatchRows || []).length && !addTransferBatchRow()) return;
  const batchRows = [...(APP.transferBatchRows || [])];
  if (!batchRows.length) return;

  showActionModal("Transfer Stock", "Please wait while the stock is being transferred...");

  try {
    const operations = [];
    const inventoryMap = new Map((APP.cache.inventory || []).map(row => [row.id, { ...row }]));

    for (const item of batchRows) {
      const { drugId, boxes, units, from, to } = item;
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
      operations.push({ type: "update", table: "inventory", id: fromInv.id, data: { boxes: updatedFrom.boxes, units: updatedFrom.units, totalUnits: updatedFrom.totalUnits, updatedAt: jordanNowIso() } });
      operations.push({ type: "update", table: "inventory", id: toInv.id, data: { boxes: updatedTo.boxes, units: updatedTo.units, totalUnits: updatedTo.totalUnits, updatedAt: jordanNowIso() } });
      operations.push({ type: "set", table: "transactions", id: crypto.randomUUID(), data: { type: "Transfer", drugId, tradeName: drug.tradeName || "", pharmacy: `${from} → ${to}`, qtyBoxes: Number(boxes || 0), qtyUnits: Number(units || 0), performedBy: APP.currentRole || "", note: "Stock transfer", dateTime: jordanNowIso() } });
      inventoryMap.set(fromInv.id, { ...fromInv, ...updatedFrom, updatedAt: jordanNowIso() });
      inventoryMap.set(toInv.id, { ...toInv, ...updatedTo, updatedAt: jordanNowIso() });
    }

    await apiRequest("batch", { operations });
    APP.cache.inventory = [...inventoryMap.values()];
    APP.transferBatchRows = [];
    renderTransferBatchTable();
    clearTransferInputs();
    closeModal("transferModal");
    finishActionModal(true, "Stock transferred successfully.");
    renderAll();
  } catch (error) {
    console.error("Transfer Stock Error:", error);
    showActionModal("Transfer Stock Error", error?.message || "Failed to transfer stock.", false);
    q("actionOkBtn").classList.remove("hidden");
  }
}


function buildPrintShell(title, subtitle, bodyHtml) {
  return `
  <html>
    <head>
      <title>${esc(title)}</title>
      <style>
        body{font-family:Inter,Arial,sans-serif;background:#f3f6fb;color:#20344a;margin:0;padding:28px}
        .report{background:#fff;border:1px solid #d8e0eb;border-radius:20px;padding:28px;box-shadow:0 10px 24px rgba(0,0,0,.05)}
        .head{display:flex;justify-content:space-between;align-items:flex-start;border-bottom:2px solid #173a66;padding-bottom:16px;margin-bottom:20px}
        .title{font-size:24px;font-weight:900;color:#173a66}
        .sub{margin-top:8px;color:#5f7085;font-size:13px}
        .section{margin-top:18px}
        .section-title{font-size:16px;font-weight:800;color:#173a66;margin:18px 0 10px}
        table{width:100%;border-collapse:collapse}
        th,td{padding:10px 12px;border:1px solid #d8e0eb;font-size:12px;text-align:left}
        th{background:#eef4fb;color:#173a66}
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
          <div class="pill">Printed ${esc(formatJordanDateTime(jordanNowIso()))}</div>
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
  const subtitleLabel = pharmacy === "ALL_WORK_PHARMACIES" ? "All Pharmacies" : pharmacy;
  const rows = scopedPrescriptionRowsByPharmacy(pharmacy)
    .filter(row => (!from || formatJordanDateTime(row.dateTime).slice(0, 10) >= from) && (!to || formatJordanDateTime(row.dateTime).slice(0, 10) <= to))
    .sort((a, b) => String(a.dateTime || "").localeCompare(String(b.dateTime || "")));

  const grouped = sortDrugsAlphabetically(APP.cache.drugs).map(drug => ({
    drug,
    rows: rows.filter(r => r.drugId === drug.id)
  })).filter(group => group.rows.length);

  const body = grouped.map((group, idx) => {
    const totalBoxes = group.rows.reduce((s, r) => s + Number(r.qtyBoxes || 0), 0);
    const totalUnits = group.rows.reduce((s, r) => s + Number(r.qtyUnits || 0), 0);
    return `
      <div class="group ${idx ? 'page-break' : ''}">
        <div class="section-title">${esc(group.drug.tradeName || "")} ${esc(group.drug.strength || "")}</div>
        <div class="sub"><strong>Pharmacy:</strong> ${esc(subtitleLabel)} &nbsp; | &nbsp; <strong>Date Range:</strong> ${esc((from || '-') + ' to ' + (to || '-'))}</div>
        <table>
          <thead><tr><th>Date & Time</th><th>Patient</th><th>File No.</th><th>Doctor</th><th>Pharmacist</th><th>Boxes</th><th>Units</th><th>Status</th></tr></thead>
          <tbody>
            ${group.rows.map(row => `
              <tr>
                <td>${esc(formatJordanDateTime(row.dateTime))}</td>
                <td>${esc(row.patientName || "")}</td>
                <td>${esc(row.fileNumber || "")}</td>
                <td>${esc(row.doctorName || "")}</td>
                <td>${esc(row.pharmacistName || "")}</td>
                <td>${Number(row.qtyBoxes || 0)}</td>
                <td>${Number(row.qtyUnits || 0)}</td>
                <td>${esc(row.status || "")}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
        <div class="section" style="margin-top:14px">
          <table>
            <thead><tr><th>Total Boxes</th><th>Total Units</th><th>Total Prescriptions</th></tr></thead>
            <tbody><tr><td>${totalBoxes}</td><td>${totalUnits}</td><td>${group.rows.length}</td></tr></tbody>
          </table>
        </div>
      </div>`;
  }).join("") || `<div class="section-title">No prescriptions found for the selected pharmacy and date range.</div>`;

  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Comprehensive Report", `${subtitleLabel}${from || to ? ` · ${from || ""} to ${to || ""}` : ""}`, body));
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

  const inpt = inpatientPharmacists().map(p => `<option value="${esc(p.name)}">${esc(p.name)}</option>`).join("");
  const narcManagers = narcoticManagerPharmacists().map(p => `<option value="${esc(p.name)}">${esc(p.name)}</option>`).join("");
  if (q("narcoticPharmacist")) q("narcoticPharmacist").innerHTML = `<option value="">Select Pharmacist</option>${inpt}`;
  if (q("narcoticOrderPharmacist")) q("narcoticOrderPharmacist").innerHTML = `<option value="">Select Pharmacist</option>${narcManagers}`;
  if (q("narcoticInternalPharmacist")) q("narcoticInternalPharmacist").innerHTML = `<option value="">Select Pharmacist</option>${narcManagers}`;
  if (q("narcoticInternalDrug")) {
    const prev = q("narcoticInternalDrug")?.value || "";
    q("narcoticInternalDrug").innerHTML = `<option value="">Select Drug</option>${makeDrugOptions(allActiveDrugs)}`;
    if ([...q("narcoticInternalDrug").options].some(opt => opt.value === prev)) q("narcoticInternalDrug").value = prev;
  }
}

function narcoticDeptById(id) {
  return APP.cache.narcoticDepartments.find(d => d.id === id);
}
function narcoticDrugById(id) {
  return APP.cache.narcoticDrugs.find(d => d.id === id);
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
  return inpatientPharmacists().map(p => p.name).filter(Boolean);
}

function openNarcoticConfirmActionModal(type, id) {
  const rx = narcoticPrescriptionById(id);
  if (!rx) return;
  APP.confirmAction = { scope: "narcotic", type, id };
  const names = narcoticConfirmPharmacists();
  q("confirmActionTitle").textContent = type === "delete" ? "Delete Narcotic Prescription" : "Return Narcotic Prescription";
  q("confirmActionText").textContent = type === "delete"
    ? "Delete this prescription permanently and restore the stock to the same department?"
    : "Return this prescription and restore the stock to the same department?";
  if (q("confirmActionPharmacist")) {
    q("confirmActionPharmacist").innerHTML = `<option value="">Select Pharmacist</option>` + names.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
    q("confirmActionPharmacist").value = "";
  }
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
  if (q("narcoticPharmacist")) q("narcoticPharmacist").selectedIndex = 0;
  updateNarcoticAvailableStock();
}
function narcoticActionMenu(row) {
  if (!row) return `<span class="muted">-</span>`;
  if (row.status === "Returned") return `<span class="rx-returned-label">Returned</span>`;
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
    const total = APP.cache.narcoticPrescriptions.filter(r => {
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

function narcoticInternalStockRow(drugId) {
  return APP.cache.narcoticInternalStock.find(r => String(r.drugId) === String(drugId));
}
async function upsertNarcoticInternalStock(drugId, nextQty, reorderLevel) {
  const drug = narcoticDrugById(drugId);
  const row = narcoticInternalStockRow(drugId);
  const payload = {
    id: row?.id || `internal__${drugId}`,
    drugId,
    drugName: narcoticDrugLabel(drug),
    availableStockUnits: Math.max(0, Number(nextQty || 0)),
    reorderLevelUnits: Math.max(0, Number(reorderLevel || row?.reorderLevelUnits || 0)),
    updatedAt: serverTimestamp()
  };
  await setDoc(doc(db, "narcotic_internal_stock", payload.id), payload, { merge: true });
}
function renderNarcoticInternalStock() {
  if (!q("narcoticInternalStockTbody")) return;
  const term = (q("narcoticInternalSearch")?.value || "").toLowerCase().trim();
  const rows = APP.cache.narcoticDrugs.filter(d => d.active !== false).filter(drug => !term || narcoticDrugLabel(drug).toLowerCase().includes(term) || String(drug.scientificName || "").toLowerCase().includes(term));
  q("narcoticInternalStockTbody").innerHTML = rows.map(drug => {
    const row = narcoticInternalStockRow(drug.id) || {};
    const available = Number(row.availableStockUnits || 0);
    const reorder = Number(row.reorderLevelUnits || drug.reorderLevelUnits || 0);
    const status = available <= reorder ? '<span class="badge pending">Below Reorder</span>' : '<span class="badge verified">OK</span>';
    return `<tr>
      <td>${esc(narcoticDrugLabel(drug))}</td>
      <td>${available}</td>
      <td>${reorder}</td>
      <td>${status}</td>
      <td><input id="narcoticManualQty_${drug.id}" type="number" min="0" value="${available}" class="table-number-input"></td>
      <td><button class="soft-btn mini-btn" data-save-narcotic-manual-stock="${drug.id}">Save</button></td>
    </tr>`;
  }).join("") || `<tr><td colspan="6" class="empty-state">No narcotic drugs found.</td></tr>`;
}
async function receiveNarcoticInternalShipment() {
  const drugId = q("narcoticInternalDrug")?.value || "";
  const qty = Math.max(0, Number(q("narcoticInternalQty")?.value || 0));
  const pharmacist = q("narcoticInternalPharmacist")?.value || "";
  if (!drugId || qty <= 0 || !pharmacist) {
    showActionModal("Receive Narcotic Shipment", "Please select drug, quantity and pharmacist.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const row = narcoticInternalStockRow(drugId) || {};
  const nextQty = Number(row.availableStockUnits || 0) + qty;
  showActionModal("Receive Narcotic Shipment", "Please wait while narcotic stock is being updated...");
  await upsertNarcoticInternalStock(drugId, nextQty, row.reorderLevelUnits || narcoticDrugById(drugId)?.reorderLevelUnits || 0);
  await addDoc(collection(db, "transactions"), {
    type: "Receive Narcotic Shipment",
    pharmacy: "In-Patient Pharmacy",
    drugId,
    tradeName: narcoticDrugLabel(narcoticDrugById(drugId)),
    qtyBoxes: 0,
    qtyUnits: qty,
    performedBy: pharmacist,
    note: q("narcoticInternalNotes")?.value || "",
    invoiceDate: q("narcoticInternalInvoiceDate")?.value || "",
    invoiceNo: q("narcoticInternalInvoiceNo")?.value || "",
    dateTime: jordanNowIso()
  });
  ["narcoticInternalQty","narcoticInternalInvoiceDate","narcoticInternalInvoiceNo","narcoticInternalNotes"].forEach(id => { if (q(id)) q(id).value = ""; });
  if (q("narcoticInternalDrug")) q("narcoticInternalDrug").selectedIndex = 0;
  if (q("narcoticInternalPharmacist")) q("narcoticInternalPharmacist").selectedIndex = 0;
  finishActionModal(true, "Narcotic shipment received successfully.");
}
async function saveNarcoticManualStock(drugId) {
  if (APP.currentRole !== "ADMIN") return;
  const input = q(`narcoticManualQty_${drugId}`);
  if (!input) return;
  const qty = Math.max(0, Number(input.value || 0));
  const row = narcoticInternalStockRow(drugId) || {};
  showActionModal("Manual Narcotic Stock", "Saving inpatient narcotic stock...");
  await upsertNarcoticInternalStock(drugId, qty, row.reorderLevelUnits || narcoticDrugById(drugId)?.reorderLevelUnits || 0);
  finishActionModal(true, "Inpatient narcotic stock updated.");
}

function renderNarcoticRecent() {
  if (!q("narcoticRecentTbody")) return;
  const rows = APP.cache.narcoticPrescriptions.slice(0,7);
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
      <td>${narcoticActionMenu(row)}</td>
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
  return;
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
  renderNarcoticInternalStock();
  renderNarcoticOrdersTable();
  renderNarcoticDepartmentsTable();
  renderNarcoticManageDrugsTable();
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
    patientName: q("narcoticPatientName").value.trim(),
    fileNumber: q("narcoticFileNumber").value.trim(),
    nationalId: q("narcoticNationalId").value.trim(),
    prescriptionNumber: q("narcoticPrescriptionNumber").value.trim(),
    doctorName: q("narcoticDoctorName").value.trim(),
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
    createdBy: APP.currentRole,
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
    emptyAmpoulesReceived: 0,
    quantitySent: 0,
    performedBy: payload.pharmacist,
    notes: `Prescription ${payload.prescriptionNumber} registered`
  });
  await batch.commit();
  resetNarcoticEntryForm();
  finishActionModal(true, "Narcotic prescription registered successfully.");
}
function addNarcoticOrderRow() {
  const departmentId = q("narcoticOrderDepartment").value;
  const drugId = q("narcoticOrderDrug").value;
  const emptyAmpoulesReceived = Number(q("narcoticOrderEmpty").value || 0);
  const quantitySent = Number(q("narcoticOrderSent").value || 0);
  const performedBy = q("narcoticOrderPharmacist").value;
  const nurseName = q("narcoticOrderNurseName").value.trim();
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
    if (!stockRow || !isAssignedDepartmentDrug(stockRow)) throw new Error("The selected drug is not assigned to the selected department.");
    const nextAvailable = Number(stockRow?.availableStockUnits || 0) + Number(row.quantitySent || 0);
    if (stockRow?.id) {
      batch.update(doc(db, "narcotic_department_stock", stockRow.id), { availableStockUnits: nextAvailable, updatedAt: serverTimestamp() });
    }
    const internalRow = narcoticInternalStockRow(row.drugId) || {};
    const nextInternal = Math.max(0, Number(internalRow.availableStockUnits || 0) - Number(row.quantitySent || 0));
    batch.set(doc(db, "narcotic_internal_stock", internalRow.id || `internal__${row.drugId}`), {
      id: internalRow.id || `internal__${row.drugId}`,
      drugId: row.drugId,
      drugName: row.drugName,
      availableStockUnits: nextInternal,
      reorderLevelUnits: Number(internalRow.reorderLevelUnits || 0),
      updatedAt: serverTimestamp()
    });
    batch.set(doc(collection(db, "narcotic_order_movements")), {
      ...row,
      type: "Department Order Movement",
      dateTime: jordanNowIso()
    });
  }
  await batch.commit();
  APP.narcoticOrdersBatchRows = [];
  renderNarcoticOrdersTable();
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
    const nextAvailable = Number(stockRow?.availableStockUnits || 0) + Number(row.quantitySent || 0);
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
  finishActionModal(true, "Department added successfully.");
}
async function deleteNarcoticDepartment(id) {
  if (APP.currentRole !== "ADMIN") return;
  showActionModal("Department", "Please wait while the department is being deleted...");
  await updateDoc(doc(db, "narcotic_departments", id), { active: false, updatedAt: serverTimestamp() });
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
  if (q("narcoticDepartmentModalNameCard")) q("narcoticDepartmentModalNameCard").textContent = dept?.departmentName || "-";
  if (q("narcoticDepartmentModalNotes")) q("narcoticDepartmentModalNotes").value = dept?.notes || "";
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
async function saveNarcoticDepartmentInfo() {
  if (APP.currentRole !== "ADMIN" || !APP.narcoticOpenDepartmentId) return;
  showActionModal("Department Info", "Saving department information...");
  await updateDoc(doc(db, "narcotic_departments", APP.narcoticOpenDepartmentId), {
    departmentName: q("narcoticDepartmentModalName")?.value?.trim() || "",
    notes: q("narcoticDepartmentModalNotes")?.value?.trim() || "",
    updatedAt: serverTimestamp()
  });
  if (q("narcoticDepartmentModalNameCard")) q("narcoticDepartmentModalNameCard").textContent = q("narcoticDepartmentModalName")?.value?.trim() || "-";
  finishActionModal(true, "Department information updated.");
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
  finishActionModal(true, "Narcotic drug added successfully.");
}

async function deleteNarcoticDrug(id) {
  if (APP.currentRole !== "ADMIN") return;
  showActionModal("Narcotic Drug", "Please wait while the drug is being deleted...");
  await updateDoc(doc(db, "narcotic_drugs", id), { active: false, updatedAt: serverTimestamp() });
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
  const rows = APP.cache.narcoticDepartmentStock.filter(r => r.departmentId === departmentId && (!drugId || r.drugId === drugId));
  const drug = narcoticDrugById(drugId);
  const body = `
    <div class="section-title">Department Stock Report</div>
    <div class="sub"><strong>Department:</strong> ${esc(dept?.departmentName || "")} &nbsp; | &nbsp; <strong>Drug:</strong> ${esc(drug ? `${drug.tradeName || ""} ${drug.strength || ""}`.trim() : "All drugs")}</div>
    <div class="section"><table><thead><tr><th>Drug</th><th>Fixed Stock</th><th>Available Stock</th></tr></thead>
    <tbody>${rows.map(r=>`<tr><td>${esc(r.drugName||"")}</td><td>${Number(r.fixedStockUnits||0)}</td><td>${Number(r.availableStockUnits||0)}</td></tr>`).join("") || `<tr><td colspan="3">No stock rows found.</td></tr>`}</tbody></table></div>
    <div class="section" style="margin-top:42px;display:flex;justify-content:flex-end"><div style="min-width:280px;text-align:center"><div style="font-weight:800">Pharmacist Signature</div><div style="margin-top:38px;border-top:1px solid #1b2b44"></div></div></div>`;
  const w = window.open("", "_blank"); w.document.write(buildPrintShell("Department Stock Report", dept?.departmentName || "Department", body)); w.document.close();
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
  const rows = APP.cache.narcoticPrescriptions.slice(0, 50);
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
      <td>${narcoticActionMenu(row)}</td>
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
  q("narcoticEditPharmacist").value = row.pharmacist || "";
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
    pharmacist: q("narcoticEditPharmacist").value.trim(),
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
    q("passwordModalRole").textContent = USERS[APP.pendingRole].displayName;
    q("loginPassword").value = "";
    openModal("passwordModal");
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

  const narcoticDrugCard = event.target.closest("[data-narcotic-drug-card]");
  if (narcoticDrugCard) return openNarcoticDrugInfoModal(narcoticDrugCard.dataset.narcoticDrugCard);

  const saveManualNarcoticStockBtn = event.target.closest("[data-save-narcotic-manual-stock]");
  if (saveManualNarcoticStockBtn) return saveNarcoticManualStock(saveManualNarcoticStockBtn.dataset.saveNarcoticManualStock);

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

  const returnBtn = event.target.closest(".return-btn");
  if (returnBtn) return openConfirmActionModal("return", returnBtn.dataset.id);

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

  const editPharmacistBtn = event.target.closest(".edit-pharmacist-btn");
  if (editPharmacistBtn) return startEditPharmacist(editPharmacistBtn.dataset.id);

  const removeDeptDrugBtn = event.target.closest("[data-remove-dept-drug]");
  if (removeDeptDrugBtn) return removeDrugFromNarcoticDepartment(removeDeptDrugBtn.dataset.removeDeptDrug);

  const deleteNarcoticDepartmentBtn = event.target.closest(".delete-narcotic-department-btn");
  if (deleteNarcoticDepartmentBtn) return deleteNarcoticDepartment(deleteNarcoticDepartmentBtn.dataset.id);

  const deleteNarcoticDrugBtn = event.target.closest(".delete-narcotic-drug-btn");
  if (deleteNarcoticDrugBtn) return deleteNarcoticDrug(deleteNarcoticDrugBtn.dataset.id);

  const deletePharmacistBtn = event.target.closest(".delete-pharmacist-btn");
  if (deletePharmacistBtn) return deletePharmacist(deletePharmacistBtn.dataset.id);

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
q("registerNarcoticBtn").onclick = registerNarcoticPrescription;
q("narcoticDepartmentSearch").oninput = renderNarcoticDepartmentCards;
q("addNarcoticOrderRowBtn").onclick = addNarcoticOrderRow;
q("submitNarcoticOrdersBtn").onclick = submitNarcoticOrders;
q("saveNarcoticDepartmentBtn").onclick = saveNarcoticDepartment;
q("addNarcoticDrugBtn").onclick = addNarcoticDrug;
q("saveNarcoticDeptStockBtn").onclick = saveNarcoticDepartmentStock;
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
q("passwordLoginBtn").onclick = () => doLogin(APP.pendingRole, q("loginPassword").value);
q("actionOkBtn").onclick = () => closeModal("actionModal");
q("changePasswordBtn").onclick = () => { q("userMenuDropdown").classList.add("hidden"); openModal("changePasswordModal"); };
q("savePasswordBtn").onclick = savePassword;
q("logoutBtn").onclick = () => {
  q("userMenuDropdown").classList.add("hidden");
  APP.listeners.forEach(unsub => unsub && unsub());
  APP.listeners = [];
  localStorage.removeItem("cdms_session_role");
  APP.currentRole = null;
  APP.currentUser = null;
  q("appShell").classList.add("hidden");
  q("loginScreen").classList.remove("hidden");
};
q("themeToggle").onclick = () => { q("userMenuDropdown").classList.add("hidden"); openModal("themeModal"); };
q("registerQuickBtn").onclick = registerQuickPrescription;
q("saveEditPrescriptionBtn").onclick = saveEditedPrescription;
q("saveAdjustStockBtn").onclick = saveAdjustedStock;
q("saveSettingsBtn").onclick = saveSettings;
q("resetPasswordOpenBtn").onclick = openResetPasswordModal;
q("confirmResetPasswordBtn").onclick = resetSelectedPassword;
q("settingsPharmacy").onchange = () => {
  if (APP.currentRole !== "ADMIN") return;
  APP.cache.settings = { ...(APP.cache.settings || {}), pharmacyType: q("settingsPharmacy").value };
  renderAll();
};
q("savePharmacistBtn").onclick = savePharmacist;
q("cancelPharmacistEditBtn").onclick = resetPharmacistForm;
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
q("transactionsAllBtn").onclick = () => {
  q("transactionsTypeFilter").value = "";
  q("transactionsFromDate").value = "";
  q("transactionsToDate").value = "";
  q("transactionsSearch").value = "";
  renderTransactions();
};
q("printTransactionsBtn").onclick = printTransactionsPage;
q("printInventoryBtn").onclick = printInventoryPage;
q("drugCardsPrevBtn").onclick = () => { APP.drugCardsPage = Math.max(1, APP.drugCardsPage - 1); renderDashboard(); };
q("drugCardsNextBtn").onclick = () => { APP.drugCardsPage += 1; renderDashboard(); };


enableAutoFocusFlow(
  ["quickPatient","quickFile","quickPrescriptionType","quickDrugSearch","quickDrug","quickBoxes","quickUnits","quickDoctor","quickPharmacist"],
  () => registerQuickPrescription()
);

themeInit();
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


if (q("receiveNarcoticInternalBtn")) q("receiveNarcoticInternalBtn").onclick = receiveNarcoticInternalShipment;
if (q("saveNarcoticDepartmentInfoBtn")) q("saveNarcoticDepartmentInfoBtn").onclick = saveNarcoticDepartmentInfo;
if (q("narcoticInternalSearch")) q("narcoticInternalSearch").oninput = renderNarcoticInternalStock;
