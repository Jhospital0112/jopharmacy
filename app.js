
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = "https://njuztkkjakwgjsflflct.supabase.co";
const SUPABASE_ANON_KEY = "sb_publishable_CfupR7mewq144VQxFMJ9RQ_kxACfqYY";

if (SUPABASE_URL.includes("https://njuztkkjakwgjsflflct.supabase.co") || SUPABASE_ANON_KEY.includes("sb_publishable_CfupR7mewq144VQxFMJ9RQ_kxACfqYY")) {
  alert("Supabase is not configured yet. Open app.js and paste your SUPABASE_URL and SUPABASE_ANON_KEY first.");
}

const db = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { params: { eventsPerSecond: 10 } }
});

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

async function getDoc(docRef) {
  const { data, error } = await db.from(docRef.table).select("*").eq("id", docRef.id).maybeSingle();
  if (error) throw error;
  return makeDocSnapshot(data || null);
}

async function applyFilters(builder, filters = []) {
  let q = builder;
  for (const f of filters) {
    if (!f) continue;
    if (f.op === "==" || f.op === "eq") q = q.eq(f.field, f.value);
  }
  return q;
}

async function fetchRows(ref) {
  const table = ref.table;
  let builder = db.from(table).select("*");
  if (ref.kind === "query") builder = await applyFilters(builder, ref.filters);
  const { data, error } = await builder;
  if (error) throw error;
  return data || [];
}

function onSnapshot(ref, callback) {
  const table = ref.table;
  const channelName = `watch:${table}:${crypto.randomUUID()}`;
  let active = true

  const emit = async () => {
    if (!active) return;
    try {
      const rows = await fetchRows(ref);
      callback(makeQuerySnapshot(rows));
    } catch (err) {
      console.error("Snapshot error:", err);
    }
  };

  emit();

  const channel = db.channel(channelName)
    .on("postgres_changes", { event: "*", schema: "public", table }, () => emit())
    .subscribe();

  return () => {
    active = false;
    try { db.removeChannel(channel); } catch (e) {}
  };
}

async function setDoc(docRef, payload) {
  const row = cleanData({ id: docRef.id, ...payload });
  const { error } = await db.from(docRef.table).upsert(row, { onConflict: "id" });
  if (error) throw error;
}

async function updateDoc(docRef, payload) {
  const row = cleanData(payload);
  const { error } = await db.from(docRef.table).update(row).eq("id", docRef.id);
  if (error) throw error;
}

async function addDoc(collectionRef, payload) {
  const row = cleanData({ id: crypto.randomUUID(), ...payload });
  const { data, error } = await db.from(collectionRef.table).insert(row).select("id").single();
  if (error) throw error;
  return { id: data.id };
}

async function deleteDoc(docRef) {
  const { error } = await db.from(docRef.table).delete().eq("id", docRef.id);
  if (error) throw error;
}

function serverTimestamp() {
  return new Date().toISOString();
}

function writeBatch() {
  const ops = [];
  return {
    set(ref, data) { ops.push(["set", ref, data]); },
    update(ref, data) { ops.push(["update", ref, data]); },
    delete(ref) { ops.push(["delete", ref]); },
    async commit() {
      for (const [type, ref, data] of ops) {
        if (type === "set") await setDoc(ref, data);
        if (type === "update") await updateDoc(ref, data);
        if (type === "delete") await deleteDoc(ref);
      }
    }
  };
}

const DEFAULT_PASSWORD = "111111";
// Internal app password only. Database access itself is controlled by Supabase project keys and policies.
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
  cache: {
    drugs: [],
    inventory: [],
    prescriptions: [],
    transactions: [],
    pharmacists: [],
    settings: {}
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

function themeInit() {
  document.body.classList.toggle("dark", (localStorage.getItem("cdms_theme") || "light") === "dark");
}

function toggleTheme() {
  const dark = !document.body.classList.contains("dark");
  document.body.classList.toggle("dark", dark);
  localStorage.setItem("cdms_theme", dark ? "dark" : "light");
}

function openModal(id) {
  q("overlay").classList.remove("hidden");
  q(id).classList.remove("hidden");
}

function closeModal(id) {
  q(id).classList.add("hidden");
  if (!document.querySelector(".modal:not(.hidden)")) q("overlay").classList.add("hidden");
}

function showActionModal(title, body, waiting = true) {
  q("actionTitle").textContent = title;
  q("actionBody").innerHTML = waiting ? `<span class="spinner"></span>${body}` : body;
  q("actionOkBtn").classList.toggle("hidden", waiting);
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


function refreshScopedSelectors() {
  const scope = currentScopePharmacy();
  const isAdmin = APP.currentRole === "ADMIN";
  [q("dashboardScopeChip"), q("controlPharmacy")].forEach(el => { if (el) el.textContent = scope; });

  const inventoryOptions = isAdmin ? PHARMACIES : [scope];
  const currentInventory = q("inventoryLocationFilter").value || scope;
  q("inventoryLocationFilter").innerHTML = inventoryOptions.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
  q("inventoryLocationFilter").value = inventoryOptions.includes(currentInventory) ? currentInventory : scope;
  q("inventoryLocationFilter").disabled = !isAdmin;

  const reportOptions = isAdmin
    ? [{ value: "ALL_WORK_PHARMACIES", label: "All Pharmacies" }, ...WORK_PHARMACIES.map(name => ({ value: name, label: name }))]
    : [{ value: scope, label: scope }];
  const currentReport = q("reportPharmacy").value || (isAdmin ? "ALL_WORK_PHARMACIES" : scope);
  q("reportPharmacy").innerHTML = reportOptions.map(opt => `<option value="${esc(opt.value)}">${esc(opt.label)}</option>`).join("");
  q("reportPharmacy").value = reportOptions.some(opt => opt.value === currentReport) ? currentReport : reportOptions[0].value;
  q("reportPharmacy").disabled = false;

  const auditOptions = isAdmin ? WORK_PHARMACIES : [scope];
  const currentAudit = q("auditPharmacy").value || scope;
  q("auditPharmacy").innerHTML = auditOptions.map(name => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
  q("auditPharmacy").value = auditOptions.includes(currentAudit) ? currentAudit : scope;
  q("auditPharmacy").disabled = !isAdmin;

  q("shipmentLocation").value = scope;
}


async function bootstrapIfNeeded() {
  const marker = await getDoc(doc(db, "meta", "bootstrap"));
  if (marker.exists()) return;

  showActionModal("First Setup", "Preparing Supabase data...");
  const defaultHash = await sha256(DEFAULT_PASSWORD);

  for (const [id, data] of Object.entries(USERS)) {
    await setDoc(doc(db, "users", id), {
      ...data,
      id,
      passwordHash: defaultHash,
      mustChangePassword: true,
      active: true,
      createdAt: serverTimestamp(),
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

  await setDoc(doc(db, "meta", "bootstrap"), { createdAt: serverTimestamp() });
  finishActionModal(true, "Supabase data prepared.");
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

  APP.listeners.push(onSnapshot(doc(db, "settings", "main"), snap => {
    APP.cache.settings = snap.exists() ? snap.data() : {};
    renderStaticOptions();
    renderAll();
  }));
}

function applyRoleUI() {
  const isAdmin = APP.currentRole === "ADMIN";
  document.querySelectorAll(".admin-only").forEach(el => el.classList.toggle("hidden", !isAdmin));
  document.querySelectorAll(".admin-only-block").forEach(el => el.classList.toggle("hidden", !isAdmin));
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
  updateQuickAvailableStock();
  renderLiveClocks();
}

function renderControlPanel() {
  q("controlUser").textContent = APP.currentRole === "ADMIN" ? "admin@demo.local" : APP.currentUser.displayName;
  q("controlRole").textContent = APP.currentUser.role?.replaceAll("_", " ") || APP.currentRole;
  q("controlPharmacy").textContent = currentScopePharmacy();
}

function renderDashboard() {
  const scope = currentScopePharmacy();
  const scopedPrescriptions = prescriptionScopeRows();
  const scopedTransactions = transactionScopeRows();
  const cardSearch = (q("drugCardsSearch").value || "").toLowerCase();

  const today = jordanDateKey();
  q("metricRegistered").textContent = scopedPrescriptions.filter(p => formatJordanDateTime(p.dateTime).slice(0,10) === today).length;
  q("metricPending").textContent = scopedPrescriptions.filter(p => (p.status || "") === "Pending" && formatJordanDateTime(p.dateTime).slice(0,10) === today).length;
  q("metricReturned").textContent = scopedPrescriptions.filter(p => (p.status || "") === "Returned" && formatJordanDateTime(p.dateTime).slice(0,10) === today).length;

  q("recentList").innerHTML = scopedPrescriptions.slice(0, 7).map(p => {
    const drug = APP.cache.drugs.find(d => d.id === p.drugId);
    return `
      <div class="recent-row">
        <div>
          <div class="recent-name">${esc(p.patientName)}</div>
          <div class="subline">${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))} • ${esc(p.fileNumber || "")}</div>
        </div>
        <div class="subline">${esc(formatJordanDateTime(p.dateTime))}</div>
        <div class="subline">${Number(p.qtyBoxes || 0)} box(es) + ${Number(p.qtyUnits || 0)} ${esc(unitLabel(drug))}</div>
        <div>${statusBadge(p.status || "Registered")}</div>
      </div>`;
  }).join("") || `<div class="empty-state">No prescriptions found for ${esc(scope)}.</div>`;

  q("drugCards").innerHTML = APP.cache.drugs
    .filter(drug => `${drug.scientificName} ${drug.tradeName} ${drug.category} ${drug.strength} ${drug.dosageForm}`.toLowerCase().includes(cardSearch))
    .map(drug => {
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
  const term = (q("transactionsSearch").value || "").toLowerCase();
  q("transactionsTbody").innerHTML = transactionScopeRows().filter(row => `${row.type} ${row.tradeName} ${row.pharmacy} ${row.performedBy} ${row.note}`.toLowerCase().includes(term)).map(row => `
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
  openModal("confirmActionModal");
}

async function submitConfirmedAction() {
  if (!APP.confirmAction) return closeModal("confirmActionModal");
  const pharmacistName = q("confirmActionPharmacist").value;
  if (!pharmacistName) {
    showActionModal("Validation", "Please select the pharmacist who performed this action.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const { type, id } = APP.confirmAction;
  closeModal("confirmActionModal");
  APP.confirmAction = null;
  if (type === "return") return performReturnPrescription(id, pharmacistName);
  if (type === "delete") return performDeletePrescription(id, pharmacistName);
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
      <td>${p.active !== false ? "Yes" : "No"}</td>
      <td>
        <div class="button-inline-group">
          <button class="soft-btn mini-btn edit-pharmacist-btn" data-id="${p.id}">Edit</button>
          <button class="soft-btn mini-btn delete-pharmacist-btn" data-id="${p.id}">Delete</button>
        </div>
      </td>
    </tr>`;
  }).join("") || `<tr><td colspan="6" class="empty-state">No pharmacists found.</td></tr>`;
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


function findDuplicatePrescriptions(payload) {
  const now = Date.now();
  return APP.cache.prescriptions.filter(row => row.drugId === payload.drugId && WORK_PHARMACIES.includes(row.pharmacy)).filter(row => {
    const rowDate = parseJordanDateTime(row.dateTime);
    if (!rowDate) return false;
    const within30 = now - rowDate.getTime() <= 30 * 24 * 60 * 60 * 1000;
    const samePatient = String(row.patientName || "").trim().toLowerCase() === String(payload.patientName || "").trim().toLowerCase();
    return within30 && samePatient;
  }).sort((a,b) => String(b.dateTime || "").localeCompare(String(a.dateTime || "")));
}

function openDuplicateWarningModal(payload, duplicates) {
  APP.pendingQuickPayload = payload;
  q("duplicateWarningText").textContent = "Similar prescriptions were found for the same patient and drug within the last 30 days.";
  q("duplicateWarningTbody").innerHTML = duplicates.map(row => {
    const drug = APP.cache.drugs.find(d => d.id === row.drugId);
    return `<tr><td>${esc((drug?.tradeName || "") + " " + (drug?.strength || ""))}</td><td>${esc(row.patientName || "")}</td><td>${esc(row.fileNumber || "")}</td><td>${esc(row.pharmacy || "")}</td><td>${esc(formatJordanDateTime(row.dateTime))}</td></tr>`;
  }).join("");
  openModal("duplicateWarningModal");
}

async function continueQuickRegistration(payload) {
  const { drugId, patientName, fileNumber, doctorName, pharmacistName, qtyBoxes, qtyUnits } = payload;
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
      drugId, pharmacy, patientName, fileNumber, doctorName, pharmacistName, qtyBoxes, qtyUnits, status: "New", auditBy: "", auditDateTime: null, auditNote: "", returnBy: "", returnDateTime: null, returnNote: "", dateTime: jordanNowIso(), createdBy: APP.currentRole, updatedBy: APP.currentRole
    });
    const txRef = doc(collection(db, "transactions"));
    await setDoc(txRef, { type: "Dispense", drugId, tradeName: drug.tradeName, pharmacy, qtyBoxes, qtyUnits, performedBy: pharmacistName, note: `Prescription: ${patientName}`, dateTime: jordanNowIso() });
    if (!inv?.id) throw new Error("Inventory record was not found for the selected drug and pharmacy.");
    await updateDoc(doc(db, "inventory", inv.id), { ...updatedStock, updatedAt: serverTimestamp() });
    ["quickPatientName","quickPatientFile","quickDoctor"].forEach(id => q(id).value = "");
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

  const duplicates = findDuplicatePrescriptions(payload);
  if (duplicates.length) {
    openDuplicateWarningModal(payload, duplicates);
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
  const pharmacies = Array.isArray(pharmacist.pharmacies) && pharmacist.pharmacies.length ? pharmacist.pharmacies : [pharmacist.workplace].filter(Boolean);
  document.querySelectorAll('.pharmacy-checkbox').forEach(el => el.checked = pharmacies.includes(el.value));
  renderSettings();
}

async function savePharmacist() {
  const name = q("pharmacistName").value.trim();
  const jobNumber = q("pharmacistJobNumber").value.trim();
  const pharmacies = selectedPharmacistPharmacies();
  if (!name || !jobNumber || !pharmacies.length) {
    showActionModal("Validation", "Please enter pharmacist name, job number, and select at least one pharmacy.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const duplicate = APP.cache.pharmacists.find(p => p.active !== false && p.jobNumber === jobNumber && p.id !== APP.pharmacistEditId);
  if (duplicate) {
    showActionModal("Validation", "This job number already exists. Pharmacist was not saved.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  showActionModal(APP.pharmacistEditId ? "Update Pharmacist" : "Save Pharmacist", "Please wait while the pharmacist record is being saved...");
  const payload = {
    name,
    jobNumber,
    workplace: pharmacies[0],
    pharmacies,
    canAudit: q("pharmacistCanAudit").value === "true",
    active: true,
    updatedAt: serverTimestamp()
  };
  if (APP.pharmacistEditId) {
    await updateDoc(doc(db, "pharmacists", APP.pharmacistEditId), payload);
  } else {
    await addDoc(collection(db, "pharmacists"), { ...payload, createdAt: serverTimestamp() });
  }
  const wasEditing = !!APP.pharmacistEditId;
  resetPharmacistForm();
  finishActionModal(true, wasEditing ? "Pharmacist updated successfully." : "Pharmacist saved successfully.");
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
    const allowed = canEditPrescription(row);
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
      <td>${row.status === "Returned" ? "-" : `<button class="soft-btn mini-btn return-btn" data-id="${row.id}">Return</button>`}</td>
      <td>
        <div class="audit-action-group vertical-edit-group">
          ${row.status === "Returned" ? "-" : `<button class="primary-btn mini-btn edit-rx-btn ${allowed ? "" : "hidden"}" data-edit-btn data-id="${row.id}">Edit</button><div class="edit-countdown hidden" data-edit-countdown="${row.id}"></div>`}
          ${APP.currentRole === "ADMIN" ? `<button class="mini-danger-btn mini-btn delete-rx-btn" data-id="${row.id}">Delete</button>` : ""}
        </div>
      </td>
    </tr>`;
  }).join("") || `<tr><td colspan="10" class="empty-state">No prescriptions for this drug.</td></tr>`;
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

async function receiveShipment() {
  const drugId = q("shipmentDrug").value;
  const boxes = Number(q("shipmentBoxes").value || 0);
  const units = Number(q("shipmentUnits").value || 0);
  const pharmacy = q("shipmentLocation").value || currentScopePharmacy();
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const inv = invRow(drugId, pharmacy);
  if (!drug || !inv) return;
  const updated = normalizeInventory(Number(inv.boxes || 0) + boxes, Number(inv.units || 0) + units, drug.unitsPerBox);

  showActionModal("Receive Shipment", "Please wait while the shipment is being received...");
  const batch = writeBatch(db);
  batch.update(doc(db, "inventory", inv.id), { ...updated, updatedAt: serverTimestamp() });
  batch.set(doc(collection(db, "transactions")), {
    type: "Receive Shipment",
    drugId,
    tradeName: drug.tradeName,
    pharmacy,
    qtyBoxes: boxes,
    qtyUnits: units,
    performedBy: APP.currentRole,
    note: "Shipment received",
    dateTime: jordanNowIso()
  });
  await batch.commit();
  closeModal("shipmentModal");
  finishActionModal(true, "Shipment received successfully.");
}

async function transferStock() {
  const drugId = q("transferDrug").value;
  const boxes = Number(q("transferBoxes").value || 0);
  const units = Number(q("transferUnits").value || 0);
  const from = q("transferFrom").value;
  const to = q("transferTo").value;
  if (from === to) {
    showActionModal("Transfer", "From and To locations must be different.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const drug = APP.cache.drugs.find(d => d.id === drugId);
  const fromInv = invRow(drugId, from);
  const toInv = invRow(drugId, to);
  const delta = boxes * Number(drug.unitsPerBox || 1) + units;
  if (delta > Number(fromInv.totalUnits || 0)) {
    showActionModal("Transfer", "Insufficient stock in source location.", false);
    q("actionOkBtn").classList.remove("hidden");
    return;
  }
  const updatedFrom = normalizeInventory(0, Number(fromInv.totalUnits || 0) - delta, drug.unitsPerBox);
  const updatedTo = normalizeInventory(0, Number(toInv.totalUnits || 0) + delta, drug.unitsPerBox);

  showActionModal("Transfer Stock", "Please wait while the stock is being transferred...");
  const batch = writeBatch(db);
  batch.update(doc(db, "inventory", fromInv.id), { ...updatedFrom, updatedAt: serverTimestamp() });
  batch.update(doc(db, "inventory", toInv.id), { ...updatedTo, updatedAt: serverTimestamp() });
  batch.set(doc(collection(db, "transactions")), {
    type: "Transfer",
    drugId,
    tradeName: drug.tradeName,
    pharmacy: `${from} → ${to}`,
    qtyBoxes: boxes,
    qtyUnits: units,
    performedBy: APP.currentRole,
    note: "Stock transfer",
    dateTime: jordanNowIso()
  });
  await batch.commit();
  closeModal("transferModal");
  finishActionModal(true, "Stock transferred successfully.");
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
              <td colspan="7"><strong>Status:</strong> ${esc(row.status || "-")}</td>
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
  const selectedPharmacy = currentReportPharmacy();

  const reportPharmacies = selectedPharmacy === "ALL_WORK_PHARMACIES" ? WORK_PHARMACIES : [selectedPharmacy];
  const orderedSections = ["General Hospital Stock", ...reportPharmacies];

  const body = orderedSections.map((pharmacy, sectionIndex) => {
    const stockRows = sortDrugsAlphabetically(APP.cache.drugs).map(drug => {
      const inv = invRow(drug.id, pharmacy) || { boxes: 0, units: 0, totalUnits: 0 };
      return `
        <tr>
          <td>${esc(drug.tradeName || "")}</td>
          <td>${esc(drug.strength || "")}</td>
          <td>${esc(drug.scientificName || "")}</td>
          <td>${esc(drug.dosageForm || "")}</td>
          <td>${Number(drug.unitsPerBox || 0)}</td>
          <td>${Number(inv.boxes || 0)}</td>
          <td>${Number(inv.units || 0)}</td>
          <td>${Number(inv.totalUnits || 0)}</td>
        </tr>`;
    }).join("");

    const prescriptions = pharmacy === "General Hospital Stock"
      ? []
      : scopedPrescriptionRowsByPharmacy(pharmacy).filter(row => (!from || formatJordanDateTime(row.dateTime).slice(0, 10) >= from) && (!to || formatJordanDateTime(row.dateTime).slice(0, 10) <= to));

    const prescriptionGroups = sortDrugsAlphabetically(APP.cache.drugs).map(drug => ({
      drug,
      rows: prescriptions.filter(row => row.drugId === drug.id).sort((a, b) => String(a.patientName || "").localeCompare(String(b.patientName || "")) || String(a.dateTime || "").localeCompare(String(b.dateTime || "")))
    })).filter(group => group.rows.length);

    const stockTitle = pharmacy === "General Hospital Stock" ? "General Hospital Stock Inventory" : `${pharmacy} Inventory`;
    const prescriptionsTitle = pharmacy === "General Hospital Stock" ? "" : `${pharmacy} Registered Prescriptions`;

    return `
      <div class="group ${sectionIndex ? 'page-break' : ''}">
        <div class="section-title">${esc(stockTitle)}</div>
        <table>
          <thead><tr><th>Trade Name</th><th>Strength</th><th>Scientific Name</th><th>Dosage Form</th><th>Units / Box</th><th>Boxes</th><th>Units</th><th>Total Units</th></tr></thead>
          <tbody>${stockRows}</tbody>
        </table>

        ${pharmacy === "General Hospital Stock" ? "" : `
          <div class="section-title" style="margin-top:24px">${esc(prescriptionsTitle)}</div>
          ${prescriptionGroups.map((group, groupIndex) => `
            <div class="group ${groupIndex ? 'page-break' : ''}">
              <div class="section-title" style="font-size:14px">${esc(group.drug.tradeName)} ${esc(group.drug.strength)}</div>
              <div class="sub">${esc(group.drug.scientificName)} · ${esc(group.drug.dosageForm)} · ${esc(pharmacy)}</div>
              <table>
                <thead><tr><th>Date & Time</th><th>Patient</th><th>File No.</th><th>Boxes</th><th>Units</th><th>Doctor</th><th>Status</th></tr></thead>
                <tbody>
                  ${group.rows.map(row => `
                    <tr>
                      <td>${esc(formatJordanDateTime(row.dateTime))}</td>
                      <td>${esc(row.patientName || "")}</td>
                      <td>${esc(row.fileNumber || "")}</td>
                      <td>${Number(row.qtyBoxes || 0)}</td>
                      <td>${Number(row.qtyUnits || 0)}</td>
                      <td>${esc(row.doctorName || "")}</td>
                      <td>${esc(row.status || "")}</td>
                    </tr>
                    <tr>
                      <td colspan="7"><strong>Audit Details:</strong> ${row.status === 'Returned' ? '-' : esc((row.auditBy || '') + (row.auditDateTime ? ` • ${formatJordanDateTime(row.auditDateTime)}` : ''))}</td>
                    </tr>
                  `).join("")}
                </tbody>
              </table>
            </div>
          `).join("") || `<div class="sub">No registered prescriptions found for the selected date range.</div>`}
        `}
      </div>`;
  }).join("");

  const subtitleLabel = selectedPharmacy === "ALL_WORK_PHARMACIES" ? "All Pharmacies" : selectedPharmacy;
  const w = window.open("", "_blank");
  w.document.write(buildPrintShell("Comprehensive Report", `${subtitleLabel}${from || to ? ` · ${from || ""} to ${to || ""}` : ""}`, body || `<div class="section-title">No records found</div>`));
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

document.addEventListener("click", event => {
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

  const tab = event.target.closest(".tab-btn[data-audittab]");
  if (tab) {
    APP.auditTab = tab.dataset.audittab;
    document.querySelectorAll(".tab-btn").forEach(btn => btn.classList.toggle("active", btn === tab));
    renderAudit();
    return;
  }

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

  const editBtn = event.target.closest(".edit-rx-btn");
  if (editBtn) {
    closeAuditMenuPortal();
    return openEditPrescription(editBtn.dataset.id);
  }

  const deleteRxBtn = event.target.closest(".delete-rx-btn");
  if (deleteRxBtn) {
    closeAuditMenuPortal();
    return openConfirmActionModal("delete", deleteRxBtn.dataset.id);
  }

  const adjustStockBtn = event.target.closest("[data-adjust-stock]");
  if (adjustStockBtn) return openAdjustStockModal(adjustStockBtn.dataset.adjustStock);

  const editPharmacistBtn = event.target.closest(".edit-pharmacist-btn");
  if (editPharmacistBtn) return startEditPharmacist(editPharmacistBtn.dataset.id);

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

  if (event.target.dataset.close) return closeModal(event.target.dataset.close);
});

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
q("themeToggle").onclick = () => { q("userMenuDropdown").classList.add("hidden"); toggleTheme(); };
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
q("openShipmentModalBtn").onclick = () => openModal("shipmentModal");
q("openShipmentModalBtn2").onclick = () => openModal("shipmentModal");
q("openTransferModalBtn").onclick = () => openModal("transferModal");
q("openTransferModalBtn2").onclick = () => openModal("transferModal");
q("openAddDrugModalBtn").onclick = () => openModal("addDrugModal");
q("openAddDrugModalBtn2").onclick = () => openModal("addDrugModal");
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
q("drugCardsSearch").oninput = renderDashboard;
q("auditSearch").oninput = renderAudit;
q("quickDrug").onchange = updateQuickAvailableStock;
q("confirmActionCancelBtn").onclick = () => { APP.confirmAction = null; closeModal("confirmActionModal"); };
q("confirmActionOkBtn").onclick = submitConfirmedAction;
q("duplicateWarningCancelBtn").onclick = () => { APP.pendingQuickPayload = null; closeModal("duplicateWarningModal"); };
q("duplicateWarningContinueBtn").onclick = async () => { const payload = APP.pendingQuickPayload; closeModal("duplicateWarningModal"); if (payload) await continueQuickRegistration(payload); };

if (APP.liveTimer) clearInterval(APP.liveTimer);
APP.liveTimer = setInterval(renderLiveClocks, 1000);
renderLiveClocks();

themeInit();
await bootstrapIfNeeded();
await tryRestoreSession();


