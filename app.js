
// ===== GOOGLE SHEETS ONLY VERSION =====

const API_URL = window.APP_CONFIG.APPS_SCRIPT_WEB_APP_URL;

async function apiRequest(action, payload = {}) {
  const res = await fetch(API_URL, {
    method: "POST",
    body: JSON.stringify({ action, ...payload }),
  });
  const data = await res.json();
  if (!data.ok) throw new Error(data.error || "API Error");
  return data;
}

let savingPharmacistNow = false;

async function savePharmacist() {
  if (savingPharmacistNow) return;

  const name = document.getElementById("pharmacistName").value.trim();
  const jobNumber = document.getElementById("pharmacistJobNumber").value.trim();

  if (!name || !jobNumber) {
    alert("Enter required fields");
    return;
  }

  savingPharmacistNow = true;

  try {
    const res = await apiRequest("listDocs", { table: "pharmacists" });
    const list = res.data || [];

    const exists = list.find(p => String(p.jobNumber) === jobNumber);

    if (exists) {
      alert("Job number already exists");
      return;
    }

    await apiRequest("setDoc", {
      table: "pharmacists",
      id: crypto.randomUUID(),
      data: {
        name,
        jobNumber,
        active: true,
        createdAt: new Date().toISOString()
      }
    });

    alert("Saved successfully");

  } catch (err) {
    console.error(err);
    alert("Error saving pharmacist");
  } finally {
    savingPharmacistNow = false;
  }
}
