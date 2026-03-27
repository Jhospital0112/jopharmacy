// PATCHED app.js (Google Sheets version - Transfer Fix)

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

function jordanNowIso() {
  return new Date().toISOString();
}

// ===== PATCHED TRANSFER FUNCTION =====
async function transferStock() {
  const drugId = document.getElementById("transferDrug").value;
  const boxes = Number(document.getElementById("transferBoxes").value || 0);
  const units = Number(document.getElementById("transferUnits").value || 0);
  const from = document.getElementById("transferFrom").value;
  const to = document.getElementById("transferTo").value;

  if (!drugId) {
    alert("Please select a drug.");
    return;
  }

  if (from === to) {
    alert("From and To must be different.");
    return;
  }

  if (boxes === 0 && units === 0) {
    alert("Enter quantity.");
    return;
  }

  try {
    const invRes = await apiRequest("listDocs", { table: "inventory" });
    const inventory = invRes.data;

    const fromInv = inventory.find(i => i.drugId === drugId && i.pharmacy === from);
    const toInv = inventory.find(i => i.drugId === drugId && i.pharmacy === to);

    if (!fromInv || !toInv) {
      alert("Inventory not found.");
      return;
    }

    const unitsPerBox = Number(fromInv.unitsPerBox || 1);
    const delta = boxes * unitsPerBox + units;

    if (delta > Number(fromInv.totalUnits || 0)) {
      alert("Not enough stock.");
      return;
    }

    const newFrom = Number(fromInv.totalUnits) - delta;
    const newTo = Number(toInv.totalUnits) + delta;

    await apiRequest("batch", {
      operations: [
        {
          type: "update",
          table: "inventory",
          id: fromInv.id,
          data: { totalUnits: newFrom }
        },
        {
          type: "update",
          table: "inventory",
          id: toInv.id,
          data: { totalUnits: newTo }
        },
        {
          type: "set",
          table: "transactions",
          id: "tx_" + Date.now(),
          data: {
            type: "Transfer",
            drugId,
            from,
            to,
            boxes,
            units,
            dateTime: jordanNowIso()
          }
        }
      ]
    });

    alert("Transfer successful");

  } catch (e) {
    console.error(e);
    alert("Transfer failed");
  }
}
