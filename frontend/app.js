// Fernwood Mini Store dashboard. Plain JavaScript, no build step.
// All user-supplied text is inserted with textContent (never innerHTML) so a customer name
// like "<script>" cannot run in anyone's browser.

const API = (window.API_BASE_URL || "").replace(/\/$/, "");
const STATUSES = ["PENDING", "PROCESSING", "RETRYING", "COMPLETED", "FAILED"];
const REFRESH_MS = 3000;

const form = document.getElementById("order-form");
const messageEl = document.getElementById("message");
const bannerEl = document.getElementById("banner");
const tbody = document.getElementById("orders");
const countsEl = document.getElementById("counts");
const updatedEl = document.getElementById("updated");

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function badge(status) {
  return el("span", `badge ${status}`, status);
}

function renderCounts(orders) {
  countsEl.replaceChildren();
  for (const status of STATUSES) {
    const n = orders.filter((o) => o.status === status).length;
    const chip = el("span", `badge ${status}`, `${status}: ${n}`);
    countsEl.appendChild(chip);
  }
}

function renderOrders(orders) {
  tbody.replaceChildren();
  if (orders.length === 0) {
    const row = el("tr");
    const cell = el("td", "muted", "No orders yet. Place one above.");
    cell.colSpan = 7;
    row.appendChild(cell);
    tbody.appendChild(row);
    return;
  }
  for (const o of orders) {
    const row = el("tr");
    row.appendChild(el("td", "", `#${o.id}`));
    row.appendChild(el("td", "", o.customer_name));
    row.appendChild(el("td", "", o.item));
    row.appendChild(el("td", "", String(o.quantity)));
    const statusCell = el("td");
    statusCell.appendChild(badge(o.status));
    row.appendChild(statusCell);
    row.appendChild(el("td", "", String(o.attempts)));
    row.appendChild(el("td", "details", o.failure_reason || ""));
    tbody.appendChild(row);
  }
}

async function refresh() {
  try {
    const res = await fetch(`${API}/orders?limit=50`);
    if (!res.ok) throw new Error(`API returned ${res.status}`);
    const orders = await res.json();
    bannerEl.hidden = true;
    renderCounts(orders);
    renderOrders(orders);
    updatedEl.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    bannerEl.hidden = false;
    bannerEl.textContent = `Cannot reach the API at "${API || window.location.origin}". Check config.js. (${err.message})`;
  }
}

function setMessage(text, kind) {
  messageEl.textContent = text;
  messageEl.className = `message ${kind || ""}`;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = form.querySelector("button");
  button.disabled = true;
  setMessage("");
  try {
    const res = await fetch(`${API}/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        customer_name: document.getElementById("customer_name").value.trim(),
        item: document.getElementById("item").value.trim(),
        quantity: Number(document.getElementById("quantity").value),
      }),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      const detail = typeof body.detail === "string" ? body.detail : `request failed (${res.status})`;
      throw new Error(detail);
    }
    const order = await res.json();
    setMessage(`Order #${order.id} placed.`, "ok");
    form.reset();
    document.getElementById("quantity").value = 1;
    refresh();
  } catch (err) {
    setMessage(`Could not place order: ${err.message}`, "error");
  } finally {
    button.disabled = false;
  }
});

refresh();
setInterval(refresh, REFRESH_MS);
