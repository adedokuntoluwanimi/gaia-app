console.log("GAIA frontend loaded");

let csvHeaders = [];
let currentJobId = null;
let pollInterval = null;

const statusEl = document.getElementById("jobStatus");
const createBtn = document.getElementById("createJobBtn");
const downloadBtn = document.getElementById("downloadBtn");

// ---------------- CSV headers ----------------
document.getElementById("csvFile").addEventListener("change", e => {
  const file = e.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = evt => {
    csvHeaders = evt.target.result.split(/\r?\n/)[0].split(",");
    populate("xColumn");
    populate("yColumn");
    populate("valueColumn");
  };
  reader.readAsText(file);
});

function populate(id) {
  const select = document.getElementById(id);
  select.innerHTML = "";
  csvHeaders.forEach(h => {
    const opt = document.createElement("option");
    opt.value = h.trim();
    opt.textContent = h.trim();
    select.appendChild(opt);
  });
}

// ---------------- Scenario UX ----------------
document.getElementById("scenario").addEventListener("change", e => {
  const spacing = document.getElementById("spacing");
  if (e.target.value === "explicit_geometry") {
    spacing.disabled = true;
    spacing.value = "";
  } else {
    spacing.disabled = false;
    spacing.value = spacing.value || 10;
  }
});

// ---------------- Create Job ----------------
document.getElementById("createJobBtn").addEventListener("click", async () => {
  const fileInput = document.getElementById("csvFile");
  const scenario = document.getElementById("scenario").value;
  const xCol = document.getElementById("xColumn").value;
  const yCol = document.getElementById("yColumn").value;
  const valCol = document.getElementById("valueColumn").value;
  const spacing = document.getElementById("spacing").value;

  if (!fileInput.files[0]) return alert("Upload a CSV file");
  if (!xCol || !yCol) return alert("Select X and Y columns");

  if (scenario === "sparse_only") {
    if (!valCol) return alert("Value column required");
    if (!spacing || Number(spacing) <= 0) return alert("Invalid spacing");
  }

  const form = new FormData();
  form.append("csv_file", fileInput.files[0]);
  form.append("scenario", scenario);
  form.append("x_column", xCol);
  form.append("y_column", yCol);
  if (valCol) form.append("value_column", valCol);
  if (scenario === "sparse_only") form.append("output_spacing", spacing);

  createBtn.disabled = true;
  statusEl.innerText = "Uploading...";
  downloadBtn.style.display = "none";

  const res = await fetch("/jobs", { method: "POST", body: form });

  if (!res.ok) {
    statusEl.innerText = "Job failed";
    createBtn.disabled = false;
    return;
  }

  const data = await res.json();
  currentJobId = data.job_id;

  statusEl.innerText = "Inferencing...";
  loadPreview(currentJobId);
  pollStatus(currentJobId);
});

// ---------------- Status polling ----------------
function pollStatus(jobId) {
  pollInterval = setInterval(async () => {
    const res = await fetch(`/jobs/${jobId}/status`);
    if (!res.ok) return;

    const data = await res.json();
    statusEl.innerText = data.status;

    if (data.status === "complete") {
      clearInterval(pollInterval);
      statusEl.innerText = "Completed";
      downloadBtn.href = `/jobs/${jobId}/result`;
      downloadBtn.style.display = "block";
      createBtn.disabled = false;
    }

    if (data.status === "failed") {
      clearInterval(pollInterval);
      statusEl.innerText = "Failed";
      createBtn.disabled = false;
    }
  }, 3000);
}

// ---------------- Geometry preview ----------------
async function loadPreview(jobId) {
  const res = await fetch(`/jobs/${jobId}/preview`);
  if (!res.ok) return;
  const data = await res.json();
  drawGeometry(data.measured, data.generated);
}

function drawGeometry(measured, generated) {
  const svg = document.getElementById("geometry-svg");
  svg.innerHTML = "";

  const all = measured.concat(generated);
  if (!all.length) return;

  const xs = all.map(p => p.x);
  const ys = all.map(p => p.y);

  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);

  const pad = 40;
  const w = 1000 - pad * 2;
  const h = 600 - pad * 2;

  const sx = x => pad + ((x - minX) / (maxX - minX || 1)) * w;
  const sy = y => pad + h - ((y - minY) / (maxY - minY || 1)) * h;

  measured.forEach(p => drawPoint(svg, sx(p.x), sy(p.y), "measured"));
  generated.forEach(p => drawPoint(svg, sx(p.x), sy(p.y), "predicted"));
}

function drawPoint(svg, x, y, cls) {
  const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
  c.setAttribute("cx", x);
  c.setAttribute("cy", y);
  c.setAttribute("r", 5);
  c.setAttribute("class", cls);
  svg.appendChild(c);
}
