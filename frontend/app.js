console.log("GAIA frontend loaded");

let csvHeaders = [];

// --------------------------------------------------
// CSV header extraction
// --------------------------------------------------
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

// --------------------------------------------------
// Scenario toggle
// --------------------------------------------------
document.getElementById("scenario").addEventListener("change", e => {
  const spacing = document.getElementById("spacing");
  spacing.disabled = e.target.value === "explicit_geometry";
});

// --------------------------------------------------
// Create Job
// --------------------------------------------------
document.getElementById("createJobBtn").addEventListener("click", async () => {
  const fileInput = document.getElementById("csvFile");
  const scenario = document.getElementById("scenario").value;
  const xCol = document.getElementById("xColumn").value;
  const yCol = document.getElementById("yColumn").value;
  const valCol = document.getElementById("valueColumn").value;
  const spacing = document.getElementById("spacing").value;

  if (!fileInput.files[0]) {
    alert("Upload a CSV file");
    return;
  }

  if (!xCol || !yCol) {
    alert("Select X and Y columns");
    return;
  }

  if (scenario === "sparse_only") {
    if (!valCol) {
      alert("Value column is required");
      return;
    }
    if (!spacing || Number(spacing) <= 0) {
      alert("Output spacing must be > 0");
      return;
    }
  }

  const form = new FormData();
  form.append("csv_file", fileInput.files[0]);
  form.append("scenario", scenario);
  form.append("x_column", xCol);
  form.append("y_column", yCol);

  if (valCol) {
    form.append("value_column", valCol);
  }

  if (scenario === "sparse_only") {
    form.append("output_spacing", spacing);
  }

  const res = await fetch("/jobs", {
    method: "POST",
    body: form
  });

  if (!res.ok) {
    const text = await res.text();
    console.error("Backend error:", text);
    alert(text);
    return;
  }

  const data = await res.json();
  loadPreview(data.job_id);
});

// --------------------------------------------------
// Load geometry preview
// --------------------------------------------------
async function loadPreview(jobId) {
  const res = await fetch(`/jobs/${jobId}/preview`);
  const data = await res.json();
  drawGeometry(data.measured, data.generated);
}

// --------------------------------------------------
// SVG rendering
// --------------------------------------------------
function drawGeometry(measured, generated) {
  const svg = document.getElementById("geometry-svg");
  svg.innerHTML = "";

  const all = measured.concat(generated);
  if (all.length === 0) return;

  const xs = all.map(p => p.x);
  const ys = all.map(p => p.y);

  const minX = Math.min(...xs);
  const maxX = Math.max(...xs);
  const minY = Math.min(...ys);
  const maxY = Math.max(...ys);

  const pad = 40;
  const width = 1000 - pad * 2;
  const height = 600 - pad * 2;

  const sx = x =>
    pad + ((x - minX) / (maxX - minX || 1)) * width;

  const sy = y =>
    pad + height - ((y - minY) / (maxY - minY || 1)) * height;

  // ----------------------------
  // Measured stations
  // ----------------------------
  measured.forEach((p, i) => {
    const cx = sx(p.x);
    const cy = sy(p.y);

    const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    c.setAttribute("cx", cx);
    c.setAttribute("cy", cy);
    c.setAttribute("r", 5);
    c.setAttribute("class", "measured");
    svg.appendChild(c);

    const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
    t.setAttribute("x", cx + 6);
    t.setAttribute("y", cy - 6);
    t.setAttribute("font-size", "11");
    t.setAttribute("fill", "#1f6feb");
    t.textContent = i;
    svg.appendChild(t);
  });

  // ----------------------------
  // Predicted stations
  // ----------------------------
  generated.forEach((p, i) => {
    const cx = sx(p.x);
    const cy = sy(p.y);

    const c = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    c.setAttribute("cx", cx);
    c.setAttribute("cy", cy);
    c.setAttribute("r", 5);
    c.setAttribute("class", "predicted");
    svg.appendChild(c);

    const t = document.createElementNS("http://www.w3.org/2000/svg", "text");
    t.setAttribute("x", cx + 6);
    t.setAttribute("y", cy - 6);
    t.setAttribute("font-size", "11");
    t.setAttribute("fill", "#aaa");
    t.textContent = i;
    svg.appendChild(t);
  });
}
